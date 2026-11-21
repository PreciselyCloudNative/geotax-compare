# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 — Gate and load (one state)
# MAGIC
# MAGIC **Where it sits:** child job `geotax_compare_state_job` (one run per state): `gate` → `has_work` → `has_pending_units` → `process_units` (for-each) → `reconcile`.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Finds the state's **watermark**: the `batch_seq` (address_book `release_seq`) of its last DONE run.
# MAGIC 2. Decides the work: **BASELINE** (state never processed → every current record), **DELTA** (records ADDED / UPDATED / DELETED in releases after the watermark), **RESUME** (an earlier run was loaded but not finished), or **SKIP** (already at the latest release).
# MAGIC 3. Runs preflight checks (secrets + one live call to each API) before any billable work.
# MAGIC 4. Loads the records into `work_items` and plans the work units (ZIP-hash split).
# MAGIC
# MAGIC **Task values:** `go`, `batch_seq`, `release_version`, `kind`, `n_units`, `pending_units`, `pending_count`.

# COMMAND ----------
import base64
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from math import ceil

import requests
from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, os.getcwd())  # notebook folder: unit_lib.py sits next to this file
import unit_lib as L  # noqa: E402

for name, default in [
    ("state", ""),
    ("sample_size", "0"),
    ("unit_size", "100000"),
    ("catalog", "main"),
    ("source_current", "address_book.silver.adf_usa_current"),
    ("source_feed", "address_book.silver.adf_usa_change_feed"),
    ("source_releases", "address_book.silver.adf_usa_releases"),
    ("secret_scope", "precisely"),
    ("secret_key_access", "api_key"),
    ("secret_key_secret", "api_secret"),
    ("purchase_amount", "100"),
    ("allow_baseline", "false"),
]:
    dbutils.widgets.text(name, default)

W = dbutils.widgets.get
STATE = W("state").strip().upper()
if not re.fullmatch(r"[A-Z]{2}", STATE):
    raise ValueError(f"`state` must be one 2-letter code, got {STATE!r}")
SAMPLE_SIZE = int(W("sample_size") or 0)
if SAMPLE_SIZE < 0:
    raise ValueError("sample_size must be 0 (full) or a positive number")
RUN_MODE = "SAMPLE" if SAMPLE_SIZE > 0 else "FULL"
UNIT_SIZE = int(W("unit_size"))
T = L.table_names(W("catalog"), RUN_MODE)
L.ensure_tables(spark, W("catalog"))
RUN_ID = uuid.uuid4().hex[:12]


def publish_noop(reason):
    print(f"Nothing to do for {STATE}: {reason}")
    for k, v in (("go", "false"), ("pending_count", "0"), ("pending_units", []), ("n_units", "0"),
                 ("batch_seq", "0"), ("release_version", ""), ("kind", "")):
        dbutils.jobs.taskValues.set(k, v)
    dbutils.notebook.exit("no-op")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Latest release and the state's watermark
# MAGIC
# MAGIC `to_seq` is the newest applied release in `address_book`. The watermark is the highest `batch_seq` this state finished (DONE) in the same run mode. A `LOADED` row means an earlier run loaded its records but did not finish: that run is **resumed to the end first** (same release range), and the next run picks up anything newer.

# COMMAND ----------
latest = spark.table(W("source_releases")).orderBy(F.col("release_seq").desc()).limit(1).collect()
if not latest:
    raise RuntimeError(f"{W('source_releases')} has no applied release")
TO_SEQ, TO_VERSION = int(latest[0]["release_seq"]), latest[0]["release_version"]

control = spark.table(T["control"]).filter((F.col("state") == STATE) & (F.col("run_mode") == RUN_MODE)).collect()
done_seqs = [r["batch_seq"] for r in control if r["status"] == "DONE"]
watermark = max(done_seqs) if done_seqs else None
open_runs = sorted((r for r in control if r["status"] == "LOADED" and (watermark is None or r["batch_seq"] > watermark)),
                   key=lambda r: r["batch_seq"])

try:
    decision = L.decide_action(watermark, open_runs, TO_SEQ, RUN_MODE, W("allow_baseline").strip().lower() == "true")
except L.BaselineBlocked as e:
    raise RuntimeError(f"{STATE} [{RUN_MODE}] baseline refused: {e}") from None
if decision == "RESUME":
    run = open_runs[-1]
    action, FROM_SEQ, BATCH_SEQ, BATCH_VERSION, KIND = "RESUME", run["from_seq"], int(run["batch_seq"]), run["release_version"], run["kind"]
elif decision == "SKIP":
    publish_noop(f"already at release {TO_VERSION} (release_seq {TO_SEQ})")
elif decision == "BASELINE":
    action, FROM_SEQ, BATCH_SEQ, BATCH_VERSION, KIND = "LOAD", 0, TO_SEQ, TO_VERSION, "BASELINE"
else:
    action, FROM_SEQ, BATCH_SEQ, BATCH_VERSION, KIND = "LOAD", int(watermark), TO_SEQ, TO_VERSION, "DELTA"
print(f"{STATE} [{RUN_MODE}] {action} {KIND}: release_seq {FROM_SEQ} -> {BATCH_SEQ} ({BATCH_VERSION}); watermark={watermark}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Preflight checks
# MAGIC
# MAGIC Fails fast before any real work: secrets readable, one Geocode call per data option, one Geo Tax call on the returned coordinates. Results go to `preflight_log`.

# COMMAND ----------
now = datetime.now(timezone.utc)
checks = []


def check(name, passed, detail):
    checks.append((RUN_ID, STATE, BATCH_SEQ, name, bool(passed), str(detail)[:300], now))


try:
    key = dbutils.secrets.get(W("secret_scope"), W("secret_key_access"))
    secret = dbutils.secrets.get(W("secret_scope"), W("secret_key_secret"))
    headers = {"Authorization": "Apikey " + base64.b64encode(f"{key}:{secret}".encode()).decode(),
               "Content-Type": "application/json"}
    check("secrets_available", True, f"scope={W('secret_scope')}")
except Exception as e:
    headers = None
    check("secrets_available", False, f"{type(e).__name__}: {e}")

if headers:
    address = {"addressLines": ["804 N CAUSEWAY BLVD STE A"], "city": "METAIRIE", "postalCode": "70001",
               "admin1": "LA", "country": "usa"}
    coords = None
    for option, data_option in ((2, "enterprise"), (3, "premium")):
        try:
            r = requests.post(
                L.GEOCODE_URL, headers=headers, timeout=60,
                json={"preferences": {"maxResults": 1, "returnAllInfo": "true",
                                      "customPreferences": {"DATA_OPTION": data_option}},
                      "addresses": [address]})
            c = r.json()["responses"][0]["results"][0]["location"]["feature"]["geometry"]["coordinates"]
            check(f"api_geocode_option{option}", r.status_code == 200 and len(c) == 2, f"http {r.status_code}")
            coords = coords or c
        except Exception as e:
            check(f"api_geocode_option{option}", False, f"{type(e).__name__}: {e}")
    try:
        if not coords:
            raise RuntimeError("no coordinates from geocode smoke test")
        r = requests.post(
            L.TAX_URL, headers=headers, timeout=60,
            json={"preferences": L.GEOTAX_PREFS, "locations": [{"longitude": float(coords[0]), "latitude": float(coords[1])}]})
        check("api_geotax", r.status_code == 200 and r.json()["responses"][0]["status"] == "OK", f"http {r.status_code}")
    except Exception as e:
        check("api_geotax", False, f"{type(e).__name__}: {e}")

spark.createDataFrame(checks, L.ddl_columns(L.PREFLIGHT_COLS)).write.mode("append").saveAsTable(T["preflight"])
failed = [c for c in checks if not c[4]]
if failed:
    raise RuntimeError("Preflight failed: " + "; ".join(f"{c[3]} ({c[5]})" for c in failed))
print("Preflight passed:", ", ".join(c[3] for c in checks))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Load the records into `work_items`
# MAGIC
# MAGIC Skipped on RESUME (the records are already there).
# MAGIC
# MAGIC - **BASELINE:** every record of the state in the current table (`change_type = BASELINE`).
# MAGIC - **DELTA:** feed rows with `from_seq < release_seq <= to_seq` for the state (only the newest change per PBKEY when several releases are applied at once): `ADDED` and `UPDATED` are sent to the APIs again; `DELETED` rows are tombstones with no `ADMIN1`, so each is matched to the state through its PBKEY in `results`. A deleted record that was never processed is ignored.
# MAGIC - Invalid addresses are flagged (`is_valid`, `invalid_reason`), never dropped.
# MAGIC - `sample_size > 0` keeps a deterministic sample (hash order) of the records to process; deletes are not sampled.
# MAGIC
# MAGIC A reload of the same batch deletes its earlier `work_items` first, so the load is idempotent.

# COMMAND ----------
ITEM_COLS = ["PBKEY", "ADMIN1", "ADMIN2", "ADDRLINE1", "CITY", "POSTALCODE", "POSTALCODEEXT", "release_seq", "release_version"]


def blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


if action == "LOAD":
    spark.sql(f"DELETE FROM {T['work']} WHERE ADMIN1 = '{STATE}' AND batch_seq = {BATCH_SEQ}")
    if KIND == "BASELINE":
        todo = (spark.table(W("source_current")).filter((F.col("ADMIN1") == STATE) & (F.col("status") != "DELETED"))
                .select(*ITEM_COLS).withColumn("change_type", F.lit("BASELINE")))
        deletes = None
    else:
        # A state several releases behind reads more than one release at once: keep only the newest change per PBKEY
        # (ADDED then UPDATED -> UPDATED; UPDATED then DELETED -> DELETED; DELETED then re-ADDED -> ADDED).
        feed = (spark.table(W("source_feed")).filter((F.col("release_seq") > FROM_SEQ) & (F.col("release_seq") <= BATCH_SEQ))
                .withColumn("_rn", F.row_number().over(Window.partitionBy("PBKEY").orderBy(F.col("release_seq").desc())))
                .filter("_rn = 1").drop("_rn"))
        todo = (feed.filter((F.col("ADMIN1") == STATE) & F.col("status").isin("ADDED", "UPDATED"))
                .select(*ITEM_COLS, F.col("status").alias("change_type")))
        known = spark.table(T["results"]).filter(F.col("ADMIN1") == STATE).select("PBKEY", "ADMIN1")
        deletes = (feed.filter(F.col("status") == "DELETED")
                   .select("PBKEY", F.col("release_seq"), F.col("release_version"))
                   .join(known, "PBKEY"))  # tombstones have no ADMIN1: the state comes from our own results row
    if SAMPLE_SIZE > 0:
        todo = todo.orderBy(F.xxhash64("PBKEY")).limit(SAMPLE_SIZE)  # deterministic, per state

    reason = F.concat_ws(
        ";",
        F.when(blank("ADDRLINE1"), F.lit("ADDRLINE1_MISSING")),
        F.when(blank("CITY") & blank("POSTALCODE"), F.lit("CITY_AND_POSTALCODE_MISSING")),
        F.when(~blank("POSTALCODE") & ~F.col("POSTALCODE").rlike(r"^\d{5}$"), F.lit("POSTALCODE_MALFORMED")),
        F.when(blank("PBKEY"), F.lit("PBKEY_MISSING")),
    )
    meta = [F.lit(BATCH_SEQ).alias("batch_seq"), F.lit(FROM_SEQ).alias("from_seq"), F.lit(RUN_ID).alias("run_id"),
            F.current_timestamp().alias("loaded_at")]
    items = todo.select(*[F.col(c) for c in ITEM_COLS[:7]], "change_type", "release_seq", "release_version", *meta,
                        (reason == "").alias("is_valid"), reason.alias("invalid_reason"))
    items.write.mode("append").option("maxRecordsPerFile", 50000).saveAsTable(T["work"])
    if deletes is not None:
        none = lambda t: F.lit(None).cast(t)  # noqa: E731
        (deletes.select("PBKEY", F.lit(STATE).alias("ADMIN1"), none("string").alias("ADMIN2"), none("string").alias("ADDRLINE1"),
                        none("string").alias("CITY"), none("string").alias("POSTALCODE"), none("string").alias("POSTALCODEEXT"),
                        F.lit("DELETED").alias("change_type"), "release_seq", "release_version", *meta,
                        F.lit(True).alias("is_valid"), none("string").alias("invalid_reason"))
         .write.mode("append").saveAsTable(T["work"]))

    work = spark.table(T["work"]).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ))
    cnt = {r["change_type"]: r["n"] for r in work.groupBy("change_type").agg(F.count("*").alias("n")).collect()}
    n_deletes = cnt.get("DELETED", 0)
    n_process = sum(v for k, v in cnt.items() if k != "DELETED")
    if KIND == "BASELINE" and n_process == 0:
        raise RuntimeError(f"No records for state {STATE} in {W('source_current')}: wrong state code?")
    spark.createDataFrame(
        [(STATE, RUN_MODE, SAMPLE_SIZE, KIND, FROM_SEQ, BATCH_SEQ, BATCH_VERSION, "LOADED", RUN_ID, n_process, n_deletes,
          datetime.now(timezone.utc), None)], L.ddl_columns(L.CONTROL_COLS)).write.mode("append").saveAsTable(T["control"])
    print(f"Loaded for {STATE}: {cnt}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Plan the work units
# MAGIC
# MAGIC Only records that go to the APIs (everything except `DELETED`) are split. A unit is the set of addresses with `pmod(xxhash64(POSTALCODE), n_units) = unit_id`, `n_units = ceil(rows / unit_size)` (0 when the delta has nothing to geocode: the state still advances its watermark in reconcile). A unit is **pending** unless `unit_ledger` has a `COMPLETE` row whose row count and PBKEY checksum match.

# COMMAND ----------
work = (spark.table(T["work"]).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ) & (F.col("change_type") != "DELETED")))
total = work.count()
N_UNITS = ceil(total / UNIT_SIZE) if total else 0
pending_units = []
if N_UNITS:
    ledger = spark.table(T["ledger"]).filter(
        (F.col("state") == STATE) & (F.col("run_mode") == RUN_MODE) & (F.col("batch_seq") == BATCH_SEQ)
        & (F.col("n_units") == N_UNITS) & (F.col("status") == "COMPLETE")).select("unit_id", "rows_expected", "pbkey_checksum")
    expected = (work.groupBy(F.pmod(F.xxhash64("POSTALCODE"), F.lit(N_UNITS)).alias("unit_id"))
                .agg(F.count("*").alias("rows_expected"),
                     F.sum(F.pmod(F.xxhash64("PBKEY"), F.lit(1000000007)).cast("bigint")).alias("pbkey_checksum")))
    todo_units = expected.join(ledger, ["unit_id", "rows_expected", "pbkey_checksum"], "left_anti").orderBy("unit_id").collect()
    # strings: task values turn ints into floats
    pending_units = [{"state": STATE, "unit_id": str(int(r["unit_id"])), "n_units": str(N_UNITS), "batch_seq": str(BATCH_SEQ)} for r in todo_units]
print(f"{STATE}: {total:,} records to geocode -> {N_UNITS} units, {len(pending_units)} pending")

for k, v in (("go", "true"), ("batch_seq", str(BATCH_SEQ)), ("release_version", BATCH_VERSION), ("kind", KIND),
             ("n_units", str(N_UNITS)), ("pending_units", pending_units), ("pending_count", str(len(pending_units)))):
    dbutils.jobs.taskValues.set(k, v)
print(f"GO: {STATE} {KIND} release_seq {FROM_SEQ}->{BATCH_SEQ} ({BATCH_VERSION}), units pending {len(pending_units)}")
