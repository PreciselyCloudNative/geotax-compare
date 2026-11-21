# Databricks notebook source

# COMMAND ----------

# COMMAND ----------
import base64
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType)

sys.path.insert(0, os.getcwd())  # notebook folder: unit_lib.py sits next to this file
import unit_lib as L  # noqa: E402

for name, default in [
    ("state", "AZ"),
    ("unit_id", "0"),
    ("n_units", "1"),
    ("batch_seq", "0"),
    ("sample_size", "0"),
    ("chunk_size", "5000"),
    ("geo_concurrency", "25"),
    ("tax_concurrency", "6"),
    ("tax_rate_per_sec", "12"),  # Geo Tax API limit measured at 50 calls/s per key; this is per unit task x 4 parallel units = 48/s
    ("fail_after_chunks", "0"),
    ("catalog", "main"),
    ("secret_scope", "precisely"),
    ("secret_key_access", "api_key"),
    ("secret_key_secret", "api_secret"),
    ("purchase_amount", "100"),
]:
    dbutils.widgets.text(name, default)

W = dbutils.widgets.get
STATE = W("state").strip().upper()
UNIT_ID, N_UNITS = int(float(W("unit_id"))), int(float(W("n_units")))
SAMPLE_SIZE, CHUNK = int(W("sample_size")), int(W("chunk_size"))
FAIL_AFTER = int(W("fail_after_chunks"))
RUN_MODE = "SAMPLE" if SAMPLE_SIZE > 0 else "FULL"
BATCH_SEQ = int(float(W("batch_seq")))
T = L.table_names(W("catalog"), RUN_MODE)
WORK, LEDGER, STAGING = T["work"], T["ledger"], T["staging"]
RUN_ID = uuid.uuid4().hex[:12]
if not 0 < CHUNK <= 100000:
    raise ValueError("chunk_size must be between 1 and 100000")

# COMMAND ----------

# COMMAND ----------
L.ensure_tables(spark, W("catalog"))

# COMMAND ----------
base = (spark.table(WORK).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ) & (F.col("change_type") != "DELETED")))
unit = base.filter(F.pmod(F.xxhash64("POSTALCODE"), F.lit(N_UNITS)) == UNIT_ID)  # ZIP split: addresses sharing a coordinate land in the same unit
checksum = lambda df: F.sum((F.pmod(F.xxhash64("PBKEY"), F.lit(1000000007))).cast("bigint"))  # noqa: E731
exp = unit.agg(F.count("*").alias("n"), F.countDistinct("PBKEY").alias("d"), checksum(unit).alias("c")).collect()[0]
EXPECTED, EXPECTED_SUM = exp["n"], exp["c"]
batch = (spark.table(T["control"]).filter((F.col("state") == STATE) & (F.col("run_mode") == RUN_MODE) & (F.col("batch_seq") == BATCH_SEQ))
         .select("from_seq", "release_version").limit(1).collect())
if not batch:
    raise RuntimeError(f"No run_control row for {STATE} batch {BATCH_SEQ} ({RUN_MODE}): the gate did not load it")
FROM_SEQ, RELEASE_VERSION = int(batch[0]["from_seq"]), batch[0]["release_version"]
if exp["n"] != exp["d"]:
    raise RuntimeError(f"PBKEY is not unique in the unit ({exp['n']} rows, {exp['d']} distinct)")
if EXPECTED == 0:
    raise RuntimeError(f"No addresses for {STATE} batch {BATCH_SEQ} unit {UNIT_ID}/{N_UNITS}: has the gate loaded them?")
print(f"{RUN_MODE} unit {UNIT_ID}/{N_UNITS} {STATE} batch {BATCH_SEQ}: {EXPECTED:,} addresses")

ledger_key = ((F.col("batch_seq") == BATCH_SEQ) & (F.col("run_mode") == RUN_MODE)
              & (F.col("state") == STATE)
              & (F.col("n_units") == N_UNITS) & (F.col("unit_id") == UNIT_ID) & (F.col("status") == "COMPLETE"))
prior = spark.table(LEDGER).filter(ledger_key).collect()
if prior and prior[0]["rows_expected"] == EXPECTED and prior[0]["pbkey_checksum"] == EXPECTED_SUM:
    print("Unit already COMPLETE with matching count and checksum: nothing to do.")
    dbutils.notebook.exit("already complete")

# COMMAND ----------

# COMMAND ----------
# Resume: addresses already written for this unit are skipped.
res_unit = spark.table(STAGING).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ) & (F.col("unit_id") == UNIT_ID)
                                       & (F.col("n_units") == N_UNITS)).select("PBKEY")
todo = unit.join(res_unit, "PBKEY", "left_anti").orderBy("POSTALCODE", "ADDRLINE1", "PBKEY")  # same ZIP/street in the same chunk, so Geo Tax dedup catches more
pending = [r.asDict() for r in todo.select("PBKEY", "ADMIN1", "ADMIN2", "ADDRLINE1", "CITY", "POSTALCODE", "change_type",
                                           "release_seq", "release_version", "is_valid", "invalid_reason").collect()]
SKIPPED = EXPECTED - len(pending)
print(f"Resume: {SKIPPED:,} already written, {len(pending):,} to process")

scope = W("secret_scope")
pair = f"{dbutils.secrets.get(scope, W('secret_key_access'))}:{dbutils.secrets.get(scope, W('secret_key_secret'))}"
session = requests.Session()
session.mount("https://", HTTPAdapter(pool_maxsize=int(W("geo_concurrency")) + int(W("tax_concurrency")) + 10))
stop = threading.Event()  # set on the first pipeline failure; also wakes threads waiting on the API circuit breaker
client = L.ApiClient(session, "Apikey " + base64.b64encode(pair.encode()).decode(),
                     int(W("geo_concurrency")), int(W("tax_concurrency")), float(W("purchase_amount")), stop=stop,
                     tax_rate=float(W("tax_rate_per_sec")) or None)
del pair
PREFS2, PREFS3 = L.geocode_prefs("enterprise"), L.geocode_prefs("premium")

SPARK_TYPES = {"string": StringType(), "int": IntegerType(), "bigint": LongType(), "double": DoubleType(), "timestamp": TimestampType()}
SCHEMA = StructType([StructField(n, SPARK_TYPES[t], True) for n, t in L.RESULT_COLS])


def write_chunk(rows):
    """Plain append to results_staging: it reads nothing, so concurrent units never conflict (a MERGE read the whole table and did).
    Idempotency comes from the resume anti-join at unit start; the unit verification and reconcile catch duplicates."""
    sdf = spark.createDataFrame([tuple(r[n] for n in L.RESULT_NAMES) for r in rows], SCHEMA)
    for attempt in range(4):
        try:
            sdf.writeTo(STAGING).append()
            return
        except Exception as e:
            if "Concurrent" not in type(e).__name__ + str(e)[:200] or attempt == 3:
                raise
            time.sleep(2 + attempt * 2)


# COMMAND ----------

# COMMAND ----------
# Three pipelined stages, each on its own thread, joined by bounded queues:
#   main thread  : Geocode chunk k+2      (geo pool)
#   tax thread   : Geo Tax for chunk k+1  (tax pool)
#   write thread : assemble + append chunk k (Spark; no API calls, so a slow Delta commit never idles an API pool)
started = datetime.now(timezone.utc)
t_start = time.perf_counter()
stats = {"processed": 0, "needed": 0, "sent": 0, "chunks": 0}
# Seconds each stage spent waiting: shows which stage is the bottleneck (blocked = downstream is slower, starved = upstream is slower).
waits = {"geo_blocked": 0.0, "tax_starved": 0.0, "tax_blocked": 0.0, "write_starved": 0.0, "write_busy": 0.0}
errors = []
geo_to_tax = queue.Queue(maxsize=1)
tax_to_write = queue.Queue(maxsize=2)
meta = dict(batch_seq=BATCH_SEQ, unit_id=UNIT_ID, n_units=N_UNITS, run_id=RUN_ID, processed_at=started)


def fail(e):
    errors.append(e)
    stop.set()


def put(q, item, key):
    """Blocking put that gives up once a stage has failed; the time spent blocked is added to waits[key]."""
    t0 = time.perf_counter()
    while not stop.is_set():
        try:
            q.put(item, timeout=1)
            break
        except queue.Full:
            pass
    waits[key] += time.perf_counter() - t0


def tax_worker(pool):
    while True:
        t0 = time.perf_counter()
        item = geo_to_tax.get()
        waits["tax_starved"] += time.perf_counter() - t0
        if item is None:
            break
        if stop.is_set():
            continue  # keep draining so the producer never blocks; the writer still flushes chunks that already passed this stage
        try:
            valid, invalid, geo2, geo3 = item
            taxmap, needed = L.tax_stage(client, pool, geo2, geo3)
            put(tax_to_write, (valid, invalid, geo2, geo3, taxmap, needed), "tax_blocked")
        except BaseException as e:  # noqa: BLE001
            fail(e)
    tax_to_write.put(None)


def write_worker():
    broken = False  # only a write failure stops writing: chunks already geocoded and taxed are paid for, so they are still flushed
    while True:
        t0 = time.perf_counter()
        item = tax_to_write.get()
        waits["write_starved"] += time.perf_counter() - t0
        if item is None:
            return
        if broken:
            continue
        try:
            t1 = time.perf_counter()
            valid, invalid, geo2, geo3, taxmap, needed = item
            write_chunk(L.assemble(valid, geo2, geo3, taxmap, meta, invalid))
            waits["write_busy"] += time.perf_counter() - t1
            stats["processed"] += len(valid) + len(invalid)
            stats["needed"] += needed
            stats["sent"] += len(taxmap)
            stats["chunks"] += 1
            print(f"chunk {stats['chunks']} merged: {stats['processed']:,}/{len(pending):,} addresses, "
                  f"{time.perf_counter() - t_start:.0f}s elapsed")
            if FAIL_AFTER and stats["chunks"] >= FAIL_AFTER:
                raise RuntimeError(f"injected failure after {stats['chunks']} chunks (fail_after_chunks)")
        except BaseException as e:  # noqa: BLE001
            broken = True
            fail(e)


from concurrent.futures import ThreadPoolExecutor  # noqa: E402

with ThreadPoolExecutor(client.geo_limiter.max) as geo_pool, ThreadPoolExecutor(client.tax_limiter.max) as tax_pool:
    tax_thread = threading.Thread(target=tax_worker, args=(tax_pool,), daemon=True)
    write_thread = threading.Thread(target=write_worker, daemon=True)
    tax_thread.start()
    write_thread.start()
    try:
        for i in range(0, len(pending), CHUNK):
            if stop.is_set():
                break
            part = pending[i:i + CHUNK]
            valid = [r for r in part if r["is_valid"]]
            invalid = [r for r in part if not r["is_valid"]]
            geo2, geo3 = L.geocode_stage(client, geo_pool, valid, PREFS2, PREFS3) if valid else ([], [])
            put(geo_to_tax, (valid, invalid, geo2, geo3), "geo_blocked")
    except BaseException as e:  # noqa: BLE001
        errors.append(e)  # geocode failed: downstream stages still finish the chunks already handed to them
    geo_to_tax.put(None)
    tax_thread.join()
    write_thread.join()
print("stage waits (s): " + ", ".join(f"{k}={v:.0f}" for k, v in waits.items())
      + f"; API circuit breaker: opened {client.breaker.opens}x, threads waited {client.breaker.waited:.0f}s in total")
if errors:
    raise errors[0]

# COMMAND ----------

# COMMAND ----------
# Verify the unit before committing it.
written = (spark.table(STAGING).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ) & (F.col("unit_id") == UNIT_ID)
                                       & (F.col("n_units") == N_UNITS))
           .join(unit.select("PBKEY"), "PBKEY", "left_semi"))  # only this unit's addresses
w = written.agg(F.count("*").alias("n"), checksum(written).alias("c")).collect()[0]
if w["n"] != EXPECTED or w["c"] != EXPECTED_SUM:
    raise RuntimeError(f"Unit verification failed: expected {EXPECTED} rows / checksum {EXPECTED_SUM}, found {w['n']} / {w['c']}")

wall = time.perf_counter() - t_start
s = client.stats
spark.createDataFrame(
    [(STATE, RUN_MODE, SAMPLE_SIZE, BATCH_SEQ, FROM_SEQ, RELEASE_VERSION, N_UNITS, UNIT_ID, "COMPLETE", EXPECTED, w["n"], w["c"],
      stats["processed"], SKIPPED, s["geocode_calls"], s["tax_calls"], stats["needed"], stats["sent"], s["retries"],
      client.geo_limiter.throttled, client.tax_limiter.throttled, round(s["api_seconds"], 1), round(wall, 1),
      RUN_ID, started, datetime.now(timezone.utc))],
    L.ddl_columns(L.LEDGER_COLS),
).write.mode("append").saveAsTable(LEDGER)

print(f"COMPLETE unit {UNIT_ID}: {stats['processed']:,} addresses in {wall:.0f}s = {stats['processed'] / wall:,.0f} addr/s "
      f"({stats['processed'] * 4 / wall:,.0f} transactions/s upper bound); geocode calls {s['geocode_calls']:,}, tax calls {s['tax_calls']:,}; "
      f"Geo Tax lookups {stats['needed']:,} -> {stats['sent']:,} sent after dedupe "
      f"({100 * (1 - stats['sent'] / max(stats['needed'], 1)):.0f}% saved); retries {s['retries']}, "
      f"throttles geo {client.geo_limiter.throttled} tax {client.tax_limiter.throttled}")
