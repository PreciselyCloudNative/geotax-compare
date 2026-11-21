# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4 — Reconcile, merge and advance the watermark (one state)
# MAGIC
# MAGIC Runs once per state after its units. The state only advances when everything adds up:
# MAGIC 1. **Verify** — every record of the batch has exactly one row in `results_staging`; every unit has a matching `COMPLETE` ledger row. Any gap raises and leaves the state `LOADED`, so a re-run re-drives only the missing units.
# MAGIC 2. **Merge** — staged rows are upserted into `results` (one row per PBKEY): new PBKEYs are inserted, existing ones replaced.
# MAGIC 3. **Deletes** — a record deleted in the source keeps its `PBKEY` and `ADMIN1`, every other column becomes NULL and `row_status = 'DELETED'`.
# MAGIC 4. **Mark DONE** — `run_control` goes `LOADED` → `DONE`; that `batch_seq` is the state's new watermark.
# MAGIC
# MAGIC Every step is idempotent, so a failed or repeated reconcile is safe. The outcome is always logged to `reconcile_log`.

# COMMAND ----------
import os
import random
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

sys.path.insert(0, os.getcwd())  # notebook folder: unit_lib.py sits next to this file
import unit_lib as L  # noqa: E402

for name, default in [
    ("state", ""),
    ("n_units", "0"),
    ("batch_seq", "0"),
    ("sample_size", "0"),
    ("catalog", "main"),
]:
    dbutils.widgets.text(name, default)

W = dbutils.widgets.get
STATE, N_UNITS, BATCH_SEQ, SAMPLE_SIZE = W("state").strip().upper(), int(float(W("n_units"))), int(float(W("batch_seq"))), int(W("sample_size"))
RUN_MODE = "SAMPLE" if SAMPLE_SIZE > 0 else "FULL"
T = L.table_names(W("catalog"), RUN_MODE)
if not STATE or BATCH_SEQ <= 0:
    raise ValueError("state and batch_seq are required (the gate publishes batch_seq 0 when there is nothing to do)")
L.ensure_tables(spark, W("catalog"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Verify the batch

# COMMAND ----------
work = spark.table(T["work"]).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ))
to_process = work.filter("change_type != 'DELETED'")
deleted = work.filter("change_type = 'DELETED'")
expected, deletes_expected = to_process.count(), deleted.count()

staged = (spark.table(T["staging"]).filter((F.col("ADMIN1") == STATE) & (F.col("batch_seq") == BATCH_SEQ))
          .join(to_process.select("PBKEY"), "PBKEY", "left_semi"))
r = staged.agg(
    F.count("*").alias("n"), F.countDistinct("PBKEY").alias("d"),
    *[F.count_if(F.col("row_status") == s).alias(s) for s in ("OK", "GEOCODE_FAILED", "TAX_FAILED", "INVALID_INPUT")],
).collect()[0]
led = (spark.table(T["ledger"]).filter((F.col("state") == STATE) & (F.col("run_mode") == RUN_MODE) & (F.col("batch_seq") == BATCH_SEQ)
                                      & (F.col("n_units") == N_UNITS) & (F.col("status") == "COMPLETE"))
       .agg(F.countDistinct("unit_id").alias("units"), F.sum("rows_written").alias("rows")).collect()[0])
units_complete, ledger_rows = led["units"], led["rows"] or 0
missing = expected - r["d"]

problems = []
if missing:
    problems.append(f"{missing:,} records have no staged result row")
if r["n"] != r["d"]:
    problems.append(f"{r['n'] - r['d']:,} duplicate staged rows")
if units_complete != N_UNITS:
    problems.append(f"{units_complete} of {N_UNITS} units have a COMPLETE ledger row")
if ledger_rows != expected:
    problems.append(f"ledger rows_written {ledger_rows:,} != expected {expected:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Merge into `results` and null the deleted records
# MAGIC
# MAGIC Only when verification passed. A source DELETE for a PBKEY that was never processed has no `results` row and is ignored (`deletes_applied` counts the rows actually nulled).

# COMMAND ----------
merged = deletes_applied = 0
if not problems:
    # A record can be re-sent by a later batch only through a newer batch_seq, so staging rows of this batch are the latest.
    staged.createOrReplaceTempView("staged_batch")
    spark.sql(f"""MERGE INTO {T['results']} t USING staged_batch s ON t.PBKEY = s.PBKEY
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    merged = spark.table(T["results"]).filter(F.col("batch_seq") == BATCH_SEQ).join(to_process.select("PBKEY"), "PBKEY", "left_semi").count()
    if merged != expected:
        problems.append(f"after merge only {merged:,} of {expected:,} records are in results")

    if deletes_expected and not problems:
        nulls = ", ".join(f"t.{n} = NULL" for n, _ in L.RESULT_COLS if n not in L.TOMBSTONE_KEEP | {"change_type", "release_seq", "release_version", "batch_seq", "processed_at", "row_status"})
        deleted.select("PBKEY", "release_seq", "release_version").createOrReplaceTempView("deleted_batch")
        spark.sql(f"""MERGE INTO {T['results']} t USING deleted_batch d ON t.PBKEY = d.PBKEY AND t.ADMIN1 = '{STATE}'
                      WHEN MATCHED THEN UPDATE SET {nulls}, t.change_type = 'DELETED', t.row_status = 'DELETED',
                        t.release_seq = d.release_seq, t.release_version = d.release_version,
                        t.batch_seq = {BATCH_SEQ}, t.processed_at = current_timestamp()""")
        deletes_applied = (spark.table(T["results"]).filter((F.col("batch_seq") == BATCH_SEQ) & (F.col("row_status") == "DELETED"))
                           .join(deleted.select("PBKEY"), "PBKEY", "left_semi").count())

passed = not problems
detail = "; ".join(problems) or f"all {expected:,} records accounted for; {deletes_applied:,} of {deletes_expected:,} deletes applied (the rest were never processed)"
spark.createDataFrame(
    [(STATE, RUN_MODE, BATCH_SEQ, N_UNITS, expected, r["n"], r["d"], missing, r["OK"], r["GEOCODE_FAILED"], r["TAX_FAILED"],
      r["INVALID_INPUT"], int(units_complete), int(ledger_rows), deletes_expected, deletes_applied, merged, passed, detail,
      datetime.now(timezone.utc))],
    L.ddl_columns(L.RECONCILE_COLS),
).write.mode("append").saveAsTable(T["reconcile_log"])
print(f"{STATE} {RUN_MODE} batch {BATCH_SEQ}: expected {expected:,}, staged {r['n']:,}, OK {r['OK']:,}, geocode failed {r['GEOCODE_FAILED']:,}, "
      f"tax failed {r['TAX_FAILED']:,}, invalid {r['INVALID_INPUT']:,}, merged {merged:,}; {detail}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Mark DONE (advance the watermark)

# COMMAND ----------
if not passed:
    raise RuntimeError(f"Reconcile failed for {STATE}: {detail}. The state stays LOADED; re-run the job to resume the same batch.")
for attempt in range(8):
    try:
        spark.sql(f"""UPDATE {T['control']} SET status = 'DONE', completed_at = current_timestamp()
                      WHERE state = '{STATE}' AND run_mode = '{RUN_MODE}' AND batch_seq = {BATCH_SEQ} AND status = 'LOADED'""")
        break
    except Exception as e:
        if "Concurrent" not in type(e).__name__ + str(e)[:300] or attempt == 7:
            raise
        time.sleep(random.uniform(1, 4))
print(f"{STATE} DONE at batch_seq {BATCH_SEQ} ({RUN_MODE}): the next run only processes source changes after it.")
