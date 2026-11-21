# Databricks notebook source
# MAGIC %md
# MAGIC # Task 0 — Plan states
# MAGIC
# MAGIC Turns the `states` job parameter into the list the parent job loops over **one state at a time**.
# MAGIC
# MAGIC - `AZ` or `AZ,CA,TX` → those states, in the order given (duplicates removed).
# MAGIC - `ALL` → every 2-letter `ADMIN1` found in the `address_book` current table, alphabetical, **minus the states that are already at the newest release**. A run with nothing to do plans no states at all (the loop is skipped), and a scheduled run costs seconds. A state that has never been processed is left out with a warning unless `allow_baseline=true` (a first run bills every address).
# MAGIC
# MAGIC Also creates the schemas and tables (idempotent) so the downstream pipeline never reads a missing table.
# MAGIC Published task value: `states` (list of 2-letter codes).

# COMMAND ----------
import os
import re
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.getcwd())  # notebook folder: unit_lib.py sits next to this file
import unit_lib as L  # noqa: E402

for name, default in [
    ("states", ""),
    ("catalog", "main"),
    ("source_current", "address_book.silver.adf_usa_current"),
    ("source_releases", "address_book.silver.adf_usa_releases"),
    ("sample_size", "0"),
    ("allow_baseline", "false"),
]:
    dbutils.widgets.text(name, default)

W = dbutils.widgets.get
L.ensure_tables(spark, W("catalog"))

raw = [s.strip().upper() for s in W("states").split(",") if s.strip()]
if not raw:
    raise ValueError("Enter at least one state in the `states` job parameter: AZ, AZ,CA,TX or ALL")

if raw == ["ALL"]:
    rows = spark.table(W("source_current")).filter(F.col("ADMIN1").isNotNull()).select("ADMIN1").distinct().collect()
    every_state = sorted(r["ADMIN1"] for r in rows if re.fullmatch(r"[A-Z]{2}", r["ADMIN1"]))
    run_mode = "SAMPLE" if int(W("sample_size") or 0) > 0 else "FULL"
    latest = spark.table(W("source_releases")).agg(F.max("release_seq")).collect()[0][0]
    if latest is None:
        raise RuntimeError(f"{W('source_releases')} has no applied release")
    control = [r.asDict() for r in spark.table(L.table_names(W("catalog"), run_mode)["control"])
               .select("state", "run_mode", "batch_seq", "status").collect()]
    states, blocked, current = L.states_needing_work(every_state, control, int(latest), run_mode,
                                                     W("allow_baseline").strip().lower() == "true")
    if blocked:
        print(f"WARNING: {len(blocked)} state(s) were never processed and are left out (set allow_baseline=true to baseline them): {', '.join(blocked)}")
    print(f"ALL: {len(current)} state(s) already at release_seq {latest}, {len(states)} need work")
else:
    bad = [s for s in raw if not re.fullmatch(r"[A-Z]{2}", s)]
    if bad:
        raise ValueError(f"Not 2-letter state codes (use ALL for every state): {bad}")
    states = list(dict.fromkeys(raw))  # keep the given order, drop duplicates

print(f"{len(states)} state(s), processed one at a time: {', '.join(states)}")
dbutils.jobs.taskValues.set("states", states)
