# Databricks notebook source
# MAGIC %md
# MAGIC # Test tool — synthetic address_book release
# MAGIC
# MAGIC `address_book` has a single baseline release today, so the delta path (ADDED / UPDATED / DELETED) cannot be exercised on real data. This notebook builds a **fake next release** in our own schema. It never writes to `address_book`.
# MAGIC
# MAGIC Creates in `<catalog>.geotax_compare_ops`:
# MAGIC - `test_releases` — the real releases plus one synthetic release (`release_seq = real max + 1`, version `TEST-<seq>`).
# MAGIC - `test_feed` — change-feed rows in the same shape as `silver.adf_usa_change_feed`:
# MAGIC   - **ADDED**: copies of real addresses of the state that are not in results yet, under new PBKEYs `TEST-ADD-<PBKEY>`.
# MAGIC   - **UPDATED**: records already in results, marked UPDATED (they are re-sent to the APIs).
# MAGIC   - **DELETED**: tombstones (only PBKEY and release fields) for records already in results.
# MAGIC
# MAGIC **How to test (use `sample_size` so only the `*_sample` tables are touched and the cost is a few hundred API calls):**
# MAGIC 1. Run `geotax_compare_job` with `states=AZ`, `sample_size=20` → BASELINE of 20 records.
# MAGIC 2. Run this notebook (`state=AZ`, `sample_size=20` so it reads `results_sample`).
# MAGIC 3. Run `geotax_compare_job` again with `states=AZ`, `sample_size=20`, `source_feed=<catalog>.geotax_compare_ops.test_feed`, `source_releases=<catalog>.geotax_compare_ops.test_releases` → DELTA: ADDED+UPDATED re-processed, DELETED nulled in `results_sample`.
# MAGIC 4. Run it a third time with the same parameters → no-op (already at the latest release).

# COMMAND ----------
import re
import sys
import os

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.getcwd(), "..", "job"))  # unit_lib.py lives in ../job
import unit_lib as L  # noqa: E402

for name, default in [
    ("state", "AZ"),
    ("sample_size", "20"),
    ("n_added", "5"),
    ("n_updated", "5"),
    ("n_deleted", "3"),
    ("catalog", "main"),
    ("source_current", "address_book.silver.adf_usa_current"),
    ("source_releases", "address_book.silver.adf_usa_releases"),
]:
    dbutils.widgets.text(name, default)

W = dbutils.widgets.get
STATE = W("state").strip().upper()
assert re.fullmatch(r"[A-Z]{2}", STATE), "state must be a 2-letter code"
RUN_MODE = "SAMPLE" if int(W("sample_size")) > 0 else "FULL"
T = L.table_names(W("catalog"), RUN_MODE)
OPS = T["ops"]  # test tables live in the ops schema, not bronze
N_ADD, N_UPD, N_DEL = int(W("n_added")), int(W("n_updated")), int(W("n_deleted"))

# COMMAND ----------
rel = spark.table(W("source_releases"))
max_seq = rel.agg(F.max("release_seq")).collect()[0][0]
NEW_SEQ, NEW_VERSION = int(max_seq) + 1, f"TEST-{int(max_seq) + 1}"

(rel.unionByName(spark.createDataFrame([(NEW_SEQ, NEW_VERSION)], "release_seq bigint, release_version string")
                 .withColumn("release_date", F.current_date()).withColumn("added", F.lit(N_ADD).cast("bigint"))
                 .withColumn("updated", F.lit(N_UPD).cast("bigint")).withColumn("deleted", F.lit(N_DEL).cast("bigint"))
                 .withColumn("row_count", F.lit(None).cast("bigint")).withColumn("finished_at", F.current_timestamp()),
                 allowMissingColumns=True)
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OPS}.test_releases"))

# COMMAND ----------
cur = spark.table(W("source_current"))
processed = spark.table(T["results"]).filter((F.col("ADMIN1") == STATE) & (F.col("row_status") != "DELETED")).select("PBKEY")
if processed.count() < N_UPD + N_DEL:
    raise RuntimeError(f"Only {processed.count()} processed records for {STATE} in {T['results']}: run the BASELINE first (need >= {N_UPD + N_DEL})")
picked = processed.orderBy(F.xxhash64("PBKEY")).limit(N_UPD + N_DEL).collect()
upd_keys = [r["PBKEY"] for r in picked[:N_UPD]]
del_keys = [r["PBKEY"] for r in picked[N_UPD:]]

stamp = [F.lit(NEW_VERSION).alias("release_version"), F.lit(NEW_SEQ).cast("bigint").alias("release_seq"),
         F.current_timestamp().alias("changed_at"), F.lit(False).alias("reappeared")]


def feed_rows(df, status, pbkey_expr=None):
    cols = [c for c in df.columns if c not in ("status", "release_version", "release_seq", "changed_at", "reappeared", "hash_spec_version")]
    out = df.select(*[(pbkey_expr if (c == "PBKEY" and pbkey_expr is not None) else F.col(c)).alias(c) for c in cols],
                    F.lit(status).alias("status"), *stamp)
    return out


feed_cols = spark.table("address_book.silver.adf_usa_change_feed").columns  # same shape as the real feed
added_src = (cur.filter((F.col("ADMIN1") == STATE) & (F.col("status") != "DELETED"))
             .join(spark.table(T["results"]).select("PBKEY"), "PBKEY", "left_anti")
             .orderBy(F.xxhash64("PBKEY")).limit(N_ADD))
added = feed_rows(added_src, "ADDED", F.concat(F.lit("TEST-ADD-"), F.col("PBKEY")))
updated = feed_rows(cur.filter(F.col("PBKEY").isin(upd_keys)), "UPDATED")
cur_schema = {f.name: f.dataType for f in cur.schema.fields}
keep = {"PBKEY"}
tomb = (cur.filter(F.col("PBKEY").isin(del_keys)).select(
    *[(F.col(c) if c in keep else F.lit(None).cast(cur_schema[c])).alias(c) for c in cur.columns if c not in ("status", "release_version", "release_seq", "changed_at", "reappeared", "hash_spec_version")],
    F.lit("DELETED").alias("status"), *stamp))

feed = added.unionByName(updated).unionByName(tomb)
feed = feed.select(*[c for c in feed_cols if c in feed.columns])
feed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OPS}.test_feed")
print(f"Synthetic release {NEW_VERSION} (seq {NEW_SEQ}) for {STATE}: {N_ADD} added, {len(upd_keys)} updated, {len(del_keys)} deleted")
print(f"  source_feed     = {OPS}.test_feed\n  source_releases = {OPS}.test_releases")
display(spark.table(f"{OPS}.test_feed").groupBy("status").count())
