"""Silver: one row per failed stage of an address (current state of each record, full runs). Nothing is silently dropped.

The units already parse the API responses into silver.results (one wide row per PBKEY, latest result), so silver only
unpivots the failures into a single quarantine view. Records deleted in the source (row_status DELETED) are not quarantined.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("geotax.catalog", "main")
SILVER = f"{CATALOG}.geotax_compare_silver"

# stage name -> (status column, error column, http column, raw column) in silver.results
STAGES = {
    "opt2_geocode": ("geo2_status", "geo2_error", "geo2_http_status", "geo2_raw_response"),
    "opt3_geocode": ("geo3_status", "geo3_error", "geo3_http_status", "geo3_raw_response"),
    "opt2_geotax": ("tax2_status", "tax2_error", "tax2_http_status", "tax2_raw_response"),
    "opt3_geotax": ("tax3_status", "tax3_error", "tax3_http_status", "tax3_raw_response"),
}


@dp.materialized_view(
    name=f"{SILVER}.quarantine_records",
    comment="Every address whose latest result failed input validation or a Geocode / Geo Tax stage (full runs), one row per failed stage, with the reason. Stages skipped because an earlier stage failed are not repeated. Deleted records are excluded.",
)
def quarantine_records():
    r = spark.read.table(f"{SILVER}.results")
    cols = ["PBKEY", "release_version", "ADMIN1", "stage", "failure_reason", "http_status", "raw_response"]

    out = r.filter("row_status = 'INVALID_INPUT'").select(
        "PBKEY", "release_version", "ADMIN1",
        F.lit("input_validation").alias("stage"),
        F.col("geo2_error").alias("failure_reason"),
        F.lit(None).cast("int").alias("http_status"),
        F.lit(None).cast("string").alias("raw_response"),
    )
    for stage, (status, error, http, raw) in STAGES.items():
        failed = (
            r.filter(F.col(status).isin("NO_MATCH", "API_ERROR"))
            .select(
                "PBKEY", "release_version", "ADMIN1",
                F.lit(stage).alias("stage"),
                F.col(error).alias("failure_reason"),
                F.col(http).alias("http_status"),
                F.col(raw).alias("raw_response"),
            )
        )
        out = out.unionByName(failed.select(*cols))
    return (
        out.join(r.select("PBKEY", "ADDRLINE1", "CITY", "POSTALCODE"), "PBKEY", "left")
        .select(*cols, "ADDRLINE1", "CITY", "POSTALCODE")
        .withColumn("quarantined_at", F.current_timestamp())
    )
