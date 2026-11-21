"""Gold: Option 2 vs Option 3 comparison and leadership-ready summary tables.

silver.results holds the current result per PBKEY (deltas are merged in, deleted records are nulled), so gold simply
reads it for every state with a DONE full run. `as_of_release` is the address_book release that state was last brought
up to. A state appears as soon as its first full run is DONE.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("geotax.catalog", "main")
BRONZE = f"{CATALOG}.geotax_compare_bronze"
SILVER = f"{CATALOG}.geotax_compare_silver"
GOLD = f"{CATALOG}.geotax_compare_gold"

EARTH_RADIUS_M = 6371008.8
METERS_PER_MILE = 1609.344
# Points closer than this are treated as the same location.
SAME_LOCATION_M = 1.0
RATE_EPSILON = 1e-9


def _scope():
    """(ADMIN1, as_of_release): each state's latest DONE full run."""
    done = spark.read.table(f"{BRONZE}.run_control").filter("status = 'DONE' AND run_mode = 'FULL'")
    latest = done.withColumn("rn", F.row_number().over(Window.partitionBy("state").orderBy(F.col("batch_seq").desc())))
    return latest.filter("rn = 1").select(F.col("state").alias("ADMIN1"), F.col("release_version").alias("as_of_release"))


def _in_scope(df):
    return df.join(F.broadcast(_scope()), "ADMIN1")


def _haversine_m(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return F.lit(2 * EARTH_RADIUS_M) * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def _differs(a, b):
    return ~F.col(a).eqNullSafe(F.col(b))


def _option_cols(option):
    g, t = f"geo{option}", f"tax{option}"
    return [
        F.col(f"{g}_lat").alias(f"opt{option}_lat"), F.col(f"{g}_lon").alias(f"opt{option}_lon"),
        F.col(f"{g}_match_code").alias(f"opt{option}_match_code"), F.col(f"{g}_loc_code").alias(f"opt{option}_loc_code"),
        F.col(f"{g}_location_type").alias(f"opt{option}_location_type"),
        F.col(f"{g}_precision_level").alias(f"opt{option}_precision_level"), F.col(f"{g}_score").alias(f"opt{option}_score"),
        F.col(f"{t}_state_code").alias(f"opt{option}_state_code"), F.col(f"{t}_county_code").alias(f"opt{option}_county_code"),
        F.col(f"{t}_county_name").alias(f"opt{option}_county_name"), F.col(f"{t}_place_code").alias(f"opt{option}_place_code"),
        F.col(f"{t}_place_name").alias(f"opt{option}_place_name"), F.col(f"{t}_spd_districts").alias(f"opt{option}_spd_districts"),
        F.col(f"{t}_jurisdiction_key").alias(f"opt{option}_jurisdiction_key"),
        F.col(f"{t}_total_tax_rate").alias(f"opt{option}_total_tax_rate"),
        F.col(f"{t}_state_tax_rate").alias(f"opt{option}_state_tax_rate"),
        F.col(f"{t}_county_tax_rate").alias(f"opt{option}_county_tax_rate"),
        F.col(f"{t}_municipal_tax_rate").alias(f"opt{option}_municipal_tax_rate"),
    ]


@dp.materialized_view(
    name=f"{GOLD}.geocode_tax_comparison",
    comment="One row per address (latest result) that completed both pipelines in every state with a completed run, with Option 2 vs Option 3 geocode distance and Geo Tax jurisdiction/rate differences.",
    cluster_by=["ADMIN1"],
)
def geocode_tax_comparison():
    results = _in_scope(spark.read.table(f"{SILVER}.results").filter("row_status = 'OK'"))
    df = results.select(
        "PBKEY", "as_of_release", F.col("release_version").alias("record_release"), "ADMIN1", "ADMIN2", "ADDRLINE1", "CITY", "POSTALCODE",
        *_option_cols(2), *_option_cols(3),
    )
    df = df.withColumn("distance_m", _haversine_m(F.col("opt2_lat"), F.col("opt2_lon"), F.col("opt3_lat"), F.col("opt3_lon")))
    df = (
        df.withColumn("distance_miles", F.col("distance_m") / METERS_PER_MILE)
        .withColumn("location_differs", F.col("distance_m") >= SAME_LOCATION_M)
        .withColumn(
            "distance_bucket",
            F.when(F.col("distance_miles") < 0.01, "1: under 0.01 mi")
            .when(F.col("distance_miles") < 0.05, "2: 0.01-0.05 mi")
            .when(F.col("distance_miles") < 0.1, "3: 0.05-0.1 mi")
            .when(F.col("distance_miles") < 0.25, "4: 0.1-0.25 mi")
            .when(F.col("distance_miles") < 0.5, "5: 0.25-0.5 mi")
            .when(F.col("distance_miles") < 1, "6: 0.5-1 mi")
            .otherwise("7: over 1 mi"),
        )
        .withColumn("state_differs", _differs("opt2_state_code", "opt3_state_code"))
        .withColumn("county_differs", _differs("opt2_county_code", "opt3_county_code"))
        .withColumn("place_differs", _differs("opt2_place_code", "opt3_place_code"))
        .withColumn("spd_differs", _differs("opt2_spd_districts", "opt3_spd_districts"))
        .withColumn("jurisdiction_differs", _differs("opt2_jurisdiction_key", "opt3_jurisdiction_key"))
        .withColumn("total_tax_rate_delta", F.col("opt3_total_tax_rate") - F.col("opt2_total_tax_rate"))
        .withColumn("rate_differs", F.abs(F.col("total_tax_rate_delta")) > RATE_EPSILON)
    )
    return df.withColumn("tax_outcome_differs", F.col("jurisdiction_differs") | F.col("rate_differs"))


@dp.materialized_view(
    name=f"{GOLD}.comparison_summary",
    comment="Headline numbers per state plus an ALL row, at each state's as_of_release: how often the geocodes differ and how often that changes the Geo Tax result.",
)
def comparison_summary():
    c = spark.read.table(f"{GOLD}.geocode_tax_comparison")
    agg = c.rollup("ADMIN1").agg(
        F.count("*").alias("records_compared"),
        F.sum(F.col("location_differs").cast("int")).alias("location_differs_count"),
        F.round(F.avg(F.col("location_differs").cast("int")) * 100, 2).alias("location_differs_pct"),
        F.round(F.expr("percentile_approx(distance_miles, 0.5)"), 4).alias("median_distance_miles"),
        F.round(F.expr("percentile_approx(distance_miles, 0.95)"), 4).alias("p95_distance_miles"),
        F.round(F.max("distance_miles"), 4).alias("max_distance_miles"),
        F.sum(F.col("tax_outcome_differs").cast("int")).alias("tax_outcome_differs_count"),
        F.round(F.avg(F.col("tax_outcome_differs").cast("int")) * 100, 3).alias("tax_outcome_differs_pct"),
        F.sum(F.col("jurisdiction_differs").cast("int")).alias("jurisdiction_differs_count"),
        F.sum(F.col("county_differs").cast("int")).alias("county_differs_count"),
        F.sum(F.col("place_differs").cast("int")).alias("place_differs_count"),
        F.sum(F.col("spd_differs").cast("int")).alias("spd_differs_count"),
        F.sum(F.col("rate_differs").cast("int")).alias("rate_differs_count"),
        F.round(F.avg(F.abs("total_tax_rate_delta")), 6).alias("avg_abs_rate_delta"),
        F.round(F.max(F.abs("total_tax_rate_delta")), 6).alias("max_abs_rate_delta"),
    )
    out = agg.join(F.broadcast(_scope()), "ADMIN1", "left")
    return out.withColumn("ADMIN1", F.coalesce(F.col("ADMIN1"), F.lit("ALL")))


@dp.materialized_view(
    name=f"{GOLD}.distance_distribution",
    comment="Records by state and Option 2 / Option 3 distance bucket (miles), with how many in each bucket have a different Geo Tax result.",
)
def distance_distribution():
    return (
        spark.read.table(f"{GOLD}.geocode_tax_comparison")
        .groupBy("ADMIN1", "distance_bucket")
        .agg(
            F.count("*").alias("records"),
            F.sum(F.col("tax_outcome_differs").cast("int")).alias("tax_outcome_differs_count"),
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.tax_differences",
    comment="Only the records where Option 2 and Option 3 produce a different Geo Tax jurisdiction or rate, for review.",
)
def tax_differences():
    return (
        spark.read.table(f"{GOLD}.geocode_tax_comparison")
        .filter("tax_outcome_differs")
        .select(
            "as_of_release", "record_release", "PBKEY", "ADMIN1", "ADDRLINE1", "CITY", "POSTALCODE", "distance_m", "distance_miles",
            "opt2_lat", "opt2_lon", "opt3_lat", "opt3_lon",
            "opt2_match_code", "opt3_match_code", "opt2_loc_code", "opt3_loc_code",
            "state_differs", "county_differs", "place_differs", "spd_differs", "rate_differs",
            "opt2_county_name", "opt3_county_name", "opt2_place_name", "opt3_place_name",
            "opt2_spd_districts", "opt3_spd_districts",
            "opt2_total_tax_rate", "opt3_total_tax_rate", "total_tax_rate_delta",
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.pipeline_funnel",
    comment="Record counts per state at each stage (latest result per record), so leadership can see how many addresses made it through and where the rest were quarantined.",
)
def pipeline_funnel():
    r = _in_scope(spark.read.table(f"{SILVER}.results"))
    live = r.filter("row_status != 'DELETED'")
    stages = [
        (1, "Loaded from source (current records)", live),
        (2, "Passed input validation", live.filter("row_status != 'INVALID_INPUT'")),
        (3, "Option 2 geocoded", live.filter("geo2_status = 'OK'")),
        (4, "Option 3 geocoded", live.filter("geo3_status = 'OK'")),
        (5, "Option 2 Geo Tax resolved", live.filter("tax2_status = 'OK'")),
        (6, "Option 3 Geo Tax resolved", live.filter("tax3_status = 'OK'")),
        (7, "Compared (both options complete)", live.filter("row_status = 'OK'")),
        (8, "Deleted in source (record nulled)", r.filter("row_status = 'DELETED'")),
    ]
    out = None
    for order, label, df in stages:
        row = (
            df.groupBy("as_of_release", "ADMIN1")
            .agg(F.count("*").alias("records"))
            .select("as_of_release", "ADMIN1", F.lit(order).alias("stage_order"), F.lit(label).alias("stage"), "records")
        )
        out = row if out is None else out.unionByName(row)
    return out


@dp.materialized_view(
    name=f"{GOLD}.quarantine_summary",
    comment="Quarantined record counts by state, stage and failure reason (latest result per record).",
)
def quarantine_summary():
    return (
        _in_scope(spark.read.table(f"{SILVER}.quarantine_records"))
        .groupBy("as_of_release", "ADMIN1", "stage", "failure_reason")
        .agg(F.count("*").alias("records"))
    )
