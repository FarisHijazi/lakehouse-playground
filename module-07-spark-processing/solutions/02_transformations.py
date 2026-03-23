"""
Exercise 3: Transformations - filter, select, withColumn, when/otherwise
========================================================================
Narrow transformations that process data without shuffling.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    lower,
    round as spark_round,
    to_date,
    trim,
    when,
)


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PodcastAnalytics")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "raw"

    # Load data
    events_df = spark.read.json(str(data_dir / "listening_events"))
    users_df = spark.read.csv(str(data_dir / "users.csv"), header=True, inferSchema=True)

    # ----------------------------------------------------------------
    # 1. Filter: listening events from Saudi Arabia
    # ----------------------------------------------------------------
    sa_events = events_df.filter(col("country") == "SA")
    print("=== Filter: Events from Saudi Arabia ===")
    print(f"Total events      : {events_df.count():,}")
    print(f"Saudi Arabia events: {sa_events.count():,}")
    sa_events.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 2. Select: keep only relevant columns
    # ----------------------------------------------------------------
    selected = sa_events.select("user_id", "episode_id", "event_type", "listened_seconds")
    print("=== Select: Relevant columns only ===")
    selected.printSchema()
    selected.show(5)

    # ----------------------------------------------------------------
    # 3. withColumn: add listened_minutes
    # ----------------------------------------------------------------
    with_minutes = selected.withColumn(
        "listened_minutes",
        spark_round(col("listened_seconds") / 60.0, 2),
    )
    print("=== withColumn: listened_minutes ===")
    with_minutes.show(10)

    # ----------------------------------------------------------------
    # 4. when/otherwise: engagement tiers
    # ----------------------------------------------------------------
    tiered = with_minutes.withColumn(
        "engagement_tier",
        when(col("listened_minutes") < 1, "bounce")
        .when(col("listened_minutes") < 10, "short")
        .when(col("listened_minutes") < 30, "medium")
        .otherwise("long"),
    )
    print("=== when/otherwise: Engagement tiers ===")
    tiered.show(10)

    print("Tier distribution:")
    tiered.groupBy("engagement_tier").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 5. Standardize gender in users
    # ----------------------------------------------------------------
    print("=== Gender before cleaning ===")
    users_df.groupBy("gender").count().orderBy("count", ascending=False).show()

    gender_cleaned = users_df.withColumn(
        "gender_clean",
        when(lower(trim(col("gender"))).isin("m", "male"), "M")
        .when(lower(trim(col("gender"))).isin("f", "female"), "F")
        .otherwise("Unknown"),
    )

    print("=== Gender after cleaning ===")
    gender_cleaned.groupBy("gender_clean").count().orderBy("count", ascending=False).show()
    gender_cleaned.select("user_id", "name", "gender", "gender_clean").show(10, truncate=False)

    # ----------------------------------------------------------------
    # 6. Parse inconsistent signup_date formats
    # ----------------------------------------------------------------
    print("=== Date parsing ===")
    print("Sample raw signup_date values:")
    users_df.select("user_id", "signup_date").show(10, truncate=False)

    date_cleaned = users_df.withColumn(
        "signup_date_parsed",
        coalesce(
            to_date(col("signup_date"), "yyyy-MM-dd"),    # 2024-09-10
            to_date(col("signup_date"), "dd-MM-yyyy"),    # 03-09-2019
            to_date(col("signup_date"), "dd/MM/yyyy"),    # 23/09/2022
            to_date(col("signup_date"), "MM-dd-yyyy"),    # 09-03-2019 (ambiguous)
        ),
    )

    print("Parsed dates:")
    date_cleaned.select("user_id", "signup_date", "signup_date_parsed").show(15, truncate=False)

    null_dates = date_cleaned.filter(col("signup_date_parsed").isNull()).count()
    print(f"Dates that could not be parsed: {null_dates}")
    print()

    # ----------------------------------------------------------------
    # 7. Explain plan -- verify no shuffle
    # ----------------------------------------------------------------
    print("=== Explain plan for narrow transformations ===")
    print("(No 'Exchange' node = no shuffle)")
    tiered.explain()

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
