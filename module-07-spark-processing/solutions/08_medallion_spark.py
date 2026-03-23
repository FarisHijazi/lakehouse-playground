"""
Exercise 10: Full Bronze -> Silver -> Gold Pipeline in PySpark
===============================================================
Implements the medallion architecture using Spark DataFrames.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    desc,
    first,
    input_file_name,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    when,
)


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PodcastAnalytics-Medallion")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_dir = Path(__file__).resolve().parent.parent.parent / "data" / "raw"
    output_dir = Path(__file__).resolve().parent.parent.parent / "data" / "spark_output"

    bronze_dir = output_dir / "bronze"
    silver_dir = output_dir / "silver"
    gold_dir = output_dir / "gold"

    batch_id = "batch_001"

    # ==================================================================
    # BRONZE LAYER: Ingest raw data as-is with metadata
    # ==================================================================
    print("=" * 70)
    print("BRONZE LAYER: Raw ingestion with metadata")
    print("=" * 70)

    # --- Podcasts ---
    podcasts_raw = (
        spark.read.json(str(raw_dir / "podcasts.json"), multiLine=True)
        .withColumn("_source_file", lit("podcasts.json"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    podcasts_raw.write.mode("overwrite").parquet(str(bronze_dir / "podcasts"))
    print(f"Bronze podcasts : {podcasts_raw.count():>8,} rows")

    # --- Episodes ---
    episodes_raw = (
        spark.read.json(str(raw_dir / "episodes.json"), multiLine=True)
        .withColumn("_source_file", lit("episodes.json"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    episodes_raw.write.mode("overwrite").parquet(str(bronze_dir / "episodes"))
    print(f"Bronze episodes : {episodes_raw.count():>8,} rows")

    # --- Users ---
    users_raw = (
        spark.read.csv(str(raw_dir / "users.csv"), header=True, inferSchema=True)
        .withColumn("_source_file", lit("users.csv"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    users_raw.write.mode("overwrite").parquet(str(bronze_dir / "users"))
    print(f"Bronze users    : {users_raw.count():>8,} rows")

    # --- Listening Events ---
    events_raw = (
        spark.read.json(str(raw_dir / "listening_events"))
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    events_raw.write.mode("overwrite").parquet(str(bronze_dir / "listening_events"))
    print(f"Bronze events   : {events_raw.count():>8,} rows")

    # --- CDN Logs ---
    cdn_raw = (
        spark.read.csv(str(raw_dir / "cdn_logs.csv"), header=True, inferSchema=True)
        .withColumn("_source_file", lit("cdn_logs.csv"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    cdn_raw.write.mode("overwrite").parquet(str(bronze_dir / "cdn_logs"))
    print(f"Bronze CDN logs : {cdn_raw.count():>8,} rows")

    # --- Ad Events ---
    ads_raw = (
        spark.read.json(str(raw_dir / "ad_events.json"), multiLine=True)
        .withColumn("_source_file", lit("ad_events.json"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    ads_raw.write.mode("overwrite").parquet(str(bronze_dir / "ad_events"))
    print(f"Bronze ad events: {ads_raw.count():>8,} rows")
    print()

    # ==================================================================
    # SILVER LAYER: Clean, standardize, enrich
    # ==================================================================
    print("=" * 70)
    print("SILVER LAYER: Cleaning and enrichment")
    print("=" * 70)

    # --- Read from Bronze ---
    podcasts_bronze = spark.read.parquet(str(bronze_dir / "podcasts"))
    episodes_bronze = spark.read.parquet(str(bronze_dir / "episodes"))
    users_bronze = spark.read.parquet(str(bronze_dir / "users"))
    events_bronze = spark.read.parquet(str(bronze_dir / "listening_events"))
    cdn_bronze = spark.read.parquet(str(bronze_dir / "cdn_logs"))
    ads_bronze = spark.read.parquet(str(bronze_dir / "ad_events"))

    # --- Clean users ---
    users_silver = (
        users_bronze
        # Standardize gender
        .withColumn(
            "gender",
            when(lower(trim(col("gender"))).isin("m", "male"), "M")
            .when(lower(trim(col("gender"))).isin("f", "female"), "F")
            .otherwise("Unknown"),
        )
        # Parse signup_date (multiple formats)
        .withColumn(
            "signup_date",
            coalesce(
                to_date(col("signup_date"), "yyyy-MM-dd"),
                to_date(col("signup_date"), "dd-MM-yyyy"),
                to_date(col("signup_date"), "dd/MM/yyyy"),
            ),
        )
        # Fill missing city
        .fillna({"city": "Unknown"})
        # Cast age and filter invalid
        .withColumn("age", col("age").cast("int"))
        .filter((col("age") >= 13) & (col("age") <= 120))
        # Drop bronze metadata (keep clean data only)
        .drop("_source_file", "_ingested_at", "_batch_id")
    )

    users_silver.write.mode("overwrite").parquet(str(silver_dir / "users"))
    print(f"Silver users    : {users_silver.count():>8,} rows")
    users_silver.show(5, truncate=False)

    # --- Clean listening events ---
    events_silver = (
        events_bronze
        # Cast and filter
        .withColumn("listened_seconds", col("listened_seconds").cast("int"))
        .filter(col("listened_seconds") > 0)
        # Parse timestamp
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        # Deduplicate by event_id
        .dropDuplicates(["event_id"])
        # Add date column for partitioning
        .withColumn("event_date", to_date(col("timestamp")))
        .drop("_source_file", "_ingested_at", "_batch_id")
    )

    # Enrich with episode/podcast info
    events_enriched = events_silver.join(
        episodes_bronze.select("episode_id", "podcast_id", "duration_seconds"),
        on="episode_id",
        how="left",
    )

    events_enriched.write.mode("overwrite").partitionBy("event_date").parquet(
        str(silver_dir / "listening_events")
    )
    print(f"Silver events   : {events_enriched.count():>8,} rows")
    events_enriched.show(5, truncate=False)

    # --- Clean podcasts and episodes (passthrough with minor cleanup) ---
    podcasts_silver = podcasts_bronze.drop("_source_file", "_ingested_at", "_batch_id")
    podcasts_silver.write.mode("overwrite").parquet(str(silver_dir / "podcasts"))
    print(f"Silver podcasts : {podcasts_silver.count():>8,} rows")

    episodes_silver = (
        episodes_bronze
        .withColumn("published_at", to_timestamp(col("published_at")))
        .drop("_source_file", "_ingested_at", "_batch_id")
    )
    episodes_silver.write.mode("overwrite").parquet(str(silver_dir / "episodes"))
    print(f"Silver episodes : {episodes_silver.count():>8,} rows")

    # CDN and ads silver
    cdn_silver = cdn_bronze.drop("_source_file", "_ingested_at", "_batch_id")
    cdn_silver.write.mode("overwrite").parquet(str(silver_dir / "cdn_logs"))
    print(f"Silver CDN logs : {cdn_silver.count():>8,} rows")

    ads_silver = ads_bronze.drop("_source_file", "_ingested_at", "_batch_id")
    ads_silver.write.mode("overwrite").parquet(str(silver_dir / "ad_events"))
    print(f"Silver ad events: {ads_silver.count():>8,} rows")
    print()

    # ==================================================================
    # GOLD LAYER: Business-level aggregations
    # ==================================================================
    print("=" * 70)
    print("GOLD LAYER: Business aggregations")
    print("=" * 70)

    # Read from Silver
    events_s = spark.read.parquet(str(silver_dir / "listening_events"))
    users_s = spark.read.parquet(str(silver_dir / "users"))
    podcasts_s = spark.read.parquet(str(silver_dir / "podcasts"))

    # --- Gold 1: Podcast Performance ---
    podcast_perf = (
        events_s
        .groupBy("podcast_id")
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_listeners"),
            spark_round(spark_sum("listened_seconds") / 3600.0, 2).alias("total_hours"),
            spark_round(avg("listened_seconds"), 2).alias("avg_seconds_per_event"),
            spark_round(
                avg(
                    when(
                        col("duration_seconds") > 0,
                        col("listened_seconds") / col("duration_seconds") * 100,
                    )
                ),
                2,
            ).alias("avg_completion_pct"),
        )
        .join(podcasts_s.select("podcast_id", "name_en", "category"), on="podcast_id", how="left")
        .orderBy(desc("total_hours"))
    )

    podcast_perf.write.mode("overwrite").parquet(str(gold_dir / "podcast_performance"))
    print("=== Gold: Podcast Performance ===")
    podcast_perf.show(truncate=False)

    # --- Gold 2: User Engagement ---
    # Find each user's most-listened podcast
    user_podcast_time = (
        events_s
        .groupBy("user_id", "podcast_id")
        .agg(spark_sum("listened_seconds").alias("podcast_seconds"))
    )

    from pyspark.sql.window import Window

    user_fav_window = Window.partitionBy("user_id").orderBy(desc("podcast_seconds"))
    user_fav_podcast = (
        user_podcast_time
        .withColumn("rn", countDistinct(lit(1)).over(user_fav_window))  # placeholder
    )
    # Simpler approach: use first() with ordering
    user_fav = (
        user_podcast_time
        .orderBy("user_id", desc("podcast_seconds"))
        .groupBy("user_id")
        .agg(first("podcast_id").alias("favorite_podcast"))
    )

    user_engagement = (
        events_s
        .groupBy("user_id")
        .agg(
            count("*").alias("total_events"),
            countDistinct("podcast_id").alias("distinct_podcasts"),
            countDistinct("episode_id").alias("distinct_episodes"),
            spark_round(spark_sum("listened_seconds") / 3600.0, 2).alias("total_hours"),
            spark_max("timestamp").alias("last_event_at"),
        )
        .withColumn(
            "days_since_last_listen",
            datediff(current_timestamp(), col("last_event_at")),
        )
        .join(user_fav, on="user_id", how="left")
        .join(
            users_s.select("user_id", "country", "subscription_type"),
            on="user_id",
            how="left",
        )
        .orderBy(desc("total_hours"))
    )

    user_engagement.write.mode("overwrite").parquet(str(gold_dir / "user_engagement"))
    print("=== Gold: User Engagement (top 20) ===")
    user_engagement.show(20, truncate=False)

    # --- Gold 3: Daily Metrics ---
    daily_metrics = (
        events_s
        .groupBy("event_date")
        .agg(
            countDistinct("user_id").alias("daily_active_users"),
            count("*").alias("total_events"),
            spark_round(spark_sum("listened_seconds") / 3600.0, 2).alias("total_hours"),
            count(when(col("event_type") == "play", True)).alias("plays"),
            count(when(col("event_type") == "pause", True)).alias("pauses"),
            count(when(col("event_type") == "complete", True)).alias("completes"),
            count(when(col("event_type") == "skip", True)).alias("skips"),
            count(when(col("event_type") == "resume", True)).alias("resumes"),
        )
        .orderBy("event_date")
    )

    daily_metrics.write.mode("overwrite").parquet(str(gold_dir / "daily_metrics"))
    print("=== Gold: Daily Metrics (sample) ===")
    daily_metrics.show(20, truncate=False)

    # ==================================================================
    # Validation summary
    # ==================================================================
    print("=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    for layer, tables in [
        ("Bronze", ["podcasts", "episodes", "users", "listening_events", "cdn_logs", "ad_events"]),
        ("Silver", ["podcasts", "episodes", "users", "listening_events", "cdn_logs", "ad_events"]),
        ("Gold", ["podcast_performance", "user_engagement", "daily_metrics"]),
    ]:
        layer_dir = output_dir / layer.lower()
        print(f"\n{layer}:")
        for t in tables:
            try:
                df = spark.read.parquet(str(layer_dir / t))
                print(f"  {t:25s}: {df.count():>10,} rows, {len(df.columns):>3} columns")
            except Exception as e:
                print(f"  {t:25s}: ERROR - {e}")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
