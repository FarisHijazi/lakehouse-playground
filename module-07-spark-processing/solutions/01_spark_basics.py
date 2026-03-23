"""
Exercise 1: Create a SparkSession and Read Raw Data
====================================================
Sets up PySpark locally and loads all podcast platform data files.
"""

from pathlib import Path
from pyspark.sql import SparkSession


def main():
    # ----------------------------------------------------------------
    # 1. Create a SparkSession
    # ----------------------------------------------------------------
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PodcastAnalytics")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")  # sensible default for local
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version : {spark.version}")
    print(f"App name      : {spark.sparkContext.appName}")
    print(f"Master        : {spark.sparkContext.master}")
    print()

    # ----------------------------------------------------------------
    # 2. Resolve data paths
    # ----------------------------------------------------------------
    data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "raw"
    print(f"Data directory: {data_dir}")
    print()

    # ----------------------------------------------------------------
    # 3. Read podcasts.json (JSON array -> multiLine)
    # ----------------------------------------------------------------
    podcasts_df = spark.read.json(str(data_dir / "podcasts.json"), multiLine=True)
    print("=== Podcasts ===")
    print(f"Row count: {podcasts_df.count()}")
    podcasts_df.printSchema()
    podcasts_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4. Read episodes.json (JSON array -> multiLine)
    # ----------------------------------------------------------------
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)
    print("=== Episodes ===")
    print(f"Row count: {episodes_df.count()}")
    episodes_df.printSchema()
    episodes_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 5. Read users.csv
    # ----------------------------------------------------------------
    users_df = spark.read.csv(
        str(data_dir / "users.csv"),
        header=True,
        inferSchema=True,
    )
    print("=== Users ===")
    print(f"Row count: {users_df.count()}")
    users_df.printSchema()
    users_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 6. Read listening events (daily JSONL files)
    # ----------------------------------------------------------------
    events_df = spark.read.json(str(data_dir / "listening_events"))
    print("=== Listening Events ===")
    print(f"Row count: {events_df.count()}")
    print(f"Partitions: {events_df.rdd.getNumPartitions()}")
    events_df.printSchema()
    events_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 7. Read cdn_logs.csv
    # ----------------------------------------------------------------
    cdn_df = spark.read.csv(
        str(data_dir / "cdn_logs.csv"),
        header=True,
        inferSchema=True,
    )
    print("=== CDN Logs ===")
    print(f"Row count: {cdn_df.count()}")
    cdn_df.printSchema()
    cdn_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 8. Read ad_events.json
    # ----------------------------------------------------------------
    ad_df = spark.read.json(str(data_dir / "ad_events.json"), multiLine=True)
    print("=== Ad Events ===")
    print(f"Row count: {ad_df.count()}")
    ad_df.printSchema()
    ad_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, df in [
        ("Podcasts", podcasts_df),
        ("Episodes", episodes_df),
        ("Users", users_df),
        ("Listening Events", events_df),
        ("CDN Logs", cdn_df),
        ("Ad Events", ad_df),
    ]:
        print(f"  {name:20s}: {df.count():>10,} rows, {len(df.columns):>3} columns")
    print()

    spark.stop()
    print("SparkSession stopped.")


if __name__ == "__main__":
    main()
