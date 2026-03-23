"""
Exercise 6: Window Functions in Spark
======================================
Ranking, running totals, lag/lead, and moving averages.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    dense_rank,
    desc,
    lag,
    lead,
    percent_rank,
    rank,
    row_number,
    sum as spark_sum,
    to_date,
    unix_timestamp,
)
from pyspark.sql.window import Window


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

    events_df = spark.read.json(str(data_dir / "listening_events"))
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)

    # Join events with episodes to get podcast_id
    events_enriched = events_df.join(
        episodes_df.select("episode_id", "podcast_id", "duration_seconds"),
        on="episode_id",
        how="inner",
    )

    # ----------------------------------------------------------------
    # 1. Row number: number each user's events chronologically
    # ----------------------------------------------------------------
    user_window = Window.partitionBy("user_id").orderBy("timestamp")

    user_events_numbered = events_df.withColumn(
        "event_sequence", row_number().over(user_window)
    )

    print("=== Row Number: User event sequence ===")
    (
        user_events_numbered
        .filter(col("user_id") == "usr_000001")
        .select("user_id", "event_sequence", "timestamp", "episode_id", "event_type")
        .orderBy("event_sequence")
        .show(20, truncate=False)
    )

    # ----------------------------------------------------------------
    # 2. Rank: podcasts by total listening time
    # ----------------------------------------------------------------
    podcast_totals = (
        events_enriched
        .groupBy("podcast_id")
        .agg(spark_sum("listened_seconds").alias("total_seconds"))
    )

    rank_window = Window.orderBy(desc("total_seconds"))
    podcast_ranked = podcast_totals.withColumn("rank", rank().over(rank_window))

    print("=== Rank: Podcasts by total listening time ===")
    podcast_ranked.orderBy("rank").show(truncate=False)

    # ----------------------------------------------------------------
    # 3. Dense rank: users by distinct episodes listened
    # ----------------------------------------------------------------
    user_episode_counts = (
        events_df
        .groupBy("user_id")
        .agg(countDistinct("episode_id").alias("distinct_episodes"))
    )

    dense_rank_window = Window.orderBy(desc("distinct_episodes"))
    users_ranked = user_episode_counts.withColumn(
        "dense_rank", dense_rank().over(dense_rank_window)
    )

    print("=== Dense Rank: Users by distinct episodes ===")
    users_ranked.orderBy("dense_rank").show(15, truncate=False)

    # ----------------------------------------------------------------
    # 4. Running total: cumulative listened_seconds per user
    # ----------------------------------------------------------------
    running_window = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    running_totals = events_df.withColumn(
        "cumulative_seconds", spark_sum("listened_seconds").over(running_window)
    )

    print("=== Running Total: Cumulative seconds for usr_000001 ===")
    (
        running_totals
        .filter(col("user_id") == "usr_000001")
        .select("user_id", "timestamp", "listened_seconds", "cumulative_seconds")
        .orderBy("timestamp")
        .show(20, truncate=False)
    )

    # ----------------------------------------------------------------
    # 5. Lag / Lead: previous and next event info
    # ----------------------------------------------------------------
    lag_lead_df = (
        events_df
        .withColumn("prev_event_type", lag("event_type", 1).over(user_window))
        .withColumn("next_episode_id", lead("episode_id", 1).over(user_window))
        .withColumn("prev_timestamp", lag("timestamp", 1).over(user_window))
        .withColumn(
            "time_since_last_event_sec",
            unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp")),
        )
    )

    print("=== Lag/Lead: Previous and next event info ===")
    (
        lag_lead_df
        .filter(col("user_id") == "usr_000001")
        .select(
            "user_id", "timestamp", "event_type", "prev_event_type",
            "episode_id", "next_episode_id", "time_since_last_event_sec",
        )
        .orderBy("timestamp")
        .show(15, truncate=False)
    )

    # ----------------------------------------------------------------
    # 6. Percent rank: user percentile by total listening time
    # ----------------------------------------------------------------
    user_totals = events_df.groupBy("user_id").agg(
        spark_sum("listened_seconds").alias("total_seconds")
    )

    pct_window = Window.orderBy("total_seconds")
    user_percentiles = user_totals.withColumn(
        "percentile", percent_rank().over(pct_window)
    )

    print("=== Percent Rank: User percentiles by listening time ===")
    print("Top 10 users:")
    user_percentiles.orderBy(desc("total_seconds")).show(10, truncate=False)

    print("Bottom 10 users:")
    user_percentiles.orderBy("total_seconds").show(10, truncate=False)

    # ----------------------------------------------------------------
    # 7. Moving average: 7-day moving average of daily listens per podcast
    # ----------------------------------------------------------------
    daily_listens = (
        events_enriched
        .withColumn("date", to_date("timestamp"))
        .groupBy("podcast_id", "date")
        .agg(count("*").alias("daily_events"))
    )

    moving_window = (
        Window
        .partitionBy("podcast_id")
        .orderBy("date")
        .rowsBetween(-6, 0)  # current row + 6 preceding = 7 days
    )

    daily_with_ma = daily_listens.withColumn(
        "events_7day_ma", avg("daily_events").over(moving_window)
    )

    print("=== Moving Average: 7-day MA of daily events for pod_001 ===")
    (
        daily_with_ma
        .filter(col("podcast_id") == "pod_001")
        .orderBy("date")
        .show(30, truncate=False)
    )

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
