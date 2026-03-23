"""
Exercise 4: Aggregations - groupBy, agg, pivot, rollup, cube
=============================================================
Wide transformations that trigger shuffles.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    sum as spark_sum,
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

    events_df = spark.read.json(str(data_dir / "listening_events"))
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)

    # ----------------------------------------------------------------
    # 1. groupBy + count: events per event_type
    # ----------------------------------------------------------------
    print("=== Events by event_type ===")
    events_df.groupBy("event_type").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 2. groupBy + agg: podcast-level aggregations
    #    First join events with episodes to get podcast_id
    # ----------------------------------------------------------------
    events_with_podcast = events_df.join(
        episodes_df.select("episode_id", "podcast_id", "duration_seconds"),
        on="episode_id",
        how="inner",
    )

    podcast_stats = events_with_podcast.groupBy("podcast_id").agg(
        spark_sum("listened_seconds").alias("total_listened_seconds"),
        avg("listened_seconds").alias("avg_listened_seconds"),
        countDistinct("user_id").alias("unique_listeners"),
        countDistinct("episode_id").alias("unique_episodes"),
        count("*").alias("total_events"),
    ).orderBy("total_listened_seconds", ascending=False)

    print("=== Podcast-level statistics ===")
    podcast_stats.show(truncate=False)

    # ----------------------------------------------------------------
    # 3. pivot: event counts by platform x event_type
    # ----------------------------------------------------------------
    pivot_table = (
        events_df
        .groupBy("platform")
        .pivot("event_type")
        .count()
        .fillna(0)
        .orderBy("platform")
    )
    print("=== Pivot: Platform x Event Type ===")
    pivot_table.show(truncate=False)

    # ----------------------------------------------------------------
    # 4. rollup: country > platform with subtotals
    # ----------------------------------------------------------------
    rollup_df = (
        events_df
        .rollup("country", "platform")
        .agg(count("*").alias("event_count"))
        .orderBy(
            col("country").asc_nulls_last(),
            col("platform").asc_nulls_last(),
        )
    )
    print("=== Rollup: Country > Platform (nulls = subtotals/grand total) ===")
    rollup_df.show(40, truncate=False)

    # ----------------------------------------------------------------
    # 5. cube: country x event_type (all combinations)
    # ----------------------------------------------------------------
    cube_df = (
        events_df
        .cube("country", "event_type")
        .agg(
            count("*").alias("event_count"),
            avg("listened_seconds").alias("avg_seconds"),
        )
        .orderBy(
            col("country").asc_nulls_last(),
            col("event_type").asc_nulls_last(),
        )
    )
    print("=== Cube: Country x Event Type ===")
    cube_df.show(40, truncate=False)

    # ----------------------------------------------------------------
    # 6. Compare explain plans
    # ----------------------------------------------------------------
    print("=== Explain: Simple groupBy ===")
    events_df.groupBy("event_type").count().explain()

    print("\n=== Explain: Rollup ===")
    events_df.rollup("country", "platform").agg(count("*")).explain()

    print("\n=== Explain: Cube ===")
    events_df.cube("country", "event_type").agg(count("*")).explain()

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
