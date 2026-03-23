"""
Exercise 5: Joins - Join Users with Listening Events
=====================================================
Practice different join types and handle nulls.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, count, when


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
    users_df = spark.read.csv(str(data_dir / "users.csv"), header=True, inferSchema=True)
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)
    podcasts_df = spark.read.json(str(data_dir / "podcasts.json"), multiLine=True)

    print(f"Events count  : {events_df.count():,}")
    print(f"Users count   : {users_df.count():,}")
    print(f"Episodes count: {episodes_df.count():,}")
    print(f"Podcasts count: {podcasts_df.count():,}")
    print()

    # ----------------------------------------------------------------
    # 1. Inner join: events + users
    # ----------------------------------------------------------------
    inner_joined = events_df.join(users_df, on="user_id", how="inner")
    print("=== Inner Join: Events + Users ===")
    print(f"Result rows: {inner_joined.count():,}")
    print(f"Events lost: {events_df.count() - inner_joined.count():,}")
    inner_joined.select(
        "user_id", "event_type", "episode_id", "name", "subscription_type", "age"
    ).show(5, truncate=False)

    # ----------------------------------------------------------------
    # 2. Left join: keep all events
    # ----------------------------------------------------------------
    left_joined = events_df.join(users_df, on="user_id", how="left")
    null_user_count = left_joined.filter(col("name").isNull()).count()
    print("=== Left Join: Events + Users (keep all events) ===")
    print(f"Result rows          : {left_joined.count():,}")
    print(f"Events with null user: {null_user_count:,}")
    print()

    # ----------------------------------------------------------------
    # 3. Enrich with episodes
    # ----------------------------------------------------------------
    enriched = left_joined.join(
        episodes_df.select("episode_id", "title", "podcast_id", "duration_seconds"),
        on="episode_id",
        how="left",
    )
    print("=== Enriched with Episode metadata ===")
    enriched.select(
        "user_id", "episode_id", "title", "podcast_id", "listened_seconds", "duration_seconds"
    ).show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4. Further enrich with podcasts
    # ----------------------------------------------------------------
    fully_enriched = enriched.join(
        podcasts_df.select("podcast_id", "name_en", "category"),
        on="podcast_id",
        how="left",
    )
    print("=== Fully Enriched (with podcast name) ===")
    fully_enriched.select(
        "user_id", "name_en", "category", "title", "event_type", "listened_seconds"
    ).show(5, truncate=False)

    # ----------------------------------------------------------------
    # 5. Anti join: users who never listened
    # ----------------------------------------------------------------
    silent_users = users_df.join(events_df, on="user_id", how="left_anti")
    print("=== Anti Join: Users who never listened ===")
    print(f"Users with zero events: {silent_users.count():,}")
    silent_users.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 6. Cross join: all podcast-platform combinations
    # ----------------------------------------------------------------
    platforms_df = events_df.select("platform").distinct()
    cross = podcasts_df.select("podcast_id", "name_en").crossJoin(platforms_df)
    print("=== Cross Join: Podcasts x Platforms ===")
    print(f"Expected: {podcasts_df.count()} x {platforms_df.count()} = {podcasts_df.count() * platforms_df.count()}")
    print(f"Actual  : {cross.count()}")
    cross.orderBy("podcast_id", "platform").show(20, truncate=False)

    # ----------------------------------------------------------------
    # 7. Broadcast join: podcasts (10 rows) broadcast to all executors
    # ----------------------------------------------------------------
    print("=== Broadcast Join: Episodes + Podcasts ===")

    # Without broadcast hint
    normal_join = episodes_df.join(podcasts_df, on="podcast_id", how="inner")
    print("Plan WITHOUT broadcast hint:")
    normal_join.explain()

    # With broadcast hint
    broadcast_join = episodes_df.join(broadcast(podcasts_df), on="podcast_id", how="inner")
    print("\nPlan WITH broadcast hint:")
    broadcast_join.explain()

    print(f"Broadcast join result rows: {broadcast_join.count():,}")
    broadcast_join.select(
        "episode_id", "podcast_id", "name_en", "title", "duration_seconds"
    ).show(5, truncate=False)

    # ----------------------------------------------------------------
    # Summary of null handling
    # ----------------------------------------------------------------
    print("=== Null Summary in fully enriched DataFrame ===")
    null_counts = fully_enriched.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in fully_enriched.columns]
    )
    null_counts.show(truncate=False)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
