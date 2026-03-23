"""
Exercise 9: Spark SQL - Register Temp Views and Query with SQL
===============================================================
Demonstrates SQL queries on Spark DataFrames via temporary views.
"""

from pathlib import Path

from pyspark.sql import SparkSession


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

    # Load all data
    users_df = spark.read.csv(str(data_dir / "users.csv"), header=True, inferSchema=True)
    events_df = spark.read.json(str(data_dir / "listening_events"))
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)
    podcasts_df = spark.read.json(str(data_dir / "podcasts.json"), multiLine=True)

    # ----------------------------------------------------------------
    # 1. Register all DataFrames as temp views
    # ----------------------------------------------------------------
    users_df.createOrReplaceTempView("users")
    events_df.createOrReplaceTempView("listening_events")
    episodes_df.createOrReplaceTempView("episodes")
    podcasts_df.createOrReplaceTempView("podcasts")

    print("=== Registered temp views ===")
    spark.sql("SHOW TABLES").show()

    # ----------------------------------------------------------------
    # 2. Basic query: top 10 episodes by total listening seconds
    # ----------------------------------------------------------------
    print("=== Top 10 Episodes by Total Listening Time ===")
    spark.sql("""
        SELECT
            e.episode_id,
            e.title,
            COUNT(*)                           AS total_events,
            SUM(le.listened_seconds)           AS total_seconds,
            ROUND(SUM(le.listened_seconds) / 3600.0, 2) AS total_hours
        FROM listening_events le
        JOIN episodes e ON le.episode_id = e.episode_id
        GROUP BY e.episode_id, e.title
        ORDER BY total_seconds DESC
        LIMIT 10
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 3. Join query: total listening hours per podcast
    # ----------------------------------------------------------------
    print("=== Total Listening Hours per Podcast ===")
    spark.sql("""
        SELECT
            p.podcast_id,
            p.name_en                           AS podcast_name,
            p.category,
            COUNT(DISTINCT le.user_id)          AS unique_listeners,
            COUNT(*)                            AS total_events,
            ROUND(SUM(le.listened_seconds) / 3600.0, 1) AS total_hours
        FROM listening_events le
        JOIN episodes e   ON le.episode_id = e.episode_id
        JOIN podcasts p   ON e.podcast_id  = p.podcast_id
        GROUP BY p.podcast_id, p.name_en, p.category
        ORDER BY total_hours DESC
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 4. CTE query: users whose total listening time exceeds the average
    # ----------------------------------------------------------------
    print("=== Users Above Average Listening Time (top 20) ===")
    spark.sql("""
        WITH user_totals AS (
            SELECT
                user_id,
                SUM(listened_seconds) AS total_seconds
            FROM listening_events
            GROUP BY user_id
        ),
        avg_total AS (
            SELECT AVG(total_seconds) AS avg_seconds FROM user_totals
        )
        SELECT
            ut.user_id,
            ROUND(ut.total_seconds / 3600.0, 2) AS total_hours,
            ROUND(at.avg_seconds / 3600.0, 2)   AS avg_hours
        FROM user_totals ut
        CROSS JOIN avg_total at
        WHERE ut.total_seconds > at.avg_seconds
        ORDER BY ut.total_seconds DESC
        LIMIT 20
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 5. Window function in SQL: rank users within each country
    # ----------------------------------------------------------------
    print("=== User Rank by Listening Time within Country (top 5 per country) ===")
    spark.sql("""
        WITH user_country AS (
            SELECT
                le.user_id,
                le.country,
                SUM(le.listened_seconds) AS total_seconds
            FROM listening_events le
            GROUP BY le.user_id, le.country
        ),
        ranked AS (
            SELECT
                user_id,
                country,
                total_seconds,
                RANK() OVER (PARTITION BY country ORDER BY total_seconds DESC) AS country_rank
            FROM user_country
        )
        SELECT *
        FROM ranked
        WHERE country_rank <= 5
        ORDER BY country, country_rank
    """).show(40, truncate=False)

    # ----------------------------------------------------------------
    # 6. Subquery: episodes never listened to
    # ----------------------------------------------------------------
    print("=== Episodes Never Listened To ===")
    never_listened = spark.sql("""
        SELECT episode_id, title, podcast_id, duration_seconds
        FROM episodes
        WHERE episode_id NOT IN (
            SELECT DISTINCT episode_id FROM listening_events
        )
        ORDER BY podcast_id, episode_id
    """)
    print(f"Episodes with zero listens: {never_listened.count()}")
    never_listened.show(20, truncate=False)

    # ----------------------------------------------------------------
    # 7. CASE WHEN: user engagement levels
    # ----------------------------------------------------------------
    print("=== User Engagement Levels ===")
    spark.sql("""
        WITH user_events AS (
            SELECT user_id, COUNT(*) AS event_count
            FROM listening_events
            GROUP BY user_id
        )
        SELECT
            CASE
                WHEN event_count < 10  THEN 'inactive'
                WHEN event_count < 50  THEN 'casual'
                WHEN event_count < 200 THEN 'active'
                ELSE 'power_user'
            END AS engagement_level,
            COUNT(*)        AS user_count,
            MIN(event_count) AS min_events,
            MAX(event_count) AS max_events,
            ROUND(AVG(event_count), 1) AS avg_events
        FROM user_events
        GROUP BY
            CASE
                WHEN event_count < 10  THEN 'inactive'
                WHEN event_count < 50  THEN 'casual'
                WHEN event_count < 200 THEN 'active'
                ELSE 'power_user'
            END
        ORDER BY avg_events
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 8. Compare SQL vs DataFrame API: same query, same plan
    # ----------------------------------------------------------------
    print("=== Comparison: SQL vs DataFrame API ===")

    # SQL version
    sql_result = spark.sql("""
        SELECT country, COUNT(*) AS event_count, AVG(listened_seconds) AS avg_seconds
        FROM listening_events
        WHERE event_type = 'complete'
        GROUP BY country
        ORDER BY event_count DESC
    """)

    # DataFrame API version
    from pyspark.sql.functions import avg, col, count

    df_result = (
        events_df
        .filter(col("event_type") == "complete")
        .groupBy("country")
        .agg(
            count("*").alias("event_count"),
            avg("listened_seconds").alias("avg_seconds"),
        )
        .orderBy(col("event_count").desc())
    )

    print("SQL result:")
    sql_result.show()

    print("DataFrame API result:")
    df_result.show()

    print("SQL explain plan:")
    sql_result.explain()

    print("\nDataFrame API explain plan:")
    df_result.explain()

    print("\nBoth produce the same physical plan -- the Catalyst optimizer")
    print("treats SQL and DataFrame API identically.")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
