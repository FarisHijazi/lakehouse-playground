"""
Exercise 7: Spark SQL - Register Temp Views and Query with SQL
===============================================================
Demonstrates SQL queries on Spark DataFrames via temporary views
using NYC taxi data.

Databricks equivalent:
    - Same SQL syntax works in Databricks SQL notebooks (%sql magic)
    - Temp views are notebook-scoped; use CREATE TABLE for persistence
    - In Unity Catalog: CREATE VIEW catalog.schema.my_view AS ...
    - display() renders SQL results as interactive tables/charts
"""

from pathlib import Path

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TaxiAnalytics")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    data_dir = PROJECT_ROOT / "data" / "raw"

    # Load all data
    yellow_df = spark.read.parquet(str(data_dir / "yellow_tripdata_*.parquet"))
    green_df = spark.read.parquet(str(data_dir / "green_tripdata_*.parquet"))
    zones_df = spark.read.csv(
        str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True
    )
    payment_types_df = spark.read.csv(
        str(data_dir / "payment_types.csv"), header=True, inferSchema=True
    )
    weather_df = spark.read.csv(
        str(data_dir / "nyc_weather_2023.csv"), header=True, inferSchema=True
    )

    # ----------------------------------------------------------------
    # 1. Register all DataFrames as temp views
    # ----------------------------------------------------------------
    yellow_df.createOrReplaceTempView("yellow_trips")
    green_df.createOrReplaceTempView("green_trips")
    zones_df.createOrReplaceTempView("zones")
    payment_types_df.createOrReplaceTempView("payment_types")
    weather_df.createOrReplaceTempView("weather")

    print("=== Registered temp views ===")
    spark.sql("SHOW TABLES").show()

    # ----------------------------------------------------------------
    # 2. Basic query: top 10 pickup zones by total fare
    # ----------------------------------------------------------------
    print("=== Top 10 Pickup Zones by Total Fare ===")
    spark.sql("""
        SELECT
            z.Borough,
            z.Zone,
            COUNT(*)                                   AS total_trips,
            ROUND(SUM(t.fare_amount), 2)               AS total_fare,
            ROUND(AVG(t.fare_amount), 2)               AS avg_fare
        FROM yellow_trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        WHERE t.fare_amount > 0 AND t.trip_distance > 0
        GROUP BY z.Borough, z.Zone
        ORDER BY total_fare DESC
        LIMIT 10
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 3. Join query: trips enriched with zone and payment info
    # ----------------------------------------------------------------
    print("=== Revenue by Borough and Payment Type ===")
    spark.sql("""
        SELECT
            z.Borough,
            pt.payment_type_name,
            COUNT(*)                                   AS trip_count,
            ROUND(SUM(t.total_amount), 2)              AS total_revenue,
            ROUND(AVG(t.tip_amount), 2)                AS avg_tip
        FROM yellow_trips t
        JOIN zones z              ON t.PULocationID = z.LocationID
        JOIN payment_types pt     ON t.payment_type = pt.payment_type_id
        WHERE t.fare_amount > 0
        GROUP BY z.Borough, pt.payment_type_name
        ORDER BY total_revenue DESC
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 4. CTE query: zones whose total revenue exceeds the average
    # ----------------------------------------------------------------
    print("=== Zones Above Average Revenue (top 20) ===")
    spark.sql("""
        WITH zone_revenue AS (
            SELECT
                PULocationID,
                SUM(total_amount) AS total_revenue
            FROM yellow_trips
            WHERE fare_amount > 0
            GROUP BY PULocationID
        ),
        avg_revenue AS (
            SELECT AVG(total_revenue) AS avg_rev FROM zone_revenue
        )
        SELECT
            zr.PULocationID,
            z.Zone,
            z.Borough,
            ROUND(zr.total_revenue, 2)   AS total_revenue,
            ROUND(ar.avg_rev, 2)         AS avg_zone_revenue
        FROM zone_revenue zr
        CROSS JOIN avg_revenue ar
        LEFT JOIN zones z ON zr.PULocationID = z.LocationID
        WHERE zr.total_revenue > ar.avg_rev
        ORDER BY zr.total_revenue DESC
        LIMIT 20
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 5. Window function in SQL: rank zones within each borough
    # ----------------------------------------------------------------
    print("=== Zone Rank by Trip Count within Borough (top 3 per borough) ===")
    spark.sql("""
        WITH zone_trips AS (
            SELECT
                t.PULocationID,
                z.Borough,
                z.Zone,
                COUNT(*) AS trip_count
            FROM yellow_trips t
            JOIN zones z ON t.PULocationID = z.LocationID
            WHERE t.fare_amount > 0 AND z.Borough != 'Unknown'
            GROUP BY t.PULocationID, z.Borough, z.Zone
        ),
        ranked AS (
            SELECT
                Borough,
                Zone,
                trip_count,
                RANK() OVER (PARTITION BY Borough ORDER BY trip_count DESC) AS borough_rank
            FROM zone_trips
        )
        SELECT *
        FROM ranked
        WHERE borough_rank <= 3
        ORDER BY Borough, borough_rank
    """).show(40, truncate=False)

    # ----------------------------------------------------------------
    # 6. Subquery: zones with no yellow taxi pickups
    # ----------------------------------------------------------------
    print("=== Zones with No Yellow Taxi Pickups ===")
    no_pickups = spark.sql("""
        SELECT LocationID, Borough, Zone, service_zone
        FROM zones
        WHERE LocationID NOT IN (
            SELECT DISTINCT PULocationID FROM yellow_trips
        )
        ORDER BY Borough, Zone
    """)
    print(f"Zones with zero yellow taxi pickups: {no_pickups.count()}")
    no_pickups.show(20, truncate=False)

    # ----------------------------------------------------------------
    # 7. CASE WHEN: trip distance categories and fare analysis
    # ----------------------------------------------------------------
    print("=== Trip Distance Categories ===")
    spark.sql("""
        WITH categorized AS (
            SELECT
                CASE
                    WHEN trip_distance <= 1.0  THEN 'short (0-1 mi)'
                    WHEN trip_distance <= 5.0  THEN 'medium (1-5 mi)'
                    WHEN trip_distance <= 15.0 THEN 'long (5-15 mi)'
                    ELSE 'very_long (15+ mi)'
                END AS distance_category,
                fare_amount,
                tip_amount,
                trip_distance
            FROM yellow_trips
            WHERE trip_distance > 0 AND fare_amount > 0
        )
        SELECT
            distance_category,
            COUNT(*)                        AS trip_count,
            ROUND(AVG(fare_amount), 2)      AS avg_fare,
            ROUND(AVG(tip_amount), 2)       AS avg_tip,
            ROUND(AVG(trip_distance), 2)    AS avg_distance
        FROM categorized
        GROUP BY distance_category
        ORDER BY avg_distance
    """).show(truncate=False)

    # ----------------------------------------------------------------
    # 8. Compare SQL vs DataFrame API: same query, same plan
    # ----------------------------------------------------------------
    print("=== Comparison: SQL vs DataFrame API ===")

    # SQL version
    sql_result = spark.sql("""
        SELECT PULocationID, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
        FROM yellow_trips
        WHERE trip_distance > 0 AND fare_amount > 0
        GROUP BY PULocationID
        ORDER BY trip_count DESC
    """)

    # DataFrame API version
    from pyspark.sql.functions import avg, col, count

    df_result = (
        yellow_df
        .filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
        .groupBy("PULocationID")
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
        )
        .orderBy(col("trip_count").desc())
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
