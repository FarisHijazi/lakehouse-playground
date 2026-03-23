"""
Module 06 - Exercise 5: Gold Layer -- Daily Trip Metrics
=========================================================
Aggregate daily trip-level metrics across all taxi types (yellow, green, FHV)
for dashboards and reporting.

Databricks DLT equivalent:
    @dlt.table(
        comment="Daily trip metrics by taxi type",
        table_properties={"quality": "gold"},
        partition_cols=["pickup_date"]
    )
    def gold_daily_metrics():
        yellow = dlt.read("silver_yellow_trips").withColumn("taxi_type", lit("yellow"))
        green = dlt.read("silver_green_trips").withColumn("taxi_type", lit("green"))
        fhv = dlt.read("silver_fhv_trips").withColumn("taxi_type", lit("fhv"))
        all_trips = yellow.unionByName(green, allowMissingColumns=True)
                          .unionByName(fhv, allowMissingColumns=True)
        return (
            all_trips.groupBy("pickup_date", "taxi_type")
            .agg(
                count("*").alias("total_trips"),
                sum("total_amount").alias("total_revenue"),
                avg("trip_distance").alias("avg_distance"),
                avg("tip_amount").alias("avg_tip"),
                avg("trip_duration_minutes").alias("avg_duration_minutes"),
            )
        )

Databricks SQL equivalent:
    CREATE OR REPLACE MATERIALIZED VIEW nyc_taxi.gold.daily_metrics AS
    SELECT
        pickup_date,
        taxi_type,
        COUNT(*) AS total_trips,
        SUM(total_amount) AS total_revenue,
        AVG(trip_distance) AS avg_distance,
        AVG(tip_amount) AS avg_tip,
        AVG(trip_duration_minutes) AS avg_duration_minutes,
        SUM(CASE WHEN is_airport_trip THEN 1 ELSE 0 END) / COUNT(*) * 100
            AS airport_trip_pct
    FROM nyc_taxi.silver.all_trips
    GROUP BY pickup_date, taxi_type;

Unity Catalog target:
    nyc_taxi.gold.daily_metrics
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("gold_daily_metrics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: DAILY TRIP METRICS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Silver trip tables and union with taxi_type column
        # -------------------------------------------------------------------
        # Databricks: dlt.read("silver_yellow_trips") etc.
        dfs = []

        # Yellow trips
        yellow_path = SILVER_DIR / "yellow_trips"
        if yellow_path.exists():
            yellow = spark.read.parquet(str(yellow_path))
            # Standardize column names for union
            if "tpep_pickup_datetime" in yellow.columns:
                yellow = yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
                yellow = yellow.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
            yellow = yellow.withColumn("taxi_type", F.lit("yellow"))
            dfs.append(yellow)
            print(f"  Yellow trips loaded: {yellow.count():,}")

        # Green trips
        green_path = SILVER_DIR / "green_trips"
        if green_path.exists():
            green = spark.read.parquet(str(green_path))
            green = green.withColumn("taxi_type", F.lit("green"))
            dfs.append(green)
            print(f"  Green trips loaded:  {green.count():,}")

        # FHV trips (no fare data)
        fhv_path = SILVER_DIR / "fhv_trips"
        if fhv_path.exists():
            fhv = spark.read.parquet(str(fhv_path))
            fhv = fhv.withColumn("taxi_type", F.lit("fhv"))
            dfs.append(fhv)
            print(f"  FHV trips loaded:    {fhv.count():,}")

        if not dfs:
            print("ERROR: No Silver trip data found. Run Silver layer scripts first.")
            return

        # Union all trip types using unionByName with missing column handling
        # Databricks: .unionByName(allowMissingColumns=True)
        all_trips = dfs[0]
        for other_df in dfs[1:]:
            all_trips = all_trips.unionByName(other_df, allowMissingColumns=True)

        total_trips = all_trips.count()
        print(f"\n[1] Total trips across all types: {total_trips:,}")

        # -------------------------------------------------------------------
        # 2. Daily metrics by taxi type
        # -------------------------------------------------------------------
        daily_by_type = (
            all_trips
            .groupBy("pickup_date", "taxi_type")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
                F.round(
                    F.sum(F.when(F.col("is_airport_trip"), 1).otherwise(0))
                    / F.count("*") * 100, 1
                ).alias("airport_trip_pct"),
            )
            .orderBy("pickup_date", "taxi_type")
        )

        print(f"\n[2] Daily metrics by taxi type computed")

        # -------------------------------------------------------------------
        # 3. Overall daily metrics (all types combined)
        # -------------------------------------------------------------------
        daily_overall = (
            all_trips
            .groupBy("pickup_date")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
                F.round(
                    F.sum(F.when(F.col("is_airport_trip"), 1).otherwise(0))
                    / F.count("*") * 100, 1
                ).alias("airport_trip_pct"),
            )
            .orderBy("pickup_date")
        )

        # -------------------------------------------------------------------
        # 4. Add 7-day rolling averages
        # -------------------------------------------------------------------
        # Databricks SQL: AVG(total_trips) OVER (ORDER BY pickup_date
        #     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        window_7d = (
            Window
            .orderBy("pickup_date")
            .rowsBetween(-6, 0)
        )

        daily_overall = (
            daily_overall
            .withColumn("trips_7d_avg", F.round(F.avg("total_trips").over(window_7d), 1))
            .withColumn("revenue_7d_avg", F.round(F.avg("total_revenue").over(window_7d), 2))
        )

        print(f"[3-4] Overall daily metrics with rolling averages computed")

        # -------------------------------------------------------------------
        # 5. Write output
        # -------------------------------------------------------------------
        # Databricks: .write.format("delta").mode("overwrite")
        #     .saveAsTable("nyc_taxi.gold.daily_metrics")
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        by_type_path = str(GOLD_DIR / "daily_metrics")
        daily_by_type.write.mode("overwrite").parquet(by_type_path)
        print(f"\n[5] Written to: {by_type_path}")

        overall_path = str(GOLD_DIR / "daily_metrics_overall")
        daily_overall.write.mode("overwrite").parquet(overall_path)
        print(f"    Overall: {overall_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        print(f"\n{'='*60}")
        print(f"  DAILY TRIP METRICS SUMMARY")
        print(f"{'='*60}")

        # Date range
        date_stats = daily_overall.select(
            F.min("pickup_date").alias("min_date"),
            F.max("pickup_date").alias("max_date"),
            F.count("*").alias("total_days"),
        ).collect()[0]
        print(f"  Date range: {date_stats['min_date']} to {date_stats['max_date']}")
        print(f"  Total days: {date_stats['total_days']:,}")

        # Revenue by taxi type
        print(f"\n  Revenue by taxi type:")
        daily_by_type.groupBy("taxi_type").agg(
            F.sum("total_trips").alias("total_trips"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("avg_distance"), 2).alias("avg_distance"),
        ).orderBy(F.desc("total_trips")).show(truncate=False)

        # Peak day
        peak = daily_overall.orderBy(F.desc("total_trips")).first()
        if peak:
            print(f"  Peak day: {peak['pickup_date']} with {peak['total_trips']:,} trips")

        print(f"\n  First 5 days (overall):")
        daily_overall.show(5, truncate=False)

        print(f"\n  Last 5 days (overall):")
        daily_overall.orderBy(F.desc("pickup_date")).limit(5).orderBy("pickup_date").show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
