"""
Module 06 - Exercise 7: Gold Layer -- Hourly Patterns
======================================================
Analyze temporal patterns in taxi usage: hourly distributions, weekday vs
weekend patterns, rush hour analysis, and day-of-week trends.

Databricks DLT equivalent:
    @dlt.table(
        comment="Hourly trip patterns for operational optimization",
        table_properties={"quality": "gold"}
    )
    def gold_hourly_patterns():
        return (
            dlt.read("silver_yellow_trips")
            .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
            .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
            .withColumn("is_weekend", col("day_of_week").isin(1, 7))
            .groupBy("pickup_hour", "is_weekend")
            .agg(count("*").alias("avg_trips"), ...)
        )

Databricks SQL equivalent:
    CREATE OR REPLACE MATERIALIZED VIEW nyc_taxi.gold.hourly_patterns AS
    SELECT
        HOUR(tpep_pickup_datetime) AS pickup_hour,
        CASE WHEN DAYOFWEEK(tpep_pickup_datetime) IN (1,7)
             THEN 'weekend' ELSE 'weekday' END AS day_type,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS avg_fare,
        AVG(tip_amount) AS avg_tip
    FROM nyc_taxi.silver.yellow_trips
    GROUP BY 1, 2;

Unity Catalog target:
    nyc_taxi.gold.hourly_patterns
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
        .appName("gold_hourly_patterns")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: HOURLY PATTERNS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Silver trips (yellow + green)
        # -------------------------------------------------------------------
        dfs = []

        yellow_path = SILVER_DIR / "yellow_trips"
        if yellow_path.exists():
            yellow = spark.read.parquet(str(yellow_path))
            if "tpep_pickup_datetime" in yellow.columns:
                yellow = yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
                yellow = yellow.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
            yellow = yellow.withColumn("taxi_type", F.lit("yellow"))
            dfs.append(yellow)

        green_path = SILVER_DIR / "green_trips"
        if green_path.exists():
            green = spark.read.parquet(str(green_path))
            green = green.withColumn("taxi_type", F.lit("green"))
            dfs.append(green)

        if not dfs:
            print("ERROR: No Silver trip data found.")
            return

        trips = dfs[0]
        for other_df in dfs[1:]:
            trips = trips.unionByName(other_df, allowMissingColumns=True)

        # Add temporal columns
        trips = (
            trips
            .withColumn("pickup_hour", F.hour("pickup_datetime"))
            .withColumn("day_of_week", F.dayofweek("pickup_datetime"))
            # Spark: 1=Sun, 2=Mon, ..., 7=Sat
            .withColumn(
                "day_name",
                F.when(F.col("day_of_week") == 1, "Sunday")
                .when(F.col("day_of_week") == 2, "Monday")
                .when(F.col("day_of_week") == 3, "Tuesday")
                .when(F.col("day_of_week") == 4, "Wednesday")
                .when(F.col("day_of_week") == 5, "Thursday")
                .when(F.col("day_of_week") == 6, "Friday")
                .when(F.col("day_of_week") == 7, "Saturday")
            )
            .withColumn(
                "is_weekend",
                F.col("day_of_week").isin(1, 7)  # Sun, Sat
            )
            .withColumn(
                "day_type",
                F.when(F.col("is_weekend"), "weekend").otherwise("weekday")
            )
            .withColumn(
                "rush_category",
                F.when(
                    (~F.col("is_weekend"))
                    & F.col("pickup_hour").between(7, 9),
                    "morning_rush"
                ).when(
                    (~F.col("is_weekend"))
                    & F.col("pickup_hour").between(17, 19),
                    "evening_rush"
                ).otherwise("non_rush")
            )
        )

        total_count = trips.count()
        print(f"\n[1] Total trips loaded: {total_count:,}")

        # -------------------------------------------------------------------
        # 2. Hourly distribution by day type
        # -------------------------------------------------------------------
        # Databricks SQL: GROUP BY pickup_hour, day_type
        num_days = trips.select(F.countDistinct("pickup_date")).collect()[0][0]

        hourly_by_daytype = (
            trips
            .groupBy("pickup_hour", "day_type")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            )
            .orderBy("day_type", "pickup_hour")
        )

        print(f"[2] Hourly distribution by day type computed ({num_days} unique days)")

        # -------------------------------------------------------------------
        # 3. Rush hour analysis
        # -------------------------------------------------------------------
        rush_analysis = (
            trips
            .groupBy("rush_category")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
                F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
                F.round(
                    F.sum(F.when(F.col("is_airport_trip"), 1).otherwise(0))
                    / F.count("*") * 100, 1
                ).alias("airport_trip_pct"),
            )
            .orderBy("rush_category")
        )

        print(f"[3] Rush hour analysis computed")

        # -------------------------------------------------------------------
        # 4. Day of week analysis
        # -------------------------------------------------------------------
        dow_analysis = (
            trips
            .groupBy("day_of_week", "day_name")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            )
            .orderBy("day_of_week")
        )

        print(f"[4] Day of week analysis computed")

        # -------------------------------------------------------------------
        # 5. Weekday rush vs weekend same hours comparison
        # -------------------------------------------------------------------
        # Compare weekday 7-9AM and 5-7PM vs those same hours on weekends
        rush_hours_mask = (
            F.col("pickup_hour").between(7, 9)
            | F.col("pickup_hour").between(17, 19)
        )

        rush_comparison = (
            trips
            .filter(rush_hours_mask)
            .groupBy("day_type")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            )
        )

        # -------------------------------------------------------------------
        # 6. Write outputs
        # -------------------------------------------------------------------
        # Databricks: .write.format("delta").mode("overwrite")
        #     .saveAsTable("nyc_taxi.gold.hourly_patterns")
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        hourly_path = str(GOLD_DIR / "hourly_patterns")
        hourly_by_daytype.write.mode("overwrite").parquet(hourly_path)
        print(f"\n[6] Hourly patterns written to: {hourly_path}")

        rush_path = str(GOLD_DIR / "hourly_patterns_rush")
        rush_analysis.write.mode("overwrite").parquet(rush_path)
        print(f"    Rush analysis: {rush_path}")

        dow_path = str(GOLD_DIR / "hourly_patterns_dow")
        dow_analysis.write.mode("overwrite").parquet(dow_path)
        print(f"    Day of week: {dow_path}")

        comparison_path = str(GOLD_DIR / "hourly_patterns_rush_comparison")
        rush_comparison.write.mode("overwrite").parquet(comparison_path)
        print(f"    Rush comparison: {comparison_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        print(f"\n{'='*60}")
        print(f"  HOURLY PATTERNS SUMMARY")
        print(f"{'='*60}")

        print(f"\n  Hourly distribution (weekday vs weekend):")
        hourly_by_daytype.show(48, truncate=False)

        print(f"\n  Rush hour analysis:")
        rush_analysis.show(truncate=False)

        print(f"\n  Day of week analysis:")
        dow_analysis.show(truncate=False)

        print(f"\n  Weekday rush vs weekend same hours:")
        rush_comparison.show(truncate=False)

        # Peak hour
        peak_hour = hourly_by_daytype.orderBy(F.desc("total_trips")).first()
        if peak_hour:
            print(f"  Peak hour: {peak_hour['pickup_hour']}:00 ({peak_hour['day_type']}) "
                  f"with {peak_hour['total_trips']:,} trips")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
