"""
Exercise 3: Aggregations on NYC Taxi Trips
============================================
Revenue by borough/zone/hour, trip counts by day of week, average tips
by payment type, and group by with multiple aggregations.

Databricks equivalent:
    - Same PySpark API works in Databricks
    - Use display(result_df) for interactive charts
    - Databricks auto-generates bar/line charts from aggregations
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    countDistinct,
    dayofweek,
    hour,
    round as spark_round,
    sum as spark_sum,
    when,
)


def main():
    # Databricks: spark is pre-configured
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

    # Load data
    yellow_df = spark.read.parquet(str(data_dir / "yellow_tripdata_*.parquet"))
    zones_df = spark.read.csv(str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True)
    payment_types_df = spark.read.csv(str(data_dir / "payment_types.csv"), header=True, inferSchema=True)

    # Pre-compute useful columns
    trips = (
        yellow_df
        .filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
        .withColumn(
            "tip_pct",
            when(col("fare_amount") > 0,
                 spark_round(col("tip_amount") / col("fare_amount") * 100, 2))
            .otherwise(0),
        )
    )

    # ----------------------------------------------------------------
    # 1. Revenue by borough
    #    Join trips with zones on PULocationID, then aggregate
    # ----------------------------------------------------------------
    # Databricks: broadcast hint is often automatic for small tables
    trips_with_borough = trips.join(
        broadcast(zones_df.select(
            col("LocationID"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone"),
        )),
        trips.PULocationID == col("LocationID"),
        how="inner",
    ).drop("LocationID")

    print("=== Revenue by Borough ===")
    borough_revenue = (
        trips_with_borough
        .groupBy("pickup_borough")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
        )
        .orderBy("total_revenue", ascending=False)
    )
    borough_revenue.show(truncate=False)
    # Databricks: display(borough_revenue) then click chart icon for bar chart

    # ----------------------------------------------------------------
    # 2. Revenue by zone and hour (fine-grained)
    # ----------------------------------------------------------------
    print("=== Top 20: Revenue by Zone and Hour ===")
    zone_hour_revenue = (
        trips_with_borough
        .groupBy("pickup_zone", "pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("total_revenue", ascending=False)
    )
    zone_hour_revenue.show(20, truncate=False)

    # ----------------------------------------------------------------
    # 3. Trip counts by day of week
    # ----------------------------------------------------------------
    print("=== Trips by Day of Week (1=Sunday, 7=Saturday) ===")
    trips.groupBy("pickup_day_of_week").agg(
        count("*").alias("trip_count"),
        spark_round(avg("total_amount"), 2).alias("avg_total"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),
    ).orderBy("pickup_day_of_week").show()

    # ----------------------------------------------------------------
    # 4. Average tips by payment type
    # ----------------------------------------------------------------
    print("=== Average Tips by Payment Type ===")
    # Databricks: small dimension tables are auto-broadcast
    tips_by_payment = (
        trips
        .join(
            broadcast(payment_types_df),
            trips.payment_type == payment_types_df.payment_type_id,
            how="left",
        )
        .groupBy("payment_type_name")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
            spark_round(avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .orderBy("trip_count", ascending=False)
    )
    tips_by_payment.show(truncate=False)

    # ----------------------------------------------------------------
    # 5. Pivot: average fare by borough (rows) x hour (columns)
    # ----------------------------------------------------------------
    print("=== Pivot: Avg Fare by Borough x Hour (sample hours) ===")
    # Limit pivot values to avoid wide table in output
    sample_hours = [8, 12, 17, 22]
    pivot_table = (
        trips_with_borough
        .groupBy("pickup_borough")
        .pivot("pickup_hour", sample_hours)
        .agg(spark_round(avg("fare_amount"), 2))
        .fillna(0)
        .orderBy("pickup_borough")
    )
    pivot_table.show(truncate=False)

    # ----------------------------------------------------------------
    # 6. Multiple aggregations per zone
    # ----------------------------------------------------------------
    print("=== Zone Performance: Multiple Aggregations (top 15) ===")
    zone_performance = (
        trips_with_borough
        .groupBy("pickup_zone", "pickup_borough")
        .agg(
            count("*").alias("total_trips"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance_mi"),
            spark_round(avg(
                (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0
            ), 2).alias("avg_duration_min"),
            spark_round(avg("tip_pct"), 2).alias("avg_tip_pct"),
            countDistinct("VendorID").alias("vendor_count"),
        )
        .orderBy("total_revenue", ascending=False)
    )
    zone_performance.show(15, truncate=False)

    # ----------------------------------------------------------------
    # Compare explain plans
    # ----------------------------------------------------------------
    print("=== Explain: Simple groupBy ===")
    trips.groupBy("pickup_hour").count().explain()

    print("\n=== Explain: groupBy with join (note the Exchange nodes = shuffles) ===")
    zone_performance.explain()

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
