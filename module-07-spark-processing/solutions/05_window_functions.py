"""
Exercise 5: Window Functions in Spark
======================================
Ranking, running totals, lag/lead, and moving averages on NYC taxi data.

Databricks equivalent:
    - Same window function syntax works in Databricks notebooks
    - Use display(df) for interactive visualizations of window results
    - Window functions are heavily optimized in Databricks Runtime
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    dense_rank,
    desc,
    lag,
    lead,
    percent_rank,
    rank,
    round as spark_round,
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
        .appName("TaxiAnalytics")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    data_dir = PROJECT_ROOT / "data" / "raw"

    yellow_df = spark.read.parquet(str(data_dir / "yellow_tripdata_*.parquet"))
    zones_df = spark.read.csv(
        str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True
    )

    # Filter to valid trips
    trips = yellow_df.filter(
        (col("trip_distance") > 0) & (col("fare_amount") > 0)
    ).withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

    # ----------------------------------------------------------------
    # 1. Row number: number each zone's trips chronologically
    # ----------------------------------------------------------------
    zone_window = Window.partitionBy("PULocationID").orderBy("tpep_pickup_datetime")

    zone_trips_numbered = trips.withColumn(
        "trip_sequence", row_number().over(zone_window)
    )

    print("=== Row Number: Trip sequence per pickup zone ===")
    (
        zone_trips_numbered
        .filter(col("PULocationID") == 132)  # JFK Airport
        .select("PULocationID", "trip_sequence", "tpep_pickup_datetime",
                "trip_distance", "fare_amount")
        .orderBy("trip_sequence")
        .show(20, truncate=False)
    )

    # ----------------------------------------------------------------
    # 2. Rank: zones by total fare revenue
    # ----------------------------------------------------------------
    zone_revenue = (
        trips
        .groupBy("PULocationID")
        .agg(spark_sum("fare_amount").alias("total_fare"))
    )

    rank_window = Window.orderBy(desc("total_fare"))
    zones_ranked = (
        zone_revenue
        .withColumn("rank", rank().over(rank_window))
        .join(
            zones_df.select(
                col("LocationID"), col("Zone"), col("Borough")
            ),
            zone_revenue.PULocationID == zones_df.LocationID,
            how="left",
        )
    )

    print("=== Rank: Zones by total fare revenue ===")
    zones_ranked.select("rank", "Zone", "Borough", "total_fare").orderBy("rank").show(
        15, truncate=False
    )

    # ----------------------------------------------------------------
    # 3. Dense rank: zones by average tip percentage
    # ----------------------------------------------------------------
    zone_tips = (
        trips
        .filter(col("fare_amount") > 0)
        .groupBy("PULocationID")
        .agg(
            spark_round(avg(col("tip_amount") / col("fare_amount") * 100), 2).alias(
                "avg_tip_pct"
            ),
            count("*").alias("trip_count"),
        )
        .filter(col("trip_count") >= 100)  # Only zones with enough data
    )

    dense_rank_window = Window.orderBy(desc("avg_tip_pct"))
    zones_tip_ranked = zone_tips.withColumn(
        "dense_rank", dense_rank().over(dense_rank_window)
    )

    print("=== Dense Rank: Zones by avg tip percentage (min 100 trips) ===")
    zones_tip_ranked.orderBy("dense_rank").show(15, truncate=False)

    # ----------------------------------------------------------------
    # 4. Running total: cumulative fare revenue per zone per day
    # ----------------------------------------------------------------
    daily_zone_revenue = (
        trips
        .groupBy("PULocationID", "pickup_date")
        .agg(spark_sum("fare_amount").alias("daily_fare"))
    )

    running_window = (
        Window
        .partitionBy("PULocationID")
        .orderBy("pickup_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    running_totals = daily_zone_revenue.withColumn(
        "cumulative_fare", spark_sum("daily_fare").over(running_window)
    )

    print("=== Running Total: Cumulative fare for zone 132 (JFK Airport) ===")
    (
        running_totals
        .filter(col("PULocationID") == 132)
        .select("PULocationID", "pickup_date", "daily_fare", "cumulative_fare")
        .orderBy("pickup_date")
        .show(20, truncate=False)
    )

    # ----------------------------------------------------------------
    # 5. Lag / Lead: previous and next day fare comparison per zone
    # ----------------------------------------------------------------
    day_window = Window.partitionBy("PULocationID").orderBy("pickup_date")

    lag_lead_df = (
        daily_zone_revenue
        .withColumn("prev_day_fare", lag("daily_fare", 1).over(day_window))
        .withColumn("next_day_fare", lead("daily_fare", 1).over(day_window))
        .withColumn(
            "fare_change_pct",
            spark_round(
                (col("daily_fare") - col("prev_day_fare"))
                / col("prev_day_fare") * 100,
                2,
            ),
        )
    )

    print("=== Lag/Lead: Day-over-day fare comparison for zone 161 (Midtown) ===")
    (
        lag_lead_df
        .filter(col("PULocationID") == 161)
        .select(
            "PULocationID", "pickup_date", "daily_fare",
            "prev_day_fare", "next_day_fare", "fare_change_pct",
        )
        .orderBy("pickup_date")
        .show(15, truncate=False)
    )

    # ----------------------------------------------------------------
    # 6. Percent rank: zone percentile by total trip count
    # ----------------------------------------------------------------
    zone_counts = trips.groupBy("PULocationID").agg(
        count("*").alias("total_trips")
    )

    pct_window = Window.orderBy("total_trips")
    zone_percentiles = zone_counts.withColumn(
        "percentile", spark_round(percent_rank().over(pct_window), 4)
    )

    print("=== Percent Rank: Zone percentiles by trip count ===")
    print("Top 10 zones:")
    zone_percentiles.orderBy(desc("total_trips")).show(10, truncate=False)

    print("Bottom 10 zones:")
    zone_percentiles.orderBy("total_trips").show(10, truncate=False)

    # ----------------------------------------------------------------
    # 7. Moving average: 7-day moving average of daily trips citywide
    # ----------------------------------------------------------------
    daily_trips = (
        trips
        .groupBy("pickup_date")
        .agg(
            count("*").alias("daily_trips"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        )
    )

    moving_window = (
        Window
        .orderBy("pickup_date")
        .rowsBetween(-6, 0)  # current row + 6 preceding = 7 days
    )

    daily_with_ma = daily_trips.withColumn(
        "trips_7day_ma", spark_round(avg("daily_trips").over(moving_window), 0)
    ).withColumn(
        "fare_7day_ma", spark_round(avg("avg_fare").over(moving_window), 2)
    )

    print("=== Moving Average: 7-day MA of daily trips and fares ===")
    daily_with_ma.orderBy("pickup_date").show(30, truncate=False)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
