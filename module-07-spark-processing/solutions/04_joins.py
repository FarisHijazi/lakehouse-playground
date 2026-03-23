"""
Exercise 4: Join Operations on NYC Taxi Data
===============================================
Trips + zones (pickup and dropoff), trips + weather, trips + rate codes
+ payment types, yellow + green + FHV union/comparison, broadcast joins.

Databricks equivalent:
    - Same join syntax works in Databricks notebooks
    - broadcast() hint is often auto-applied by the Databricks optimizer
    - Use display(df) for result visualization
    - In Databricks SQL: /*+ BROADCAST(zones) */ hint in SELECT
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    lit,
    round as spark_round,
    sum as spark_sum,
    to_date,
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
    green_df = spark.read.parquet(str(data_dir / "green_tripdata_*.parquet"))
    fhvhv_df = spark.read.parquet(str(data_dir / "fhvhv_tripdata_*.parquet"))
    zones_df = spark.read.csv(str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True)
    rate_codes_df = spark.read.csv(str(data_dir / "rate_codes.csv"), header=True, inferSchema=True)
    payment_types_df = spark.read.csv(str(data_dir / "payment_types.csv"), header=True, inferSchema=True)
    weather_df = spark.read.csv(str(data_dir / "nyc_weather_2023.csv"), header=True, inferSchema=True)

    # Filter to valid trips
    yellow = yellow_df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

    print(f"Yellow trips : {yellow.count():,}")
    print(f"Green trips  : {green_df.count():,}")
    print(f"FHV trips    : {fhvhv_df.count():,}")
    print(f"Zones        : {zones_df.count()}")
    print(f"Weather days : {weather_df.count()}")
    print()

    # ----------------------------------------------------------------
    # 1. Trips + pickup zones (broadcast join for small dimension table)
    # ----------------------------------------------------------------
    # Databricks: small tables auto-broadcast; explicit hint still good practice
    pickup_zones = zones_df.select(
        col("LocationID").alias("pu_loc_id"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone"),
    )

    with_pickup = yellow.join(
        broadcast(pickup_zones),
        yellow.PULocationID == col("pu_loc_id"),
        how="left",
    ).drop("pu_loc_id")

    print("=== Join 1: Trips + Pickup Zones ===")
    with_pickup.select(
        "PULocationID", "pickup_borough", "pickup_zone", "trip_distance", "fare_amount"
    ).show(10, truncate=False)

    # ----------------------------------------------------------------
    # 2. Trips + dropoff zones
    # ----------------------------------------------------------------
    dropoff_zones = zones_df.select(
        col("LocationID").alias("do_loc_id"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
    )

    with_both_zones = with_pickup.join(
        broadcast(dropoff_zones),
        with_pickup.DOLocationID == col("do_loc_id"),
        how="left",
    ).drop("do_loc_id")

    print("=== Join 2: Trips + Pickup & Dropoff Zones ===")
    with_both_zones.select(
        "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
        "trip_distance", "fare_amount",
    ).show(10, truncate=False)

    # Popular routes: pickup -> dropoff
    print("=== Top 10 Routes (Borough to Borough) ===")
    (
        with_both_zones
        .groupBy("pickup_borough", "dropoff_borough")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy("trip_count", ascending=False)
        .show(10, truncate=False)
    )

    # ----------------------------------------------------------------
    # 3. Trips + weather (join by date)
    # ----------------------------------------------------------------
    trips_with_date = with_both_zones.withColumn(
        "pickup_date", to_date(col("tpep_pickup_datetime"))
    )

    # Ensure weather date column matches
    weather_with_date = weather_df.withColumn(
        "weather_date", to_date(col("date"))
    ).drop("date")

    trips_with_weather = trips_with_date.join(
        broadcast(weather_with_date),
        trips_with_date.pickup_date == weather_with_date.weather_date,
        how="left",
    ).drop("weather_date")

    print("=== Join 3: Trips + Weather ===")
    trips_with_weather.select(
        "pickup_date", "trip_distance", "fare_amount", "tip_amount",
    ).show(5, truncate=False)

    # Weather impact analysis
    print("=== Weather Impact on Tips ===")
    # Check available weather columns and aggregate
    print("Weather columns:", weather_df.columns)
    trips_with_weather.select(
        "pickup_date", "trip_distance", "total_amount"
    ).describe().show()

    # ----------------------------------------------------------------
    # 4. Trips + rate codes + payment types
    # ----------------------------------------------------------------
    # Databricks: these small dimension joins are auto-optimized
    fully_enriched = (
        with_both_zones
        .join(
            broadcast(rate_codes_df),
            yellow.RatecodeID == rate_codes_df.rate_code_id,
            how="left",
        )
        .join(
            broadcast(payment_types_df),
            yellow.payment_type == payment_types_df.payment_type_id,
            how="left",
        )
    )

    print("=== Join 4: Fully Enriched Trips ===")
    fully_enriched.select(
        "pickup_zone", "dropoff_zone", "rate_code_name", "payment_type_name",
        "trip_distance", "fare_amount", "tip_amount",
    ).show(10, truncate=False)

    # ----------------------------------------------------------------
    # 5. Yellow + Green + FHV union/comparison
    # ----------------------------------------------------------------
    # Align schemas: pick common columns or create them
    yellow_unified = (
        yellow
        .select(
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_distance"),
            col("total_amount"),
        )
        .withColumn("trip_type", lit("yellow"))
    )

    green_unified = (
        green_df
        .filter((col("trip_distance") > 0))
        .select(
            col("lpep_pickup_datetime").alias("pickup_datetime"),
            col("lpep_dropoff_datetime").alias("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_distance"),
            col("total_amount"),
        )
        .withColumn("trip_type", lit("green"))
    )

    fhvhv_unified = (
        fhvhv_df
        .select(
            col("pickup_datetime"),
            col("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_miles").alias("trip_distance"),
            col("base_passenger_fare").alias("total_amount"),
        )
        .withColumn("trip_type", lit("fhvhv"))
    )

    all_trips = yellow_unified.unionByName(green_unified).unionByName(fhvhv_unified)

    print("=== Union: Yellow + Green + FHV ===")
    print(f"Yellow : {yellow_unified.count():,}")
    print(f"Green  : {green_unified.count():,}")
    print(f"FHV    : {fhvhv_unified.count():,}")
    print(f"Total  : {all_trips.count():,}")

    print("\nComparison by trip type:")
    (
        all_trips
        .groupBy("trip_type")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("total_amount"), 2).alias("avg_fare"),
        )
        .orderBy("trip_count", ascending=False)
        .show(truncate=False)
    )

    # ----------------------------------------------------------------
    # 6. Broadcast join: compare explain plans
    # ----------------------------------------------------------------
    print("=== Broadcast Join: Compare Plans ===")

    # Without broadcast hint (Spark may still auto-broadcast if small enough)
    normal_join = yellow.join(
        zones_df,
        yellow.PULocationID == zones_df.LocationID,
        how="inner",
    )
    print("Plan WITHOUT explicit broadcast hint:")
    normal_join.explain()

    # With explicit broadcast hint
    broadcast_join = yellow.join(
        broadcast(zones_df),
        yellow.PULocationID == zones_df.LocationID,
        how="inner",
    )
    print("\nPlan WITH broadcast hint (look for BroadcastHashJoin):")
    broadcast_join.explain()

    # Databricks SQL equivalent:
    #   SELECT /*+ BROADCAST(z) */ t.*, z.Borough, z.Zone
    #   FROM trips t JOIN zones z ON t.PULocationID = z.LocationID

    # ----------------------------------------------------------------
    # 7. Anti join: zones with no pickups
    # ----------------------------------------------------------------
    zones_no_pickups = zones_df.join(
        yellow.select("PULocationID").distinct(),
        zones_df.LocationID == col("PULocationID"),
        how="left_anti",
    )
    print(f"\n=== Anti Join: Zones with No Pickups ===")
    print(f"Zones with no pickups: {zones_no_pickups.count()}")
    zones_no_pickups.show(truncate=False)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
