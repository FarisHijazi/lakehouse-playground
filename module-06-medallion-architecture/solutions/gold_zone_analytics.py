"""
Module 06 - Exercise 6: Gold Layer -- Zone Analytics
=====================================================
Zone-level trip analytics: top pickup/dropoff zones, revenue by borough,
and origin-destination patterns.

Databricks DLT equivalent:
    @dlt.table(
        comment="Zone-level trip analytics",
        table_properties={"quality": "gold"}
    )
    def gold_zone_analytics():
        trips = dlt.read("silver_yellow_trips").unionByName(
            dlt.read("silver_green_trips"), allowMissingColumns=True)
        zones = spark.table("nyc_taxi.bronze.taxi_zones")
        return (
            trips
            .join(zones.alias("pu"), col("PULocationID") == col("pu.LocationID"))
            .join(zones.alias("do"), col("DOLocationID") == col("do.LocationID"))
            .groupBy("pu.Borough", "pu.Zone")
            .agg(count("*").alias("total_pickups"), ...)
        )

Databricks SQL equivalent:
    CREATE OR REPLACE MATERIALIZED VIEW nyc_taxi.gold.zone_analytics AS
    SELECT
        pu_zone.Borough AS pickup_borough,
        pu_zone.Zone AS pickup_zone,
        COUNT(*) AS total_pickups,
        SUM(total_amount) AS total_revenue,
        AVG(trip_distance) AS avg_distance
    FROM nyc_taxi.silver.yellow_trips t
    JOIN nyc_taxi.bronze.taxi_zones pu_zone
        ON t.PULocationID = pu_zone.LocationID
    GROUP BY pu_zone.Borough, pu_zone.Zone;

Unity Catalog target:
    nyc_taxi.gold.zone_analytics
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("gold_zone_analytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: ZONE ANALYTICS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Silver trips and Bronze taxi zones
        # -------------------------------------------------------------------
        dfs = []

        yellow_path = SILVER_DIR / "yellow_trips"
        if yellow_path.exists():
            yellow = spark.read.parquet(str(yellow_path))
            if "tpep_pickup_datetime" in yellow.columns:
                yellow = yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
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

        # Read taxi zones
        # Databricks: spark.table("nyc_taxi.bronze.taxi_zones")
        zones_path = str(BRONZE_DIR / "taxi_zones")
        zones = spark.read.parquet(zones_path)
        # Drop metadata columns from zones
        zone_cols = [c for c in zones.columns if not c.startswith("_")]
        zones = zones.select(zone_cols).dropDuplicates()

        print(f"[1] Data loaded:")
        print(f"    Trips: {trips.count():,}")
        print(f"    Zones: {zones.count():,}")

        # -------------------------------------------------------------------
        # 2. Join trips with zones for pickup and dropoff
        # -------------------------------------------------------------------
        # Databricks: Use broadcast join for small dimension tables
        # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

        # Pickup zone join
        pu_zones = zones.select(
            F.col("LocationID").alias("PU_LocationID"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone"),
        )

        # Dropoff zone join
        do_zones = zones.select(
            F.col("LocationID").alias("DO_LocationID"),
            F.col("Borough").alias("dropoff_borough"),
            F.col("Zone").alias("dropoff_zone"),
            F.col("service_zone").alias("dropoff_service_zone"),
        )

        trips_with_zones = (
            trips
            .join(
                F.broadcast(pu_zones),
                trips["PULocationID"] == pu_zones["PU_LocationID"],
                "left"
            )
            .drop("PU_LocationID")
            .join(
                F.broadcast(do_zones),
                trips["DOLocationID"] == do_zones["DO_LocationID"],
                "left"
            )
            .drop("DO_LocationID")
        )

        print(f"[2] Trips joined with zone lookup")

        # -------------------------------------------------------------------
        # 3. Top pickup zones
        # -------------------------------------------------------------------
        pickup_zones = (
            trips_with_zones
            .groupBy("pickup_borough", "pickup_zone")
            .agg(
                F.count("*").alias("total_pickups"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            )
            .orderBy(F.desc("total_pickups"))
        )

        # -------------------------------------------------------------------
        # 4. Top dropoff zones
        # -------------------------------------------------------------------
        dropoff_zones = (
            trips_with_zones
            .groupBy("dropoff_borough", "dropoff_zone")
            .agg(
                F.count("*").alias("total_dropoffs"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            )
            .orderBy(F.desc("total_dropoffs"))
        )

        # -------------------------------------------------------------------
        # 5. Revenue by borough
        # -------------------------------------------------------------------
        borough_revenue = (
            trips_with_zones
            .groupBy("pickup_borough")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("total_amount"), 2).alias("avg_total_per_trip"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            )
            .orderBy(F.desc("total_revenue"))
        )

        # -------------------------------------------------------------------
        # 6. Top OD (origin-destination) pairs
        # -------------------------------------------------------------------
        od_matrix = (
            trips_with_zones
            .groupBy("pickup_zone", "dropoff_zone")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.avg("total_amount"), 2).alias("avg_fare"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            )
            .orderBy(F.desc("trip_count"))
        )

        # -------------------------------------------------------------------
        # 7. Write outputs
        # -------------------------------------------------------------------
        # Databricks: .write.format("delta").mode("overwrite")
        #     .saveAsTable("nyc_taxi.gold.zone_analytics")
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        pickup_path = str(GOLD_DIR / "zone_analytics_pickup")
        pickup_zones.write.mode("overwrite").parquet(pickup_path)
        print(f"\n[7] Pickup zones written to: {pickup_path}")

        dropoff_path = str(GOLD_DIR / "zone_analytics_dropoff")
        dropoff_zones.write.mode("overwrite").parquet(dropoff_path)
        print(f"    Dropoff zones: {dropoff_path}")

        borough_path = str(GOLD_DIR / "zone_analytics_borough")
        borough_revenue.write.mode("overwrite").parquet(borough_path)
        print(f"    Borough revenue: {borough_path}")

        od_path = str(GOLD_DIR / "zone_analytics_od")
        od_matrix.limit(1000).write.mode("overwrite").parquet(od_path)
        print(f"    OD matrix (top 1000): {od_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        print(f"\n{'='*60}")
        print(f"  ZONE ANALYTICS SUMMARY")
        print(f"{'='*60}")

        print(f"\n  Top 10 pickup zones:")
        pickup_zones.show(10, truncate=False)

        print(f"\n  Top 10 dropoff zones:")
        dropoff_zones.show(10, truncate=False)

        print(f"\n  Revenue by borough:")
        borough_revenue.show(10, truncate=False)

        print(f"\n  Top 10 OD pairs:")
        od_matrix.show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
