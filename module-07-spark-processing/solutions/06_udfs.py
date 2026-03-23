"""
Exercise 6: UDFs (User Defined Functions)
==========================================
Custom transformations with standard UDFs and Pandas UDFs on NYC taxi data.

Databricks equivalent:
    - Same UDF syntax works in Databricks
    - Pandas UDFs benefit from Arrow optimization in Databricks Runtime
    - Register UDFs for SQL: spark.udf.register("my_udf", func, StringType())
    - In Unity Catalog, use CREATE FUNCTION for persistent UDFs
"""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, round as spark_round, udf
from pyspark.sql.types import DoubleType, StringType

# Try importing pandas for vectorized UDFs (optional dependency)
try:
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


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
    )

    # ----------------------------------------------------------------
    # 1. Simple UDF: classify trip distance into categories
    # ----------------------------------------------------------------
    def classify_distance(distance):
        """Classify trip distance into human-readable categories."""
        if distance is None:
            return "unknown"
        if distance <= 1.0:
            return "short"
        elif distance <= 5.0:
            return "medium"
        elif distance <= 15.0:
            return "long"
        else:
            return "very_long"

    classify_distance_udf = udf(classify_distance, StringType())

    trips_with_category = trips.withColumn(
        "distance_category", classify_distance_udf(col("trip_distance"))
    )

    print("=== UDF 1: Trip distance classification ===")
    trips_with_category.select("trip_distance", "distance_category").show(10, truncate=False)
    trips_with_category.groupBy("distance_category").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 2. Multi-value UDF: map borough to region
    # ----------------------------------------------------------------
    BOROUGH_REGIONS = {
        "Manhattan": "Core",
        "Brooklyn": "Core",
        "Queens": "Outer",
        "Bronx": "Outer",
        "Staten Island": "Outer",
        "EWR": "Airport",
        "Unknown": "Unknown",
    }

    def borough_to_region(borough):
        if borough is None:
            return "Unknown"
        return BOROUGH_REGIONS.get(borough, "Other")

    region_udf = udf(borough_to_region, StringType())

    trips_with_zones = trips.join(
        zones_df.select(
            col("LocationID"),
            col("Borough"),
        ),
        trips.PULocationID == zones_df.LocationID,
        how="left",
    )

    trips_with_region = trips_with_zones.withColumn(
        "region", region_udf(col("Borough"))
    )

    print("=== UDF 2: Borough to region mapping ===")
    trips_with_region.select("Borough", "region").distinct().orderBy("region", "Borough").show(
        30, truncate=False
    )

    trips_with_region.groupBy("region").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 3. Pandas UDF (Vectorized): tip percentage calculation
    # ----------------------------------------------------------------
    if HAS_PANDAS:
        @pandas_udf(DoubleType())
        def tip_pct(tip: pd.Series, fare: pd.Series) -> pd.Series:
            """Compute tip / fare * 100, handling division by zero."""
            return (tip / fare.replace(0, float("nan"))) * 100.0

        trips_with_tip_pct = trips.withColumn(
            "tip_pct",
            spark_round(tip_pct(col("tip_amount"), col("fare_amount")), 2),
        )

        print("=== Pandas UDF: Tip percentage ===")
        trips_with_tip_pct.select(
            "fare_amount", "tip_amount", "tip_pct"
        ).show(15, truncate=False)

        print("Tip percentage stats:")
        trips_with_tip_pct.select("tip_pct").describe().show()
    else:
        print("=== Pandas UDF: Skipped (pandas not installed) ===")

    # ----------------------------------------------------------------
    # 4. UDF for data cleaning: classify time of day
    # ----------------------------------------------------------------
    def classify_time_of_day(pickup_hour):
        """Classify hour into time-of-day periods."""
        if pickup_hour is None:
            return None
        if 6 <= pickup_hour < 10:
            return "morning_rush"
        elif 10 <= pickup_hour < 16:
            return "midday"
        elif 16 <= pickup_hour < 20:
            return "evening_rush"
        elif 20 <= pickup_hour < 24:
            return "evening"
        else:
            return "overnight"

    time_of_day_udf = udf(classify_time_of_day, StringType())

    trips_with_time = trips.withColumn(
        "pickup_hour", hour(col("tpep_pickup_datetime"))
    ).withColumn(
        "time_of_day", time_of_day_udf(col("pickup_hour"))
    )

    print("=== UDF 4: Time of day classification ===")
    trips_with_time.select("tpep_pickup_datetime", "pickup_hour", "time_of_day").show(
        10, truncate=False
    )

    trips_with_time.groupBy("time_of_day").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 5. Performance comparison: built-in vs UDF
    # ----------------------------------------------------------------
    print("=== Performance Comparison: Built-in vs UDF ===")

    # Built-in approach: simple arithmetic
    t0 = time.time()
    result_builtin = trips.withColumn(
        "fare_per_mile", spark_round(col("fare_amount") / col("trip_distance"), 2)
    )
    result_builtin.foreach(lambda _: None)  # force evaluation
    builtin_time = time.time() - t0

    # UDF approach: same operation
    def fare_per_mile(fare, distance):
        if fare is None or distance is None or distance == 0:
            return None
        return round(fare / distance, 2)

    fare_per_mile_udf = udf(fare_per_mile, DoubleType())

    t0 = time.time()
    result_udf = trips.withColumn(
        "fare_per_mile", fare_per_mile_udf(col("fare_amount"), col("trip_distance"))
    )
    result_udf.foreach(lambda _: None)  # force evaluation
    udf_time = time.time() - t0

    print(f"Built-in function time: {builtin_time:.2f}s")
    print(f"Python UDF time       : {udf_time:.2f}s")
    print(f"UDF overhead          : {udf_time / max(builtin_time, 0.001):.1f}x slower")
    print()
    print("Why built-in functions are faster:")
    print("  - Built-in functions run natively in the JVM (Tungsten engine)")
    print("  - Python UDFs serialize data from JVM -> Python -> JVM for each row")
    print("  - The Catalyst optimizer cannot inspect or optimize UDFs")
    print("  - Use built-in functions whenever possible; UDFs are a last resort")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
