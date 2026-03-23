"""
Module 06 - Exercise 8: Gold Layer -- Weather Impact Analysis
==============================================================
Analyze how weather conditions affect taxi usage, revenue, and tipping
behavior. Joins daily trip aggregates with NYC weather observations.

Databricks DLT equivalent:
    @dlt.table(
        comment="Weather impact on taxi trips",
        table_properties={"quality": "gold"}
    )
    def gold_weather_impact():
        daily_trips = (
            dlt.read("silver_yellow_trips")
            .groupBy("pickup_date")
            .agg(count("*").alias("total_trips"), ...)
        )
        weather = spark.table("nyc_taxi.bronze.weather")
        return daily_trips.join(weather, daily_trips.pickup_date == weather.date)

Databricks SQL equivalent:
    CREATE OR REPLACE MATERIALIZED VIEW nyc_taxi.gold.weather_impact AS
    SELECT
        t.pickup_date,
        t.total_trips,
        t.total_revenue,
        w.TMAX, w.TMIN, w.PRCP, w.SNOW, w.AWND,
        CASE WHEN w.PRCP > 0 THEN 'rainy' ELSE 'dry' END AS precip_category,
        CASE WHEN w.TMAX >= 80 THEN 'hot'
             WHEN w.TMAX >= 60 THEN 'warm'
             WHEN w.TMAX >= 40 THEN 'cool'
             ELSE 'cold' END AS temp_category
    FROM daily_trip_agg t
    JOIN nyc_taxi.bronze.weather w ON t.pickup_date = w.date;

Unity Catalog target:
    nyc_taxi.gold.weather_impact
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

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
        .appName("gold_weather_impact")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: WEATHER IMPACT ANALYSIS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Silver trips and aggregate to daily level
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

        # Daily aggregation
        daily_trips = (
            trips
            .groupBy("pickup_date")
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("total_amount"), 2).alias("avg_fare_total"),
                F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
                F.round(
                    F.when(
                        F.sum("total_amount") > 0,
                        F.sum("tip_amount") / F.sum("total_amount") * 100
                    ).otherwise(0), 2
                ).alias("tip_pct_of_revenue"),
            )
        )

        print(f"[1] Daily trip aggregates computed: {daily_trips.count()} days")

        # -------------------------------------------------------------------
        # 2. Read Bronze weather data
        # -------------------------------------------------------------------
        # Databricks: spark.table("nyc_taxi.bronze.weather")
        weather_path = str(BRONZE_DIR / "weather")
        weather = spark.read.parquet(weather_path)

        # Drop metadata columns
        weather_cols = [c for c in weather.columns if not c.startswith("_")]
        weather = weather.select(weather_cols)

        # Cast weather columns to proper types
        # Common NOAA weather columns: DATE, TMAX, TMIN, PRCP, SNOW, SNWD, AWND
        # Find the date column (could be DATE, date, etc.)
        date_col = None
        for c in weather.columns:
            if c.upper() == "DATE":
                date_col = c
                break

        if date_col is None:
            print("ERROR: No date column found in weather data.")
            return

        weather = weather.withColumn("weather_date", F.to_date(F.col(date_col)))

        # Cast numeric weather columns
        numeric_cols = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        for col_name in numeric_cols:
            # Check case-insensitive
            matching = [c for c in weather.columns if c.upper() == col_name]
            if matching:
                weather = weather.withColumn(
                    col_name.lower(),
                    F.col(matching[0]).cast(DoubleType())
                )

        print(f"[2] Weather data loaded: {weather.count()} days")
        print(f"    Weather columns: {weather.columns}")

        # -------------------------------------------------------------------
        # 3. Join trips with weather
        # -------------------------------------------------------------------
        # Databricks: Could use a broadcast join since weather is small
        daily_with_weather = (
            daily_trips
            .join(
                F.broadcast(weather),
                daily_trips["pickup_date"] == weather["weather_date"],
                "inner"
            )
            .drop("weather_date")
        )

        matched_days = daily_with_weather.count()
        print(f"[3] Joined: {matched_days} days with weather data")

        # -------------------------------------------------------------------
        # 4. Add weather category columns
        # -------------------------------------------------------------------
        # Temperature buckets (in Fahrenheit if NOAA data, tenths of degrees C
        # if from some sources -- adjust as needed)
        daily_with_weather = daily_with_weather.withColumn(
            "temp_category",
            F.when(F.col("tmax") >= 90, "hot_90plus")
            .when(F.col("tmax") >= 75, "warm_75_89")
            .when(F.col("tmax") >= 60, "mild_60_74")
            .when(F.col("tmax") >= 40, "cool_40_59")
            .when(F.col("tmax") >= 20, "cold_20_39")
            .otherwise("frigid_below_20")
        )

        # Precipitation categories
        daily_with_weather = daily_with_weather.withColumn(
            "precip_category",
            F.when(F.col("prcp").isNull() | (F.col("prcp") == 0), "dry")
            .when(F.col("prcp") <= 0.1, "light_rain")
            .when(F.col("prcp") <= 0.5, "moderate_rain")
            .otherwise("heavy_rain")
        )

        # Snow indicator
        daily_with_weather = daily_with_weather.withColumn(
            "snow_category",
            F.when(
                F.col("snow").isNull() | (F.col("snow") == 0), "no_snow"
            ).when(F.col("snow") <= 2, "light_snow")
            .otherwise("heavy_snow")
        )

        print(f"[4] Weather categories added")

        # -------------------------------------------------------------------
        # 5. Compute bucketed analysis
        # -------------------------------------------------------------------
        # Trips by precipitation category
        precip_impact = (
            daily_with_weather
            .groupBy("precip_category")
            .agg(
                F.count("*").alias("num_days"),
                F.round(F.avg("total_trips"), 0).alias("avg_daily_trips"),
                F.round(F.avg("total_revenue"), 2).alias("avg_daily_revenue"),
                F.round(F.avg("avg_tip"), 2).alias("avg_tip"),
                F.round(F.avg("avg_duration_min"), 2).alias("avg_duration_min"),
            )
            .orderBy("precip_category")
        )

        # Trips by temperature category
        temp_impact = (
            daily_with_weather
            .groupBy("temp_category")
            .agg(
                F.count("*").alias("num_days"),
                F.round(F.avg("total_trips"), 0).alias("avg_daily_trips"),
                F.round(F.avg("total_revenue"), 2).alias("avg_daily_revenue"),
                F.round(F.avg("avg_tip"), 2).alias("avg_tip"),
            )
            .orderBy("temp_category")
        )

        # Trips by snow category
        snow_impact = (
            daily_with_weather
            .groupBy("snow_category")
            .agg(
                F.count("*").alias("num_days"),
                F.round(F.avg("total_trips"), 0).alias("avg_daily_trips"),
                F.round(F.avg("total_revenue"), 2).alias("avg_daily_revenue"),
                F.round(F.avg("avg_tip"), 2).alias("avg_tip"),
                F.round(F.avg("tip_pct_of_revenue"), 2).alias("avg_tip_pct"),
            )
            .orderBy("snow_category")
        )

        # -------------------------------------------------------------------
        # 6. Compute correlations
        # -------------------------------------------------------------------
        # Correlation between weather metrics and trip volume/revenue
        # Databricks SQL: CORR(total_trips, tmax) AS trips_temp_corr
        correlations = {}
        for weather_col in ["tmax", "tmin", "prcp", "snow"]:
            if weather_col in [c.lower() for c in daily_with_weather.columns]:
                corr_trips = daily_with_weather.stat.corr("total_trips", weather_col)
                corr_revenue = daily_with_weather.stat.corr("total_revenue", weather_col)
                correlations[weather_col] = {
                    "trips_corr": round(corr_trips, 4) if corr_trips else None,
                    "revenue_corr": round(corr_revenue, 4) if corr_revenue else None,
                }

        # -------------------------------------------------------------------
        # 7. Write outputs
        # -------------------------------------------------------------------
        # Databricks: .write.format("delta").mode("overwrite")
        #     .saveAsTable("nyc_taxi.gold.weather_impact")
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        detail_path = str(GOLD_DIR / "weather_impact")
        daily_with_weather.write.mode("overwrite").parquet(detail_path)
        print(f"\n[7] Weather impact detail written to: {detail_path}")

        precip_path = str(GOLD_DIR / "weather_impact_precip")
        precip_impact.write.mode("overwrite").parquet(precip_path)
        print(f"    Precipitation impact: {precip_path}")

        temp_path = str(GOLD_DIR / "weather_impact_temp")
        temp_impact.write.mode("overwrite").parquet(temp_path)
        print(f"    Temperature impact: {temp_path}")

        snow_path = str(GOLD_DIR / "weather_impact_snow")
        snow_impact.write.mode("overwrite").parquet(snow_path)
        print(f"    Snow impact: {snow_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        print(f"\n{'='*60}")
        print(f"  WEATHER IMPACT SUMMARY")
        print(f"{'='*60}")

        print(f"\n  Correlations with trip volume and revenue:")
        for metric, corrs in correlations.items():
            print(f"    {metric}: trips_corr={corrs['trips_corr']}, "
                  f"revenue_corr={corrs['revenue_corr']}")

        print(f"\n  Precipitation impact:")
        precip_impact.show(truncate=False)

        print(f"\n  Temperature impact:")
        temp_impact.show(truncate=False)

        print(f"\n  Snow impact:")
        snow_impact.show(truncate=False)

        # Rainy day premium
        rainy_stats = daily_with_weather.filter(F.col("prcp") > 0).agg(
            F.round(F.avg("total_trips"), 0).alias("rainy_avg_trips"),
            F.round(F.avg("total_revenue"), 2).alias("rainy_avg_revenue"),
        ).collect()[0]

        dry_stats = daily_with_weather.filter(
            F.col("prcp").isNull() | (F.col("prcp") == 0)
        ).agg(
            F.round(F.avg("total_trips"), 0).alias("dry_avg_trips"),
            F.round(F.avg("total_revenue"), 2).alias("dry_avg_revenue"),
        ).collect()[0]

        if dry_stats["dry_avg_trips"] and rainy_stats["rainy_avg_trips"]:
            trip_diff = (
                (rainy_stats["rainy_avg_trips"] - dry_stats["dry_avg_trips"])
                / dry_stats["dry_avg_trips"] * 100
            )
            print(f"\n  Rainy vs Dry day comparison:")
            print(f"    Rainy day avg trips:  {rainy_stats['rainy_avg_trips']:,.0f}")
            print(f"    Dry day avg trips:    {dry_stats['dry_avg_trips']:,.0f}")
            print(f"    Difference:           {trip_diff:+.1f}%")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
