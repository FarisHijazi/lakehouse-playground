"""
Exercise 7: UDFs (User Defined Functions)
==========================================
Custom transformations with standard UDFs and Pandas UDFs.
"""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, udf
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
        .appName("PodcastAnalytics")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "raw"

    events_df = spark.read.json(str(data_dir / "listening_events"))
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)
    users_df = spark.read.csv(str(data_dir / "users.csv"), header=True, inferSchema=True)

    # ----------------------------------------------------------------
    # 1. Simple UDF: extract major version from app_version
    # ----------------------------------------------------------------
    def extract_major_version(version_str):
        """Convert '4.13.27' -> 'v4'"""
        if version_str is None:
            return "unknown"
        try:
            major = version_str.split(".")[0]
            return f"v{major}"
        except (IndexError, ValueError):
            return "unknown"

    major_version_udf = udf(extract_major_version, StringType())

    events_with_version = events_df.withColumn(
        "major_version", major_version_udf(col("app_version"))
    )

    print("=== UDF 1: Major version extraction ===")
    events_with_version.select("app_version", "major_version").show(10, truncate=False)
    events_with_version.groupBy("major_version").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 2. Multi-value UDF: country code to region name
    # ----------------------------------------------------------------
    COUNTRY_REGIONS = {
        "SA": "Gulf",
        "AE": "Gulf",
        "KW": "Gulf",
        "QA": "Gulf",
        "BH": "Gulf",
        "OM": "Gulf",
        "EG": "North Africa",
        "MA": "North Africa",
        "TN": "North Africa",
        "DZ": "North Africa",
        "JO": "Levant",
        "LB": "Levant",
        "IQ": "Levant",
        "SY": "Levant",
        "PS": "Levant",
        "US": "International",
        "GB": "International",
        "FR": "International",
        "DE": "International",
        "CA": "International",
    }

    def country_to_region(country_code):
        if country_code is None:
            return "Unknown"
        return COUNTRY_REGIONS.get(country_code, "Other")

    region_udf = udf(country_to_region, StringType())

    events_with_region = events_df.withColumn(
        "region", region_udf(col("country"))
    )

    print("=== UDF 2: Country to region mapping ===")
    events_with_region.select("country", "region").distinct().orderBy("region", "country").show(
        30, truncate=False
    )

    events_with_region.groupBy("region").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 3. Pandas UDF (Vectorized): completion percentage
    # ----------------------------------------------------------------
    if HAS_PANDAS:
        @pandas_udf(DoubleType())
        def completion_pct(listened: pd.Series, duration: pd.Series) -> pd.Series:
            """Compute listened / duration * 100, handling division by zero."""
            return (listened / duration.replace(0, float("nan"))) * 100.0

        events_with_episodes = events_df.join(
            episodes_df.select("episode_id", "duration_seconds"),
            on="episode_id",
            how="inner",
        )

        events_with_completion = events_with_episodes.withColumn(
            "completion_pct",
            spark_round(completion_pct(col("listened_seconds"), col("duration_seconds")), 2),
        )

        print("=== Pandas UDF: Completion percentage ===")
        events_with_completion.select(
            "episode_id", "listened_seconds", "duration_seconds", "completion_pct"
        ).show(15, truncate=False)

        print("Completion stats:")
        events_with_completion.select("completion_pct").describe().show()
    else:
        print("=== Pandas UDF: Skipped (pandas not installed) ===")

    # ----------------------------------------------------------------
    # 4. UDF for data cleaning: normalize names
    # ----------------------------------------------------------------
    def normalize_name(name):
        """Strip extra whitespace and normalize."""
        if name is None:
            return None
        # Strip leading/trailing whitespace
        cleaned = name.strip()
        # Collapse multiple spaces into one
        cleaned = " ".join(cleaned.split())
        return cleaned

    normalize_udf = udf(normalize_name, StringType())

    users_cleaned = users_df.withColumn("name_clean", normalize_udf(col("name")))

    print("=== UDF 4: Name normalization ===")
    users_cleaned.select("user_id", "name", "name_clean").show(10, truncate=False)

    # ----------------------------------------------------------------
    # 5. Performance comparison: built-in vs UDF
    # ----------------------------------------------------------------
    print("=== Performance Comparison: Built-in vs UDF ===")

    # Built-in approach: simple arithmetic
    t0 = time.time()
    result_builtin = events_df.withColumn(
        "minutes", spark_round(col("listened_seconds") / 60.0, 2)
    )
    result_builtin.foreach(lambda _: None)  # force evaluation
    builtin_time = time.time() - t0

    # UDF approach: same operation
    def to_minutes(seconds):
        if seconds is None:
            return None
        return round(seconds / 60.0, 2)

    to_minutes_udf = udf(to_minutes, DoubleType())

    t0 = time.time()
    result_udf = events_df.withColumn("minutes", to_minutes_udf(col("listened_seconds")))
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
