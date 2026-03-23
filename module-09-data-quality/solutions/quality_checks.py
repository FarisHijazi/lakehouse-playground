"""
Exercise 2: Custom Data Quality Checks
========================================
A reusable framework for running quality checks on any pandas dataframe.
Check types: completeness, uniqueness, range, pattern, accepted_values.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


@dataclass
class CheckResult:
    check_type: str
    column: str
    passed: bool
    total_rows: int
    failing_rows: int
    failure_pct: float
    details: str = ""

    def __str__(self):
        status = "PASS" if self.passed else "FAIL"
        return (f"[{status}] {self.check_type:20s} | {self.column:22s} | "
                f"failing: {self.failing_rows:>6,} / {self.total_rows:>6,} "
                f"({self.failure_pct:>6.2f}%) | {self.details}")


class QualityChecker:
    """Runs configurable quality checks on a pandas DataFrame."""

    def __init__(self, df: pd.DataFrame, dataset_name: str = "unknown"):
        self.df = df
        self.dataset_name = dataset_name
        self.results: list[CheckResult] = []

    def check_completeness(self, column: str, max_null_pct: float = 0.0) -> CheckResult:
        """Check that a column has nulls at or below the threshold."""
        total = len(self.df)
        nulls = self.df[column].isnull().sum()
        # Also count empty strings as missing
        if self.df[column].dtype == object:
            nulls += (self.df[column] == "").sum()
        pct = (nulls / total * 100) if total > 0 else 0
        passed = pct <= max_null_pct
        result = CheckResult(
            check_type="completeness",
            column=column,
            passed=passed,
            total_rows=total,
            failing_rows=int(nulls),
            failure_pct=round(pct, 2),
            details=f"threshold={max_null_pct}%",
        )
        self.results.append(result)
        return result

    def check_uniqueness(self, column: str) -> CheckResult:
        """Check that column values are unique (no duplicates)."""
        total = len(self.df)
        non_null = self.df[column].dropna()
        dupes = non_null.duplicated().sum()
        pct = (dupes / total * 100) if total > 0 else 0
        result = CheckResult(
            check_type="uniqueness",
            column=column,
            passed=dupes == 0,
            total_rows=total,
            failing_rows=int(dupes),
            failure_pct=round(pct, 2),
            details=f"{int(non_null.nunique())} unique values",
        )
        self.results.append(result)
        return result

    def check_range(self, column: str, min_val: float | None = None,
                    max_val: float | None = None) -> CheckResult:
        """Check that numeric values fall within [min_val, max_val]."""
        total = len(self.df)
        series = pd.to_numeric(self.df[column], errors="coerce").dropna()
        failing = pd.Series([False] * len(series), index=series.index)
        if min_val is not None:
            failing |= series < min_val
        if max_val is not None:
            failing |= series > max_val
        fail_count = int(failing.sum())
        pct = (fail_count / total * 100) if total > 0 else 0
        bounds = f"[{min_val}, {max_val}]"
        result = CheckResult(
            check_type="range",
            column=column,
            passed=fail_count == 0,
            total_rows=total,
            failing_rows=fail_count,
            failure_pct=round(pct, 2),
            details=f"expected range {bounds}",
        )
        self.results.append(result)
        return result

    def check_pattern(self, column: str, pattern: str,
                      description: str = "") -> CheckResult:
        """Check that string values match a regex pattern."""
        total = len(self.df)
        non_null = self.df[column].dropna().astype(str)
        matches = non_null.str.match(pattern)
        fail_count = int((~matches).sum())
        pct = (fail_count / total * 100) if total > 0 else 0
        result = CheckResult(
            check_type="pattern",
            column=column,
            passed=fail_count == 0,
            total_rows=total,
            failing_rows=fail_count,
            failure_pct=round(pct, 2),
            details=description or f"pattern={pattern}",
        )
        self.results.append(result)
        return result

    def check_accepted_values(self, column: str,
                              values: list) -> CheckResult:
        """Check that column values are within an accepted set."""
        total = len(self.df)
        non_null = self.df[column].dropna()
        invalid = ~non_null.isin(values)
        fail_count = int(invalid.sum())
        pct = (fail_count / total * 100) if total > 0 else 0
        if fail_count > 0:
            bad_vals = non_null[invalid].unique()[:10].tolist()
            detail = f"unexpected: {bad_vals}"
        else:
            detail = f"all values in {values}"
        result = CheckResult(
            check_type="accepted_values",
            column=column,
            passed=fail_count == 0,
            total_rows=total,
            failing_rows=fail_count,
            failure_pct=round(pct, 2),
            details=detail,
        )
        self.results.append(result)
        return result

    def summary(self):
        """Print a summary of all check results."""
        print(f"\n{'=' * 110}")
        print(f"  QUALITY CHECK RESULTS: {self.dataset_name}")
        print(f"{'=' * 110}")
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        print(f"  Total checks: {len(self.results)}  |  Passed: {passed}  |  Failed: {failed}")
        print(f"{'-' * 110}")
        for r in self.results:
            print(f"  {r}")
        print(f"{'=' * 110}")


def main():
    # ---- Users ----
    users = pd.read_csv(RAW_DIR / "users.csv")
    uc = QualityChecker(users, "users.csv")
    uc.check_completeness("user_id")
    uc.check_completeness("email")
    uc.check_completeness("name")
    uc.check_completeness("age", max_null_pct=5.0)
    uc.check_completeness("gender", max_null_pct=5.0)
    uc.check_uniqueness("user_id")
    uc.check_uniqueness("email")
    uc.check_pattern("email", r".+@.+\..+", description="valid email format")
    uc.check_pattern("user_id", r"^usr_\d{6}$", description="user_id format usr_NNNNNN")
    uc.check_range("age", min_val=13, max_val=120)
    uc.check_accepted_values("gender",
                             ["m", "f", "M", "F", "male", "female", "Male", "Female"])
    uc.check_accepted_values("subscription_type",
                             ["free", "premium", "premium_annual", "trial"])
    uc.check_accepted_values("platform",
                             ["ios", "android", "web", "car_play", "smart_speaker"])
    uc.check_pattern("signup_date", r"^\d{4}-\d{2}-\d{2}$",
                     description="ISO date format YYYY-MM-DD")
    uc.summary()

    # ---- CDN Logs ----
    cdn = pd.read_csv(RAW_DIR / "cdn_logs.csv")
    cc = QualityChecker(cdn, "cdn_logs.csv")
    cc.check_completeness("log_id")
    cc.check_completeness("event_id")
    cc.check_completeness("user_id")
    cc.check_completeness("timestamp")
    cc.check_uniqueness("log_id")
    cc.check_range("startup_time_ms", min_val=0)
    cc.check_range("bytes_transferred", min_val=1)
    cc.check_range("buffer_events", min_val=0)
    cc.check_range("rebuffer_ratio", min_val=0.0, max_val=1.0)
    cc.check_accepted_values("bitrate",
                             ["64kbps", "128kbps", "256kbps", "320kbps"])
    cc.summary()

    print("\n  All quality checks complete.")


if __name__ == "__main__":
    main()
