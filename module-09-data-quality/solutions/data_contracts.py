"""
Exercise 9: Data Contracts
============================
Loads data contract YAML files and validates datasets against them.
Checks schema (columns, types, nullability, uniqueness) and quality rules.
"""

import json
import re
from pathlib import Path

import pandas as pd
import yaml

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"
CONTRACTS_DIR = Path(__file__).parent.parent / "contracts"

# Map contract type names to pandas dtype checks
TYPE_MAP = {
    "string": "object",
    "integer": "int64",
    "float": "float64",
}


def load_contract(contract_path: Path) -> dict:
    """Load a YAML contract file."""
    with open(contract_path) as f:
        return yaml.safe_load(f)


def validate_schema(df: pd.DataFrame, contract: dict) -> list[dict]:
    """Validate that expected columns exist with correct properties."""
    results = []
    schema = contract.get("schema", [])

    for field in schema:
        col_name = field["name"]
        expected_type = field.get("type", "string")
        nullable = field.get("nullable", True)
        unique = field.get("unique", False)

        # Column existence
        if col_name not in df.columns:
            results.append({
                "check": "column_exists",
                "column": col_name,
                "passed": False,
                "detail": f"Column '{col_name}' is missing from the dataset",
            })
            continue

        results.append({
            "check": "column_exists",
            "column": col_name,
            "passed": True,
            "detail": f"Column '{col_name}' exists",
        })

        # Nullability
        if not nullable:
            null_count = df[col_name].isnull().sum()
            if df[col_name].dtype == object:
                null_count += (df[col_name] == "").sum()
            passed = null_count == 0
            results.append({
                "check": "not_nullable",
                "column": col_name,
                "passed": passed,
                "detail": f"{null_count:,} null/empty values"
                          if not passed else "No nulls",
            })

        # Uniqueness
        if unique:
            dupes = df[col_name].dropna().duplicated().sum()
            passed = dupes == 0
            results.append({
                "check": "unique",
                "column": col_name,
                "passed": passed,
                "detail": f"{dupes:,} duplicate values"
                          if not passed else "All values unique",
            })

    return results


def validate_quality_rules(df: pd.DataFrame, contract: dict) -> list[dict]:
    """Validate quality rules defined in the contract."""
    results = []
    rules = contract.get("quality_rules", [])

    for rule in rules:
        col = rule.get("column")
        rule_type = rule.get("rule")
        severity = rule.get("severity", "warning")

        if col not in df.columns:
            results.append({
                "check": f"quality_rule_{rule_type}",
                "column": col,
                "passed": False,
                "severity": severity,
                "detail": f"Column '{col}' not found",
            })
            continue

        series = df[col].dropna()

        if rule_type == "accepted_values":
            allowed = rule.get("values", [])
            invalid = ~series.isin(allowed)
            fail_count = int(invalid.sum())
            passed = fail_count == 0
            bad_vals = series[invalid].unique()[:5].tolist() if fail_count > 0 else []
            results.append({
                "check": "accepted_values",
                "column": col,
                "passed": passed,
                "severity": severity,
                "detail": f"{fail_count:,} invalid values"
                          + (f" (e.g., {bad_vals})" if bad_vals else ""),
            })

        elif rule_type == "range":
            min_val = rule.get("min")
            max_val = rule.get("max")
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            failing = pd.Series([False] * len(numeric), index=numeric.index)
            if min_val is not None:
                failing |= numeric < min_val
            if max_val is not None:
                failing |= numeric > max_val
            fail_count = int(failing.sum())
            passed = fail_count == 0
            results.append({
                "check": "range",
                "column": col,
                "passed": passed,
                "severity": severity,
                "detail": f"{fail_count:,} values outside [{min_val}, {max_val}]",
            })

        elif rule_type == "pattern":
            pattern = rule.get("pattern", "")
            matches = series.astype(str).str.fullmatch(pattern)
            fail_count = int((~matches).sum())
            passed = fail_count == 0
            results.append({
                "check": "pattern",
                "column": col,
                "passed": passed,
                "severity": severity,
                "detail": f"{fail_count:,} values not matching {pattern}",
            })

    return results


def validate_contract(contract_path: Path, df: pd.DataFrame):
    """Run full contract validation and print results."""
    contract = load_contract(contract_path)
    name = contract.get("name", contract_path.stem)
    owner = contract.get("owner", "unknown")
    version = contract.get("version", "unknown")

    print(f"\n{'=' * 90}")
    print(f"  CONTRACT VALIDATION: {name}")
    print(f"  Owner: {owner}  |  Version: {version}")
    print(f"  Dataset rows: {len(df):,}")
    print(f"{'=' * 90}")

    # Schema validation
    print(f"\n  --- Schema Checks ---")
    schema_results = validate_schema(df, contract)
    for r in schema_results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['check']:20s} | {r['column']:20s} | {r['detail']}")

    # Quality rule validation
    print(f"\n  --- Quality Rule Checks ---")
    rule_results = validate_quality_rules(df, contract)
    for r in rule_results:
        status = "PASS" if r["passed"] else "FAIL"
        severity = r.get("severity", "")
        print(f"  [{status}] {r['check']:20s} | {r['column']:20s} | "
              f"[{severity:8s}] {r['detail']}")

    # Summary
    all_results = schema_results + rule_results
    passed = sum(1 for r in all_results if r["passed"])
    failed = sum(1 for r in all_results if not r["passed"])
    total = len(all_results)
    compliance_pct = (passed / total * 100) if total > 0 else 0

    print(f"\n  --- Summary ---")
    print(f"  Total checks: {total}")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Compliance: {compliance_pct:.1f}%")

    # Freshness check (if defined)
    freshness_conf = contract.get("freshness")
    if freshness_conf:
        ts_col = freshness_conf.get("timestamp_column")
        max_hours = freshness_conf.get("max_delay_hours", 24)
        if ts_col and ts_col in df.columns:
            parsed = pd.to_datetime(df[ts_col], errors="coerce", format="mixed")
            valid = parsed.dropna()
            if not valid.empty:
                from datetime import datetime
                gap = (datetime.now() - valid.max().to_pydatetime()).total_seconds() / 3600
                status = "PASS" if gap <= max_hours else "FAIL"
                print(f"\n  Freshness: latest={valid.max()}, gap={gap:.1f}h, "
                      f"SLO={max_hours}h [{status}]")

    # Volume check (if defined)
    volume_conf = contract.get("volume")
    if volume_conf:
        min_rows = volume_conf.get("min_rows") or volume_conf.get("min_daily_rows", 0)
        max_rows = volume_conf.get("max_rows") or volume_conf.get("max_daily_rows", float("inf"))
        row_count = len(df)
        in_range = min_rows <= row_count <= max_rows
        status = "PASS" if in_range else "FAIL"
        print(f"  Volume: {row_count:,} rows, expected [{min_rows:,}, {max_rows:,}] [{status}]")

    print(f"{'=' * 90}")

    return {
        "name": name,
        "passed": passed,
        "failed": failed,
        "compliance_pct": compliance_pct,
    }


def main():
    print(f"{'#' * 90}")
    print(f"#{'DATA CONTRACT VALIDATION':^88s}#")
    print(f"{'#' * 90}")

    summaries = []

    # Validate listening_events contract
    contract_path = CONTRACTS_DIR / "listening_events_contract.yml"
    if contract_path.exists():
        events_dir = RAW_DIR / "listening_events"
        partition_files = sorted(events_dir.glob("events_*.jsonl"))
        sample_files = partition_files[-10:] if len(partition_files) > 10 else partition_files
        frames = []
        for f in sample_files:
            lines = f.read_text().strip().split("\n")
            records = [json.loads(line) for line in lines if line.strip()]
            if records:
                frames.append(pd.DataFrame(records))
        events_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        result = validate_contract(contract_path, events_df)
        summaries.append(result)

    # Validate users contract
    contract_path = CONTRACTS_DIR / "users_contract.yml"
    if contract_path.exists():
        users_df = pd.read_csv(RAW_DIR / "users.csv")
        result = validate_contract(contract_path, users_df)
        summaries.append(result)

    # Overall summary
    print(f"\n{'=' * 90}")
    print("  OVERALL CONTRACT COMPLIANCE")
    print(f"{'=' * 90}")
    for s in summaries:
        print(f"  {s['name']:<25s}: {s['compliance_pct']:.1f}% "
              f"({s['passed']} passed, {s['failed']} failed)")
    print(f"{'=' * 90}")


if __name__ == "__main__":
    main()
