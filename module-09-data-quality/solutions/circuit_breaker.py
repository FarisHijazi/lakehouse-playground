"""
Exercise 7: Circuit Breaker
=============================
Implements a circuit breaker that halts pipeline execution when data quality
scores drop below configurable thresholds.

States:
  CLOSED   (score >= passing)  -> proceed normally
  HALF-OPEN (warning <= score < passing) -> proceed with warning
  OPEN     (score < warning)   -> halt pipeline

Critical override: if score < critical threshold, halt immediately and
escalate regardless of other settings.
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

# Import the scorer to get quality scores
import quality_scorer

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


class DataQualityError(Exception):
    """Raised when the circuit breaker trips."""
    pass


class CircuitBreaker:
    """Evaluates quality scores and decides whether a pipeline should proceed."""

    def __init__(
        self,
        passing_threshold: float = 90.0,
        warning_threshold: float = 80.0,
        critical_threshold: float = 50.0,
    ):
        self.passing_threshold = passing_threshold
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.decisions: list[dict] = []

    def evaluate(self, dataset_name: str, quality_score: float) -> str:
        """
        Evaluate a quality score and return the circuit state.

        Returns:
          'CLOSED'    - proceed normally
          'HALF-OPEN' - proceed with warning
          'OPEN'      - halt pipeline
        """
        timestamp = datetime.now().isoformat()

        if quality_score < self.critical_threshold:
            state = "OPEN"
            action = "HALT pipeline immediately. Page on-call engineer."
            severity = "CRITICAL"
        elif quality_score < self.warning_threshold:
            state = "OPEN"
            action = "HALT pipeline. Alert data team via Slack."
            severity = "ERROR"
        elif quality_score < self.passing_threshold:
            state = "HALF-OPEN"
            action = "Proceed with caution. Log warning. Monitor closely."
            severity = "WARNING"
        else:
            state = "CLOSED"
            action = "Proceed normally. Log metrics for trending."
            severity = "INFO"

        decision = {
            "timestamp": timestamp,
            "dataset": dataset_name,
            "score": quality_score,
            "state": state,
            "severity": severity,
            "action": action,
        }
        self.decisions.append(decision)
        return state

    def should_halt(self, dataset_name: str, quality_score: float) -> bool:
        """Convenience method: returns True if pipeline should stop."""
        state = self.evaluate(dataset_name, quality_score)
        return state == "OPEN"

    def summary(self):
        """Print all decisions."""
        print(f"\n{'=' * 100}")
        print("  CIRCUIT BREAKER DECISIONS")
        print(f"  Thresholds: passing={self.passing_threshold}, "
              f"warning={self.warning_threshold}, critical={self.critical_threshold}")
        print(f"{'=' * 100}")

        for d in self.decisions:
            state_indicator = {
                "CLOSED": "  [CLOSED]   ",
                "HALF-OPEN": "  [HALF-OPEN]",
                "OPEN": "  [OPEN]     ",
            }[d["state"]]

            print(f"\n{state_indicator} {d['dataset']}")
            print(f"    Score:    {d['score']:.1f}")
            print(f"    Severity: {d['severity']}")
            print(f"    Action:   {d['action']}")

        # Overall pipeline decision
        halted = [d for d in self.decisions if d["state"] == "OPEN"]
        warned = [d for d in self.decisions if d["state"] == "HALF-OPEN"]
        passed = [d for d in self.decisions if d["state"] == "CLOSED"]

        print(f"\n{'-' * 100}")
        print(f"  PIPELINE DECISION SUMMARY")
        print(f"    Passed (CLOSED):      {len(passed)}")
        print(f"    Warned (HALF-OPEN):   {len(warned)}")
        print(f"    Halted (OPEN):        {len(halted)}")

        if halted:
            print(f"\n    *** PIPELINE HALTED ***")
            print(f"    Blocking datasets:")
            for d in halted:
                print(f"      - {d['dataset']} (score: {d['score']:.1f})")
        else:
            print(f"\n    Pipeline may proceed.")

        print(f"{'=' * 100}")


def simulate_pipeline():
    """Simulate a pipeline run: score all datasets, then run circuit breaker."""
    print(f"{'=' * 100}")
    print("  PIPELINE SIMULATION WITH CIRCUIT BREAKER")
    print(f"  Simulating: Ingest -> Quality Score -> Circuit Breaker -> Load")
    print(f"{'=' * 100}")

    # Get quality scores from the scorer module
    print("\n  Step 1: Computing quality scores...")
    scores = quality_scorer.main()

    # Create circuit breaker
    cb = CircuitBreaker(
        passing_threshold=90.0,
        warning_threshold=80.0,
        critical_threshold=50.0,
    )

    print("\n  Step 2: Running circuit breaker evaluation...")
    for score_result in scores:
        dataset = score_result["dataset"]
        overall = score_result["overall"]
        cb.evaluate(dataset, overall)

    # Print decisions
    cb.summary()

    # Show what would happen in a real pipeline
    print(f"\n  --- Simulated Pipeline Steps ---")
    for d in cb.decisions:
        dataset = d["dataset"]
        if d["state"] == "CLOSED":
            print(f"  [OK]   {dataset}: Loading to warehouse...")
            print(f"         {dataset}: Load complete.")
        elif d["state"] == "HALF-OPEN":
            print(f"  [WARN] {dataset}: Quality below target. Loading with warning flag...")
            print(f"         {dataset}: Load complete (flagged for review).")
        else:
            print(f"  [HALT] {dataset}: Quality too low. SKIPPING load.")
            print(f"         {dataset}: Alert sent. Data quarantined.")


def main():
    simulate_pipeline()


if __name__ == "__main__":
    main()
