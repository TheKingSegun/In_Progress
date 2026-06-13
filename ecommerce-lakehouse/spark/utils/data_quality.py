"""
Lightweight data quality checks for PySpark DataFrames.

These are intentionally simple and fast — they run inside the Spark job before
writing each layer, not as a separate framework call.  For full suite runs
(Great Expectations) see the GE config in great_expectations/.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    check_name: str
    passed: bool
    details: str


class DataQualityChecker:
    def __init__(self, df: DataFrame, dataset_name: str):
        self.df = df
        self.dataset_name = dataset_name
        self._results: List[QualityResult] = []

    # ------------------------------------------------------------------
    # Check methods — each returns self so they can be chained.
    # ------------------------------------------------------------------

    def check_not_empty(self) -> "DataQualityChecker":
        count = self.df.count()
        passed = count > 0
        self._results.append(
            QualityResult("not_empty", passed, f"row_count={count}")
        )
        return self

    def check_no_nulls(self, columns: List[str]) -> "DataQualityChecker":
        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            passed = null_count == 0
            self._results.append(
                QualityResult(
                    f"no_nulls:{col}",
                    passed,
                    f"null_count={null_count}",
                )
            )
        return self

    def check_null_rate(
        self, column: str, max_rate: float
    ) -> "DataQualityChecker":
        """Allow nulls up to a threshold — useful for optional fields."""
        total = self.df.count()
        null_count = self.df.filter(F.col(column).isNull()).count()
        rate = null_count / total if total > 0 else 0.0
        passed = rate <= max_rate
        self._results.append(
            QualityResult(
                f"null_rate:{column}",
                passed,
                f"null_rate={rate:.4f} threshold={max_rate}",
            )
        )
        return self

    def check_unique(self, columns: List[str]) -> "DataQualityChecker":
        total = self.df.count()
        distinct = self.df.select(columns).distinct().count()
        passed = total == distinct
        self._results.append(
            QualityResult(
                f"unique:{','.join(columns)}",
                passed,
                f"total={total} distinct={distinct} dupes={total - distinct}",
            )
        )
        return self

    def check_values_in_set(
        self, column: str, allowed: List[str]
    ) -> "DataQualityChecker":
        invalid = (
            self.df.filter(~F.col(column).isin(allowed))
            .select(column)
            .distinct()
            .collect()
        )
        passed = len(invalid) == 0
        self._results.append(
            QualityResult(
                f"accepted_values:{column}",
                passed,
                f"invalid_values={[r[column] for r in invalid]}",
            )
        )
        return self

    def check_volume_vs_previous(
        self, expected_count: int, tolerance_pct: float = 0.30
    ) -> "DataQualityChecker":
        """Flag if today's volume deviates > tolerance from recent average."""
        actual = self.df.count()
        deviation = abs(actual - expected_count) / max(expected_count, 1)
        passed = deviation <= tolerance_pct
        self._results.append(
            QualityResult(
                "volume_anomaly",
                passed,
                f"actual={actual} expected={expected_count} deviation={deviation:.2%}",
            )
        )
        return self

    # ------------------------------------------------------------------
    # Finalise — raise on failure or just log warnings.
    # ------------------------------------------------------------------

    def validate(self, raise_on_failure: bool = True) -> List[QualityResult]:
        failures = [r for r in self._results if not r.passed]
        for result in self._results:
            status = "PASS" if result.passed else "FAIL"
            log_fn = logger.info if result.passed else logger.error
            log_fn(
                "[DQ] %s | %s | %s | %s",
                self.dataset_name,
                status,
                result.check_name,
                result.details,
            )

        if failures and raise_on_failure:
            failed_checks = ", ".join(r.check_name for r in failures)
            raise ValueError(
                f"Data quality checks failed for '{self.dataset_name}': {failed_checks}"
            )

        return self._results
