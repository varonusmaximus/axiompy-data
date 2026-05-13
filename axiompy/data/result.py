"""
Custom domain-specific Result types for data operations.

Provides type-safe Result subtypes for common data processing patterns,
offering better IDE support, type checking, and clearer APIs than generic
Result[Dict, str].
"""

from dataclasses import dataclass
from typing import List


@dataclass
class DataQualityResult:
    """Result object for data quality profiling operations.

    Attributes:
        row_count: Total number of rows in dataset
        column_count: Total number of columns in dataset
        null_counts: Dictionary mapping column names to null value counts
        duplicate_count: Total number of duplicate rows
        schema: Dictionary mapping column names to data types
        statistics: Dictionary with statistical summaries per column
        issues: List of quality issues found (high nulls, duplicates, etc.)
        metadata: Additional metadata about the profiling operation
    """

    row_count: int
    column_count: int
    null_counts: dict
    duplicate_count: int
    schema: dict
    statistics: dict
    issues: List[dict]
    metadata: dict

    @property
    def has_issues(self) -> bool:
        """Check if there are any quality issues."""
        return len(self.issues) > 0

    @property
    def null_percentage(self) -> dict:
        """Calculate null percentage for each column."""
        if self.row_count == 0:
            return {}
        return {col: (count / self.row_count) * 100 for col, count in self.null_counts.items()}

    @property
    def issue_count(self) -> int:
        """Get total number of issues found."""
        return len(self.issues)


@dataclass
class ValidationResult:
    """Result object for data expectation validation.

    Attributes:
        passed: Number of expectations that passed
        failed: Number of expectations that failed
        total: Total number of expectations checked
        details: List of detailed results for each expectation
        timestamp: When validation was performed
    """

    passed: int
    failed: int
    total: int
    details: List[dict]
    timestamp: str

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.passed / self.total) * 100

    @property
    def all_passed(self) -> bool:
        """Check if all expectations passed."""
        return self.failed == 0

    def get_failed_expectations(self) -> List[dict]:
        """Get list of failed expectations.

        Returns:
            List of expectation details that failed validation
        """
        return [d for d in self.details if not d.get("passed", True)]


@dataclass
class SchemaCheckResult:
    """Result object for schema validation.

    Attributes:
        matches: Whether actual schema matches expected schema
        expected_columns: List of expected column names
        actual_columns: List of actual column names in data
        missing: Columns expected but not found
        extra: Columns found but not expected
        type_mismatches: Dictionary of columns with type mismatches
    """

    matches: bool
    expected_columns: List[str]
    actual_columns: List[str]
    missing: List[str]
    extra: List[str]
    type_mismatches: dict

    @property
    def has_issues(self) -> bool:
        """Check if there are schema issues."""
        return len(self.missing) > 0 or len(self.extra) > 0 or len(self.type_mismatches) > 0

    @property
    def issue_summary(self) -> str:
        """Get human-readable summary of schema issues.

        Returns:
            Formatted string describing all schema issues found
        """
        issues = []
        if self.missing:
            issues.append(f"Missing columns: {', '.join(self.missing)}")
        if self.extra:
            issues.append(f"Extra columns: {', '.join(self.extra)}")
        if self.type_mismatches:
            mismatches = [f"{k}: {v[0]} vs {v[1]}" for k, v in self.type_mismatches.items()]
            issues.append(f"Type mismatches: {', '.join(mismatches)}")
        return " | ".join(issues) if issues else "Schema matches perfectly"


__all__ = [
    "DataQualityResult",
    "ValidationResult",
    "SchemaCheckResult",
]
