"""Pandas and Spark data profilers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from axiompy.validators import ensure_not_empty, ensure_not_none, ensure_positive

from axiompy.data.observability.ports import SignalKind, SignalSink
from axiompy.data.processing.quality import DataProfiler, logger
from axiompy.data.processing.signals import emit_signal
from axiompy.data.types import DataEngine, DataExpectation, DataQualityReport


class PandasDataProfiler(DataProfiler):
    """Data profiler for Pandas DataFrames."""

    def __init__(self, settings: Optional[Dict] = None, signal_sink: Optional[SignalSink] = None):
        super().__init__(DataEngine.PANDAS, settings, signal_sink)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def profile(self, data: pd.DataFrame) -> DataQualityReport:
        """Profile a Pandas DataFrame."""
        ensure_not_none(data, "DataFrame cannot be None")
        ensure_positive(len(data.columns), "DataFrame must have at least one column")

        logger.info(f"Profiling Pandas DataFrame with shape {data.shape}")

        row_count = len(data)
        column_count = len(data.columns)

        # Null counts
        null_counts = data.isnull().sum().to_dict()

        # Duplicates
        duplicate_count = data.duplicated().sum()

        # Schema
        schema = {col: str(dtype) for col, dtype in data.dtypes.items()}

        # Statistics per column
        statistics = {}
        for col in data.columns:
            col_stats = {}
            if self.pd.api.types.is_numeric_dtype(data[col]):
                col_stats.update(
                    {
                        "min": (
                            float(data[col].min()) if not self.pd.isna(data[col].min()) else None
                        ),
                        "max": (
                            float(data[col].max()) if not self.pd.isna(data[col].max()) else None
                        ),
                        "mean": (
                            float(data[col].mean()) if not self.pd.isna(data[col].mean()) else None
                        ),
                        "median": (
                            float(data[col].median())
                            if not self.pd.isna(data[col].median())
                            else None
                        ),
                        "std": (
                            float(data[col].std()) if not self.pd.isna(data[col].std()) else None
                        ),
                    }
                )
            col_stats["unique_count"] = int(data[col].nunique())
            col_stats["null_percentage"] = (
                float((data[col].isnull().sum() / row_count) * 100) if row_count > 0 else 0.0
            )
            statistics[col] = col_stats

        # Identify issues
        issues = []
        for col, stats in statistics.items():
            null_pct = stats["null_percentage"]
            if null_pct > 50:
                issues.append(
                    {
                        "severity": "high",
                        "column": col,
                        "issue": f"High null percentage: {null_pct:.2f}%",
                    }
                )
            if null_pct > 20:
                issues.append(
                    {
                        "severity": "medium",
                        "column": col,
                        "issue": f"Moderate null percentage: {null_pct:.2f}%",
                    }
                )

        logger.info(
            f"Profiling complete: {row_count} rows, {column_count} columns, {len(issues)} issues"
        )

        report = DataQualityReport(
            row_count=int(row_count),
            column_count=int(column_count),
            null_counts={k: int(v) for k, v in null_counts.items()},
            duplicate_count=int(duplicate_count),
            schema=schema,
            statistics=statistics,
            issues=issues,
            metadata={"engine": "pandas"},
        )
        emit_signal(
            self._signal_sink,
            SignalKind.QUALITY,
            "profiler.profile",
            {"engine": "pandas", "row_count": report.row_count, "issues": len(issues)},
        )
        return report

    def validate_expectations(
        self, data: pd.DataFrame, expectations: List[DataExpectation]
    ) -> Dict[str, Any]:
        """Validate Pandas DataFrame against expectations."""
        ensure_not_none(data, "DataFrame cannot be None")
        ensure_not_none(expectations, "expectations list cannot be None")
        ensure_positive(len(expectations), "Must provide at least one expectation")

        logger.info(f"Validating {len(expectations)} expectations on Pandas DataFrame")

        results = {"passed": 0, "failed": 0, "details": []}

        for expectation in expectations:
            result = self._check_expectation(data, expectation)
            results["details"].append(result)
            if result["passed"]:
                results["passed"] += 1
            else:
                results["failed"] += 1

        results["success"] = results["failed"] == 0
        logger.info(f"Validation complete: {results['passed']} passed, {results['failed']} failed")
        return results

    def _check_expectation(
        self, data: pd.DataFrame, expectation: DataExpectation
    ) -> Dict[str, Any]:
        """Check a single expectation."""
        col = expectation.column
        condition = expectation.condition
        params = expectation.params

        passed = False
        message = ""

        try:
            if condition == "not_null":
                null_count = data[col].isnull().sum()
                passed = null_count == 0
                message = f"Column '{col}' has {null_count} null values"

            elif condition == "unique":
                duplicate_count = data[col].duplicated().sum()
                passed = duplicate_count == 0
                message = f"Column '{col}' has {duplicate_count} duplicates"

            elif condition == "in_range":
                min_val = params.get("min")
                max_val = params.get("max")
                out_of_range = data[(data[col] < min_val) | (data[col] > max_val)]
                passed = len(out_of_range) == 0
                message = (
                    f"Column '{col}' has {len(out_of_range)} values "
                    f"out of range [{min_val}, {max_val}]"
                )

            elif condition == "in_set":
                valid_values = set(params.get("values", []))
                invalid = data[~data[col].isin(valid_values)]
                passed = len(invalid) == 0
                message = f"Column '{col}' has {len(invalid)} invalid values"

            elif condition == "regex_match":
                pattern = params.get("pattern")
                non_matching = data[~data[col].astype(str).str.match(pattern, na=False)]
                passed = len(non_matching) == 0
                message = f"Column '{col}' has {len(non_matching)} values not matching pattern"

            else:
                passed = False
                message = f"Unknown condition: {condition}"

        except Exception as e:
            passed = False
            message = f"Error checking expectation: {str(e)}"
            logger.error(f"Expectation check failed for {expectation.name}: {e}")

        return {
            "expectation": expectation.name,
            "column": col,
            "condition": condition,
            "passed": passed,
            "message": message,
        }

    def check_schema(self, data: pd.DataFrame, expected_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate Pandas DataFrame schema."""
        ensure_not_none(data, "DataFrame cannot be None")
        ensure_not_none(expected_schema, "expected_schema cannot be None")
        ensure_not_empty(expected_schema, "expected_schema cannot be empty")

        logger.info("Checking schema for Pandas DataFrame")

        issues = []
        actual_columns = set(data.columns)
        expected_columns = set(expected_schema.keys())

        # Missing columns
        missing = expected_columns - actual_columns
        if missing:
            issues.append(f"Missing columns: {sorted(missing)}")

        # Extra columns
        extra = actual_columns - expected_columns
        if extra:
            issues.append(f"Extra columns: {sorted(extra)}")

        # Type mismatches
        for col, expected_type in expected_schema.items():
            if col in data.columns:
                actual_type = str(data[col].dtype)
                if not self._types_compatible(actual_type, expected_type):
                    issues.append(f"Column '{col}': expected {expected_type}, got {actual_type}")

        result = {"valid": len(issues) == 0, "issues": issues}

        status = "valid" if result["valid"] else f"{len(issues)} issues found"
        logger.info(f"Schema check complete: {status}")
        return result

    def _types_compatible(self, actual: str, expected: Any) -> bool:
        """Check if types are compatible."""
        type_map = {
            "int": ["int64", "int32", "int16", "int8", "Int64", "Int32", "Int16", "Int8"],
            "float": ["float64", "float32", "Float64", "Float32"],
            "string": ["object", "string"],
            "bool": ["bool", "boolean"],
            "datetime": ["datetime64"],
        }

        expected_str = str(expected).lower()
        for key, values in type_map.items():
            if key in expected_str and any(v.lower() in actual.lower() for v in values):
                return True

        return actual == str(expected)


class SparkDataProfiler(DataProfiler):
    """Data profiler for PySpark DataFrames."""

    def __init__(self, settings: Optional[Dict] = None, signal_sink: Optional[SignalSink] = None):
        super().__init__(DataEngine.SPARK, settings, signal_sink)
        try:
            from pyspark.sql import DataFrame
            from pyspark.sql import functions as F

            self.DataFrame = DataFrame
            self.F = F
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def profile(self, data: DataFrame) -> DataQualityReport:
        """Profile a PySpark DataFrame."""
        logger.info("Profiling Spark DataFrame")

        # Row count
        row_count = data.count()
        column_count = len(data.columns)

        logger.info(f"DataFrame has {row_count} rows and {column_count} columns")

        # Null counts (computed in single pass for efficiency)
        null_exprs = [
            self.F.sum(self.F.when(self.F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in data.columns
        ]
        null_counts_row = data.select(null_exprs).collect()[0]
        null_counts = {col: null_counts_row[col] for col in data.columns}

        # Duplicates
        duplicate_count = row_count - data.dropDuplicates().count()

        # Schema
        schema = {field.name: str(field.dataType) for field in data.schema.fields}

        # Statistics (use Spark's describe for numeric columns)
        statistics = {}
        numeric_types = ["int", "long", "float", "double", "decimal", "short", "byte"]
        numeric_cols = [
            f.name
            for f in data.schema.fields
            if any(t in str(f.dataType).lower() for t in numeric_types)
        ]

        if numeric_cols:
            stats_df = data.select(numeric_cols).summary("min", "max", "mean", "stddev", "50%")
            stats_collected = stats_df.collect()

            # Extract stats by metric
            stats_dict = {row["summary"]: row for row in stats_collected}

            for col in numeric_cols:
                statistics[col] = {
                    "min": self._safe_float(
                        stats_dict.get("min")[col] if "min" in stats_dict else None
                    ),
                    "max": self._safe_float(
                        stats_dict.get("max")[col] if "max" in stats_dict else None
                    ),
                    "mean": self._safe_float(
                        stats_dict.get("mean")[col] if "mean" in stats_dict else None
                    ),
                    "median": self._safe_float(
                        stats_dict.get("50%")[col] if "50%" in stats_dict else None
                    ),
                    "std": self._safe_float(
                        stats_dict.get("stddev")[col] if "stddev" in stats_dict else None
                    ),
                }

        # Add unique counts and null percentages
        for col in data.columns:
            if col not in statistics:
                statistics[col] = {}
            statistics[col]["unique_count"] = data.select(col).distinct().count()
            statistics[col]["null_percentage"] = (
                float(null_counts[col] / row_count * 100) if row_count > 0 else 0.0
            )

        # Identify issues
        issues = []
        for col, stats in statistics.items():
            null_pct = stats["null_percentage"]
            if null_pct > 50:
                issues.append(
                    {
                        "severity": "high",
                        "column": col,
                        "issue": f"High null percentage: {null_pct:.2f}%",
                    }
                )
            elif null_pct > 20:
                issues.append(
                    {
                        "severity": "medium",
                        "column": col,
                        "issue": f"Moderate null percentage: {null_pct:.2f}%",
                    }
                )

        logger.info(
            f"Profiling complete: {row_count} rows, {column_count} columns, {len(issues)} issues"
        )

        report = DataQualityReport(
            row_count=int(row_count),
            column_count=int(column_count),
            null_counts={k: int(v) for k, v in null_counts.items()},
            duplicate_count=int(duplicate_count),
            schema=schema,
            statistics=statistics,
            issues=issues,
            metadata={"engine": "spark"},
        )
        emit_signal(
            self._signal_sink,
            SignalKind.QUALITY,
            "profiler.profile",
            {"engine": "spark", "row_count": report.row_count, "issues": len(issues)},
        )
        return report

    def _safe_float(self, value):
        """Safely convert to float."""
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    def validate_expectations(
        self, data: DataFrame, expectations: List[DataExpectation]
    ) -> Dict[str, Any]:
        """Validate Spark DataFrame against expectations."""
        logger.info(f"Validating {len(expectations)} expectations on Spark DataFrame")

        results = {"passed": 0, "failed": 0, "details": []}

        for expectation in expectations:
            result = self._check_expectation(data, expectation)
            results["details"].append(result)
            if result["passed"]:
                results["passed"] += 1
            else:
                results["failed"] += 1

        results["success"] = results["failed"] == 0
        logger.info(f"Validation complete: {results['passed']} passed, {results['failed']} failed")
        return results

    def _check_expectation(self, data: DataFrame, expectation: DataExpectation) -> Dict[str, Any]:
        """Check a single expectation."""
        col = expectation.column
        condition = expectation.condition
        params = expectation.params

        passed = False
        message = ""

        try:
            if condition == "not_null":
                null_count = data.filter(data[col].isNull()).count()
                passed = null_count == 0
                message = f"Column '{col}' has {null_count} null values"

            elif condition == "unique":
                total_count = data.count()
                distinct_count = data.select(col).distinct().count()
                duplicate_count = total_count - distinct_count
                passed = duplicate_count == 0
                message = f"Column '{col}' has {duplicate_count} duplicates"

            elif condition == "in_range":
                min_val = params.get("min")
                max_val = params.get("max")
                out_of_range_count = data.filter(
                    (data[col] < min_val) | (data[col] > max_val)
                ).count()
                passed = out_of_range_count == 0
                message = (
                    f"Column '{col}' has {out_of_range_count} values "
                    f"out of range [{min_val}, {max_val}]"
                )

            elif condition == "in_set":
                valid_values = params.get("values", [])
                invalid_count = data.filter(~data[col].isin(valid_values)).count()
                passed = invalid_count == 0
                message = f"Column '{col}' has {invalid_count} invalid values"

            elif condition == "regex_match":
                pattern = params.get("pattern")
                non_matching_count = data.filter(~data[col].rlike(pattern)).count()
                passed = non_matching_count == 0
                message = f"Column '{col}' has {non_matching_count} values not matching pattern"

            else:
                passed = False
                message = f"Unknown condition: {condition}"

        except Exception as e:
            passed = False
            message = f"Error checking expectation: {str(e)}"
            logger.error(f"Expectation check failed for {expectation.name}: {e}")

        return {
            "expectation": expectation.name,
            "column": col,
            "condition": condition,
            "passed": passed,
            "message": message,
        }

    def check_schema(self, data: DataFrame, expected_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate Spark DataFrame schema."""
        logger.info("Checking schema for Spark DataFrame")

        issues = []
        actual_columns = set(data.columns)
        expected_columns = set(expected_schema.keys())

        # Missing columns
        missing = expected_columns - actual_columns
        if missing:
            issues.append(f"Missing columns: {sorted(missing)}")

        # Extra columns
        extra = actual_columns - expected_columns
        if extra:
            issues.append(f"Extra columns: {sorted(extra)}")

        # Type mismatches
        actual_types = {field.name: str(field.dataType) for field in data.schema.fields}
        for col, expected_type in expected_schema.items():
            if col in actual_types:
                if not self._types_compatible(actual_types[col], expected_type):
                    issues.append(
                        f"Column '{col}': expected {expected_type}, got {actual_types[col]}"
                    )

        result = {"valid": len(issues) == 0, "issues": issues}

        status = "valid" if result["valid"] else f"{len(issues)} issues found"
        logger.info(f"Schema check complete: {status}")
        return result

    def _types_compatible(self, actual: str, expected: Any) -> bool:
        """Check if Spark types are compatible."""
        expected_str = str(expected).lower()
        actual_lower = actual.lower()

        type_map = {
            "int": ["integertype", "longtype", "shorttype", "bytetype"],
            "float": ["floattype", "doubletype", "decimaltype"],
            "string": ["stringtype"],
            "bool": ["booleantype"],
            "datetime": ["timestamptype", "datetype"],
        }

        for key, values in type_map.items():
            if key in expected_str and any(v in actual_lower for v in values):
                return True

        return actual == str(expected)
