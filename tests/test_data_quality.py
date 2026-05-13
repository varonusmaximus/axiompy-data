"""
Unit tests for axiompy.data.quality module.

Tests DataProfiler implementations for both Pandas and Spark engines.
"""

import pandas as pd
import pytest

from axiompy.data.quality import (
    DataProfiler,
    DataProfilerFactory,
    PandasDataProfiler,
)
from axiompy.data.types import DataEngine, DataExpectation


class TestPandasDataProfiler:
    """Test PandasDataProfiler implementation."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, None],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
                "age": [25, 30, None, 45, 28, 35],
                "score": [85.5, 92.0, 78.5, None, 95.0, 88.0],
                "status": ["active", "active", "inactive", "active", "pending", "active"],
            }
        )

    @pytest.fixture
    def profiler(self):
        """Create PandasDataProfiler instance."""
        return PandasDataProfiler()

    def test_profile_basic(self, profiler, sample_df):
        """Test basic profiling functionality."""
        report = profiler.profile(sample_df)

        assert report.row_count == 6
        assert report.column_count == 5
        assert isinstance(report.null_counts, dict)
        assert isinstance(report.schema, dict)
        assert isinstance(report.statistics, dict)
        assert report.metadata["engine"] == "pandas"

    def test_profile_null_counts(self, profiler, sample_df):
        """Test null count detection."""
        report = profiler.profile(sample_df)

        assert report.null_counts["id"] == 1
        assert report.null_counts["age"] == 1
        assert report.null_counts["score"] == 1
        assert report.null_counts["name"] == 0
        assert report.null_counts["status"] == 0

    def test_profile_duplicates(self, profiler):
        """Test duplicate detection."""
        df = pd.DataFrame({"id": [1, 2, 3, 2], "value": [10, 20, 30, 20]})

        report = profiler.profile(df)
        assert report.duplicate_count == 1

    def test_profile_statistics(self, profiler, sample_df):
        """Test statistical calculations."""
        report = profiler.profile(sample_df)

        # Check numeric column statistics
        age_stats = report.statistics["age"]
        assert "min" in age_stats
        assert "max" in age_stats
        assert "mean" in age_stats
        assert "unique_count" in age_stats
        assert "null_percentage" in age_stats

        # Verify some values
        assert age_stats["min"] == 25.0
        assert age_stats["max"] == 45.0
        assert age_stats["null_percentage"] > 0  # Has nulls

    def test_profile_issues_detection(self, profiler):
        """Test quality issue detection."""
        df = pd.DataFrame(
            {
                "col1": [1, None, None, None, 5],  # 60% nulls
                "col2": [10, 20, 30, 40, 50],
            }
        )

        report = profiler.profile(df)

        # Should detect high null percentage
        issues = [i for i in report.issues if i["column"] == "col1"]
        assert len(issues) > 0
        assert issues[0]["severity"] == "high"

    def test_validate_expectations_not_null(self, profiler, sample_df):
        """Test not_null expectation."""
        expectations = [DataExpectation(name="name_not_null", column="name", condition="not_null")]

        results = profiler.validate_expectations(sample_df, expectations)

        assert results["passed"] == 1
        assert results["failed"] == 0
        assert results["success"] is True

    def test_validate_expectations_unique(self, profiler):
        """Test unique expectation."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "email": ["a@ex.com", "b@ex.com", "a@ex.com", "d@ex.com", "e@ex.com"],
            }
        )

        expectations = [
            DataExpectation(name="id_unique", column="id", condition="unique"),
            DataExpectation(name="email_unique", column="email", condition="unique"),
        ]

        results = profiler.validate_expectations(df, expectations)

        assert results["passed"] == 1  # id is unique
        assert results["failed"] == 1  # email has duplicates
        assert results["success"] is False

    def test_validate_expectations_in_range(self, profiler, sample_df):
        """Test in_range expectation."""
        expectations = [
            DataExpectation(
                name="age_range", column="age", condition="in_range", params={"min": 0, "max": 100}
            )
        ]

        results = profiler.validate_expectations(sample_df, expectations)

        # All non-null ages should be in range
        assert results["passed"] == 1
        assert results["success"] is True

    def test_validate_expectations_in_set(self, profiler, sample_df):
        """Test in_set expectation."""
        expectations = [
            DataExpectation(
                name="status_valid",
                column="status",
                condition="in_set",
                params={"values": ["active", "inactive", "pending"]},
            )
        ]

        results = profiler.validate_expectations(sample_df, expectations)

        assert results["passed"] == 1
        assert results["success"] is True

    def test_validate_expectations_regex_match(self, profiler):
        """Test regex_match expectation."""
        df = pd.DataFrame({"code": ["ABC123", "DEF456", "GHI789", "invalid"]})

        expectations = [
            DataExpectation(
                name="code_format",
                column="code",
                condition="regex_match",
                params={"pattern": r"^[A-Z]{3}\d{3}$"},
            )
        ]

        results = profiler.validate_expectations(df, expectations)

        # Should fail because 'invalid' doesn't match
        assert results["failed"] == 1

    def test_check_schema_valid(self, profiler, sample_df):
        """Test schema validation with valid schema."""
        # Use actual dtypes for compatibility across pandas versions
        expected_schema = {col: str(dtype) for col, dtype in sample_df.dtypes.items()}

        result = profiler.check_schema(sample_df, expected_schema)

        assert result["valid"] is True
        assert len(result["issues"]) == 0

    def test_check_schema_missing_columns(self, profiler, sample_df):
        """Test schema validation with missing columns."""
        expected_schema = {"id": "int", "name": "string", "missing_column": "string"}

        result = profiler.check_schema(sample_df, expected_schema)

        assert result["valid"] is False
        assert any("Missing columns" in issue for issue in result["issues"])

    def test_check_schema_extra_columns(self, profiler, sample_df):
        """Test schema validation with extra columns."""
        expected_schema = {"id": "int", "name": "string"}

        result = profiler.check_schema(sample_df, expected_schema)

        assert result["valid"] is False
        assert any("Extra columns" in issue for issue in result["issues"])


class TestDataProfilerFactory:
    """Test DataProfilerFactory."""

    def test_create_pandas_profiler(self):
        """Test creating Pandas profiler explicitly."""
        profiler = DataProfilerFactory.create(DataEngine.PANDAS)

        assert isinstance(profiler, PandasDataProfiler)
        assert profiler.engine == DataEngine.PANDAS

    def test_create_auto_pandas(self):
        """Test auto-detection of Pandas engine."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        profiler = DataProfilerFactory.create_auto(df)

        assert isinstance(profiler, PandasDataProfiler)

    def test_create_unsupported_engine(self):
        """Test error on unsupported engine."""
        with pytest.raises(ValueError, match="Unsupported engine"):
            DataProfilerFactory.create(DataEngine.POLARS)

    def test_create_auto_unknown_type(self):
        """Test error on unknown data type."""
        with pytest.raises(ValueError, match="Cannot auto-detect"):
            DataProfilerFactory.create_auto("not a dataframe")

    def test_register_custom_profiler(self):
        """Test registering custom profiler."""

        class CustomProfiler(DataProfiler):
            def __init__(self, settings=None):
                super().__init__(DataEngine.POLARS, settings)

            def profile(self, data):
                pass

            def validate_expectations(self, data, expectations):
                pass

            def check_schema(self, data, expected_schema):
                pass

        DataProfilerFactory.register_profiler(DataEngine.POLARS, CustomProfiler)

        profiler = DataProfilerFactory.create(DataEngine.POLARS)
        assert isinstance(profiler, CustomProfiler)


class TestDataQualityIntegration:
    """Integration tests for data quality workflows."""

    def test_profile_and_validate_workflow(self):
        """Test complete profile and validate workflow."""
        # Create data with quality issues
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, None, 5],
                "email": ["a@ex.com", "b@ex.com", "invalid", "d@ex.com", "e@ex.com"],
                "age": [25, 30, 150, 45, -5],  # Out of range values
            }
        )

        # Profile
        profiler = DataProfilerFactory.create_auto(df)
        report = profiler.profile(df)

        assert report.row_count == 5
        assert report.null_counts["user_id"] == 1

        # Validate expectations
        expectations = [
            DataExpectation(name="id_not_null", column="user_id", condition="not_null"),
            DataExpectation(
                name="age_range", column="age", condition="in_range", params={"min": 0, "max": 120}
            ),
        ]

        results = profiler.validate_expectations(df, expectations)

        # Both should fail
        assert results["failed"] == 2
        assert results["success"] is False

    def test_profile_clean_data(self):
        """Test profiling perfectly clean data."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],
                "category": ["A", "B", "A", "C", "B"],
            }
        )

        profiler = DataProfilerFactory.create_auto(df)
        report = profiler.profile(df)

        assert report.row_count == 5
        assert report.duplicate_count == 0
        assert sum(report.null_counts.values()) == 0
        assert len(report.issues) == 0  # No quality issues


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
