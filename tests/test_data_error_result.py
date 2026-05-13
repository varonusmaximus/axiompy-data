"""Tests for axiompy.data.error and axiompy.data.result modules."""

import pytest

from axiompy.data.error import (
    BatchProcessingError,
    CompressionError,
    DataFrameOperationError,
    DataProcessingError,
    LineageTrackingError,
    PartitionError,
    PipelineExecutionError,
    QualityCheckError,
    TransformationError,
    ValidationError,
    ensure_data_valid,
)
from axiompy.data.result import (
    DataQualityResult,
    SchemaCheckResult,
    ValidationResult,
)


# ==================== Error Classes Tests ====================


class TestDataProcessingError:
    """Tests for base DataProcessingError"""

    def test_basic_error(self):
        """Test creating a basic error"""
        error = DataProcessingError("Something went wrong")
        assert "Something went wrong" in str(error)
        assert error.error_code == "DATA_ERROR"

    def test_error_with_code(self):
        """Test error with custom code"""
        error = DataProcessingError("Failed", error_code="CUSTOM_001")
        assert "[CUSTOM_001]" in str(error)

    def test_error_with_context(self):
        """Test error with context dict"""
        error = DataProcessingError("Failed", context={"batch": 5, "row": 100})
        assert "batch" in str(error)
        assert "100" in str(error)

    def test_error_with_recovery_hint(self):
        """Test error with recovery hint"""
        error = DataProcessingError("Failed", recovery_hint="Try again later")
        assert "Recovery: Try again later" in str(error)


class TestValidationError:
    """Tests for ValidationError"""

    def test_basic_validation_error(self):
        """Test creating a basic validation error"""
        error = ValidationError("Invalid data")
        assert "VALIDATION_ERROR" in str(error)

    def test_validation_error_with_field(self):
        """Test validation error with field info"""
        error = ValidationError("Invalid", field_name="email", expected="str", actual=123)
        assert "email" in str(error)
        assert "str" in str(error)
        assert "123" in str(error)

    def test_validation_error_auto_recovery_hint(self):
        """Test that recovery hint is auto-generated"""
        error = ValidationError("Invalid", field_name="age", expected="int")
        assert "age" in str(error)
        assert "int" in str(error)


class TestBatchProcessingError:
    """Tests for BatchProcessingError"""

    def test_basic_batch_error(self):
        """Test creating a basic batch error"""
        error = BatchProcessingError("Batch failed")
        assert "BATCH_ERROR" in str(error)

    def test_batch_error_with_details(self):
        """Test batch error with details"""
        error = BatchProcessingError(
            "Processing failed", batch_number=5, batch_size=100, failed_records=3
        )
        assert "batch_number" in str(error)
        assert "5" in str(error)
        assert "100" in str(error)


class TestTransformationError:
    """Tests for TransformationError"""

    def test_basic_transform_error(self):
        """Test creating a basic transformation error"""
        error = TransformationError("Transform failed")
        assert "TRANSFORM_ERROR" in str(error)

    def test_transform_error_with_operation(self):
        """Test transformation error with operation"""
        error = TransformationError("Failed", operation="map_columns", input_data={"a": 1})
        assert "map_columns" in str(error)

    def test_transform_error_truncates_large_input(self):
        """Test that large input data is truncated"""
        large_data = "x" * 200
        error = TransformationError("Failed", input_data=large_data)
        assert "..." in str(error)


class TestCompressionError:
    """Tests for CompressionError"""

    def test_basic_compression_error(self):
        """Test creating a basic compression error"""
        error = CompressionError("Compression failed")
        assert "COMPRESSION_ERROR" in str(error)

    def test_compression_error_with_format(self):
        """Test compression error with format"""
        error = CompressionError("Invalid file", format="gzip", file_size=1024)
        assert "gzip" in str(error)
        assert "1024" in str(error)


class TestPartitionError:
    """Tests for PartitionError"""

    def test_basic_partition_error(self):
        """Test creating a basic partition error"""
        error = PartitionError("Partition failed")
        assert "PARTITION_ERROR" in str(error)

    def test_partition_error_with_details(self):
        """Test partition error with details"""
        error = PartitionError("Failed", partition_key="date", strategy="time")
        assert "date" in str(error)
        assert "time" in str(error)


class TestDataFrameOperationError:
    """Tests for DataFrameOperationError"""

    def test_basic_dataframe_error(self):
        """Test creating a basic dataframe error"""
        error = DataFrameOperationError("Operation failed")
        assert "DATAFRAME_ERROR" in str(error)

    def test_dataframe_error_with_shape(self):
        """Test dataframe error with shape"""
        error = DataFrameOperationError("Failed", operation="join", dataframe_shape=(100, 5))
        assert "join" in str(error)
        assert "100 rows" in str(error)


class TestQualityCheckError:
    """Tests for QualityCheckError"""

    def test_basic_quality_error(self):
        """Test creating a basic quality error"""
        error = QualityCheckError("Quality check failed")
        assert "QUALITY_ERROR" in str(error)

    def test_quality_error_with_threshold(self):
        """Test quality error with threshold"""
        error = QualityCheckError("Failed", check_name="null_check", threshold=0.1, actual=0.5)
        assert "null_check" in str(error)
        assert "0.1" in str(error)
        assert "0.5" in str(error)


class TestLineageTrackingError:
    """Tests for LineageTrackingError"""

    def test_basic_lineage_error(self):
        """Test creating a basic lineage error"""
        error = LineageTrackingError("Tracking failed")
        assert "LINEAGE_ERROR" in str(error)

    def test_lineage_error_with_details(self):
        """Test lineage error with details"""
        error = LineageTrackingError("Failed", job_name="etl_job", storage_type="database")
        assert "etl_job" in str(error)
        assert "database" in str(error)


class TestPipelineExecutionError:
    """Tests for PipelineExecutionError"""

    def test_basic_pipeline_error(self):
        """Test creating a basic pipeline error"""
        error = PipelineExecutionError("Pipeline failed")
        assert "PIPELINE_ERROR" in str(error)

    def test_pipeline_error_with_stage(self):
        """Test pipeline error with stage"""
        error = PipelineExecutionError("Failed", pipeline_name="data_pipeline", stage="transform")
        assert "data_pipeline" in str(error)
        assert "transform" in str(error)


class TestEnsureDataValid:
    """Tests for ensure_data_valid function"""

    def test_passes_when_true(self):
        """Test that no error is raised when condition is True"""
        ensure_data_valid(True, "Should not fail")

    def test_raises_when_false(self):
        """Test that error is raised when condition is False"""
        with pytest.raises(DataProcessingError, match="Validation failed"):
            ensure_data_valid(False, "Validation failed")

    def test_custom_error_code(self):
        """Test custom error code"""
        with pytest.raises(DataProcessingError) as exc_info:
            ensure_data_valid(False, "Failed", error_code="CUSTOM_CODE")
        assert "CUSTOM_CODE" in str(exc_info.value)


# ==================== Result Classes Tests ====================


class TestDataQualityResult:
    """Tests for DataQualityResult"""

    def test_basic_result(self):
        """Test creating a basic result"""
        result = DataQualityResult(
            row_count=100,
            column_count=5,
            null_counts={"a": 10, "b": 0},
            duplicate_count=2,
            schema={"a": "int", "b": "str"},
            statistics={},
            issues=[],
            metadata={},
        )
        assert result.row_count == 100
        assert not result.has_issues

    def test_has_issues(self):
        """Test has_issues property"""
        result = DataQualityResult(
            row_count=100,
            column_count=5,
            null_counts={},
            duplicate_count=0,
            schema={},
            statistics={},
            issues=[{"type": "high_nulls", "column": "a"}],
            metadata={},
        )
        assert result.has_issues
        assert result.issue_count == 1

    def test_null_percentage(self):
        """Test null percentage calculation"""
        result = DataQualityResult(
            row_count=100,
            column_count=2,
            null_counts={"a": 10, "b": 50},
            duplicate_count=0,
            schema={},
            statistics={},
            issues=[],
            metadata={},
        )
        assert result.null_percentage["a"] == 10.0
        assert result.null_percentage["b"] == 50.0

    def test_null_percentage_empty(self):
        """Test null percentage with zero rows"""
        result = DataQualityResult(
            row_count=0,
            column_count=0,
            null_counts={"a": 0},
            duplicate_count=0,
            schema={},
            statistics={},
            issues=[],
            metadata={},
        )
        assert result.null_percentage == {}


class TestValidationResultData:
    """Tests for ValidationResult from data.result"""

    def test_basic_result(self):
        """Test creating a basic result"""
        result = ValidationResult(
            passed=8,
            failed=2,
            total=10,
            details=[],
            timestamp="2024-01-01T00:00:00",
        )
        assert result.success_rate == 80.0
        assert not result.all_passed

    def test_all_passed(self):
        """Test all_passed property"""
        result = ValidationResult(passed=10, failed=0, total=10, details=[], timestamp="2024-01-01")
        assert result.all_passed
        assert result.success_rate == 100.0

    def test_success_rate_zero_total(self):
        """Test success rate with zero total"""
        result = ValidationResult(passed=0, failed=0, total=0, details=[], timestamp="2024-01-01")
        assert result.success_rate == 0.0

    def test_get_failed_expectations(self):
        """Test getting failed expectations"""
        result = ValidationResult(
            passed=1,
            failed=1,
            total=2,
            details=[{"name": "check1", "passed": True}, {"name": "check2", "passed": False}],
            timestamp="2024-01-01",
        )
        failed = result.get_failed_expectations()
        assert len(failed) == 1
        assert failed[0]["name"] == "check2"


class TestSchemaCheckResult:
    """Tests for SchemaCheckResult"""

    def test_matching_schema(self):
        """Test schema that matches"""
        result = SchemaCheckResult(
            matches=True,
            expected_columns=["a", "b"],
            actual_columns=["a", "b"],
            missing=[],
            extra=[],
            type_mismatches={},
        )
        assert result.matches
        assert not result.has_issues
        assert "matches perfectly" in result.issue_summary

    def test_missing_columns(self):
        """Test schema with missing columns"""
        result = SchemaCheckResult(
            matches=False,
            expected_columns=["a", "b", "c"],
            actual_columns=["a", "b"],
            missing=["c"],
            extra=[],
            type_mismatches={},
        )
        assert result.has_issues
        assert "Missing columns: c" in result.issue_summary

    def test_extra_columns(self):
        """Test schema with extra columns"""
        result = SchemaCheckResult(
            matches=False,
            expected_columns=["a"],
            actual_columns=["a", "b"],
            missing=[],
            extra=["b"],
            type_mismatches={},
        )
        assert result.has_issues
        assert "Extra columns: b" in result.issue_summary

    def test_type_mismatches(self):
        """Test schema with type mismatches"""
        result = SchemaCheckResult(
            matches=False,
            expected_columns=["a"],
            actual_columns=["a"],
            missing=[],
            extra=[],
            type_mismatches={"a": ("int", "str")},
        )
        assert result.has_issues
        assert "Type mismatches" in result.issue_summary
        assert "int vs str" in result.issue_summary
