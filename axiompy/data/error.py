"""
Custom error hierarchy for data module operations.

Provides domain-specific exception classes for data processing,
transformation, and validation errors with detailed context and recovery guidance.
"""

from typing import Any, Optional


class DataProcessingError(Exception):
    """
    Base exception for all data processing errors.

    Parent class for all domain-specific data errors.
    Provides context, suggestions, and recovery information.
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        context: Optional[dict] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a DataProcessingError.

        Args:
            message: Human-readable error message
            error_code: Machine-readable error code (e.g., 'BATCH_001')
            context: Additional context dict (e.g., batch_number, row_count)
            recovery_hint: Suggested recovery action
        """
        self.message = message
        self.error_code = error_code or "DATA_ERROR"
        self.context = context or {}
        self.recovery_hint = recovery_hint

        # Build detailed error message
        full_message = f"[{self.error_code}] {message}"
        if self.context:
            full_message += f" | Context: {self.context}"
        if self.recovery_hint:
            full_message += f" | Recovery: {self.recovery_hint}"

        super().__init__(full_message)


class ValidationError(DataProcessingError):
    """
    Raised when data validation fails.

    Use when input data doesn't meet requirements:
    - Schema mismatch
    - Type errors
    - Value out of range
    - Missing required fields
    """

    def __init__(
        self,
        message: str,
        field_name: Optional[str] = None,
        expected: Optional[Any] = None,
        actual: Optional[Any] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a ValidationError.

        Args:
            message: Error message
            field_name: Name of field that failed validation
            expected: Expected value/type
            actual: Actual value that was provided
            recovery_hint: How to fix the validation error
        """
        context = {}
        if field_name:
            context["field"] = field_name
        if expected is not None:
            context["expected"] = str(expected)
        if actual is not None:
            context["actual"] = str(actual)

        recovery_hint = recovery_hint or (
            f"Check that {field_name} has type {expected}" if field_name and expected else None
        )

        super().__init__(
            message, error_code="VALIDATION_ERROR", context=context, recovery_hint=recovery_hint
        )


class BatchProcessingError(DataProcessingError):
    """
    Raised when batch processing fails.

    Use when batch-level operations fail:
    - Batch size validation errors
    - Batch processing failures
    - Parallel processing issues
    - Batch accumulation errors
    """

    def __init__(
        self,
        message: str,
        batch_number: Optional[int] = None,
        batch_size: Optional[int] = None,
        failed_records: Optional[int] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a BatchProcessingError.

        Args:
            message: Error message
            batch_number: Which batch failed (0-indexed)
            batch_size: Size of the batch
            failed_records: How many records failed in the batch
            recovery_hint: How to recover
        """
        context = {}
        if batch_number is not None:
            context["batch_number"] = batch_number
        if batch_size is not None:
            context["batch_size"] = batch_size
        if failed_records is not None:
            context["failed_records"] = failed_records

        recovery_hint = recovery_hint or (
            f"Review batch {batch_number} records and retry" if batch_number is not None else None
        )

        super().__init__(
            message, error_code="BATCH_ERROR", context=context, recovery_hint=recovery_hint
        )


class TransformationError(DataProcessingError):
    """
    Raised when data transformation fails.

    Use when transformation operations fail:
    - Column mapping errors
    - Type conversion failures
    - Computed field errors
    - Function application failures
    """

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        input_data: Optional[Any] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a TransformationError.

        Args:
            message: Error message
            operation: Name of transformation operation
            input_data: Data that failed transformation (truncated if large)
            recovery_hint: How to fix the transformation
        """
        context = {}
        if operation:
            context["operation"] = operation
        if input_data is not None:
            # Truncate large data for logging
            input_str = str(input_data)
            if len(input_str) > 100:
                input_str = input_str[:97] + "..."
            context["input_sample"] = input_str

        recovery_hint = recovery_hint or (
            f"Check {operation} function logic and input data format" if operation else None
        )

        super().__init__(
            message, error_code="TRANSFORM_ERROR", context=context, recovery_hint=recovery_hint
        )


class CompressionError(DataProcessingError):
    """
    Raised when data compression/decompression fails.

    Use when compression operations fail:
    - Unsupported compression format
    - Corrupted compressed data
    - Compression algorithm errors
    - Decompression failures
    """

    def __init__(
        self,
        message: str,
        format: Optional[str] = None,
        file_size: Optional[int] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a CompressionError.

        Args:
            message: Error message
            format: Compression format (gzip, zstd, etc.)
            file_size: Size of file being compressed
            recovery_hint: How to fix compression issue
        """
        context = {}
        if format:
            context["format"] = format
        if file_size is not None:
            context["file_size_bytes"] = file_size

        recovery_hint = recovery_hint or (
            f"Verify file is valid {format} format"
            if format
            else "Check compression format and try again"
        )

        super().__init__(
            message, error_code="COMPRESSION_ERROR", context=context, recovery_hint=recovery_hint
        )


class PartitionError(DataProcessingError):
    """
    Raised when data partitioning fails.

    Use when partition operations fail:
    - Invalid partition strategy
    - Partition key errors
    - Partition range errors
    - Partitioning logic failures
    """

    def __init__(
        self,
        message: str,
        partition_key: Optional[str] = None,
        strategy: Optional[str] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a PartitionError.

        Args:
            message: Error message
            partition_key: Key used for partitioning
            strategy: Partitioning strategy (time, hash, range)
            recovery_hint: How to fix partition issue
        """
        context = {}
        if partition_key:
            context["partition_key"] = partition_key
        if strategy:
            context["strategy"] = strategy

        recovery_hint = recovery_hint or (
            f"Verify {partition_key} is valid for {strategy} partitioning"
            if partition_key and strategy
            else None
        )

        super().__init__(
            message, error_code="PARTITION_ERROR", context=context, recovery_hint=recovery_hint
        )


class DataFrameOperationError(DataProcessingError):
    """
    Raised when DataFrame operations fail.

    Use when DataFrame-specific operations fail:
    - Column operations (add, rename, drop)
    - Index operations
    - Join/merge errors
    - DataFrame shape mismatches
    """

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        dataframe_shape: Optional[tuple] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a DataFrameOperationError.

        Args:
            message: Error message
            operation: Type of operation (join, merge, etc.)
            dataframe_shape: Shape of DataFrame (rows, columns)
            recovery_hint: How to fix the operation
        """
        context = {}
        if operation:
            context["operation"] = operation
        if dataframe_shape:
            context["shape"] = f"{dataframe_shape[0]} rows x {dataframe_shape[1]} columns"

        recovery_hint = recovery_hint or (
            f"Check {operation} operation parameters and data shape" if operation else None
        )

        super().__init__(
            message, error_code="DATAFRAME_ERROR", context=context, recovery_hint=recovery_hint
        )


class QualityCheckError(DataProcessingError):
    """
    Raised when data quality checks fail.

    Use when quality metrics fail:
    - Null value checks
    - Uniqueness constraints
    - Business rule violations
    - Data profile mismatches
    """

    def __init__(
        self,
        message: str,
        check_name: Optional[str] = None,
        threshold: Optional[float] = None,
        actual: Optional[float] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a QualityCheckError.

        Args:
            message: Error message
            check_name: Name of quality check that failed
            threshold: Expected threshold value
            actual: Actual value that failed check
            recovery_hint: How to improve data quality
        """
        context = {}
        if check_name:
            context["check"] = check_name
        if threshold is not None:
            context["threshold"] = threshold
        if actual is not None:
            context["actual"] = actual

        recovery_hint = recovery_hint or (
            f"Investigate {check_name} violations and remediate data"
            if check_name
            else "Review quality check results and fix data"
        )

        super().__init__(
            message, error_code="QUALITY_ERROR", context=context, recovery_hint=recovery_hint
        )


class LineageTrackingError(DataProcessingError):
    """
    Raised when lineage tracking operations fail.

    Use when lineage tracking fails:
    - Lineage storage errors
    - Transformation tracking failures
    - Dependency resolution errors
    - Lineage retrieval errors
    """

    def __init__(
        self,
        message: str,
        job_name: Optional[str] = None,
        storage_type: Optional[str] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a LineageTrackingError.

        Args:
            message: Error message
            job_name: Name of job whose lineage failed to track
            storage_type: Storage backend (database, file, etc.)
            recovery_hint: How to fix tracking issue
        """
        context = {}
        if job_name:
            context["job_name"] = job_name
        if storage_type:
            context["storage"] = storage_type

        recovery_hint = recovery_hint or (
            f"Check {storage_type} storage connectivity and permissions"
            if storage_type
            else "Verify lineage storage is accessible"
        )

        super().__init__(
            message, error_code="LINEAGE_ERROR", context=context, recovery_hint=recovery_hint
        )


class PipelineExecutionError(DataProcessingError):
    """
    Raised when pipeline execution fails.

    Use when pipeline-level failures occur:
    - Stage execution failures
    - Pipeline state errors
    - Checkpoint failures
    - Pipeline recovery errors
    """

    def __init__(
        self,
        message: str,
        pipeline_name: Optional[str] = None,
        stage: Optional[str] = None,
        recovery_hint: Optional[str] = None,
    ):
        """
        Initialize a PipelineExecutionError.

        Args:
            message: Error message
            pipeline_name: Name of pipeline that failed
            stage: Pipeline stage where failure occurred
            recovery_hint: How to recover or retry
        """
        context = {}
        if pipeline_name:
            context["pipeline"] = pipeline_name
        if stage:
            context["stage"] = stage

        recovery_hint = recovery_hint or (
            f"Review {stage} stage logs and retry pipeline"
            if stage
            else "Check pipeline execution logs and retry"
        )

        super().__init__(
            message, error_code="PIPELINE_ERROR", context=context, recovery_hint=recovery_hint
        )


# Convenience function for creating errors with validators integration
def ensure_data_valid(
    condition: bool,
    message: str,
    error_code: str = "VALIDATION_ERROR",
    context: Optional[dict] = None,
    recovery_hint: Optional[str] = None,
) -> None:
    """
    Validate a condition and raise ValidationError if it fails.

    Integrates with axiompy.validators pattern for consistent error handling.

    Args:
        condition: Must be True for validation to pass
        message: Error message if validation fails
        error_code: Machine-readable error code
        context: Additional context dict
        recovery_hint: Suggested recovery action

    Raises:
        ValidationError: If condition is False

    Example:
        >>> ensure_data_valid(df.shape[0] > 0, "DataFrame cannot be empty")
        >>> ensure_data_valid(
        ...     all(col in df.columns for col in required_cols),
        ...     f"Missing required columns",
        ...     context={'required': required_cols, 'available': df.columns.tolist()}
        ... )
    """
    if not condition:
        raise DataProcessingError(
            message, error_code=error_code, context=context, recovery_hint=recovery_hint
        )
