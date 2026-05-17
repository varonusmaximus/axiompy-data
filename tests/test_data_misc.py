"""
Unit tests for miscellaneous axiompy.data modules.

Tests BatchProcessor, FormatConverter, and LineageTracker.
"""

import pandas as pd
import pytest

from axiompy.data.processing.batch import (
    BatchProcessorFactory,
    ListBatchProcessor,
    PandasBatchProcessor,
)
from axiompy.data.export import FormatConverter
from axiompy.data.processing.lineage import (
    LineageTrackerFactory,
    PandasLineageTracker,
)
from axiompy.data.types import DataEngine, DataFormat

# ============================================================================
# Batch Processing Tests
# ============================================================================


class TestPandasBatchProcessor:
    """Test PandasBatchProcessor implementation."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pd.DataFrame({"id": range(1, 101), "value": range(100, 200)})

    @pytest.fixture
    def processor(self):
        """Create batch processor."""
        return PandasBatchProcessor(batch_size=10)

    def test_iter_batches(self, processor, sample_df):
        """Test iterating over batches."""
        batches = list(processor.iter_batches(sample_df))

        assert len(batches) == 10  # 100 rows / 10 per batch
        assert len(batches[0]) == 10
        assert len(batches[-1]) == 10

    def test_iter_batches_uneven(self, processor):
        """Test batching with uneven division."""
        df = pd.DataFrame({"id": range(1, 26)})  # 25 rows
        batches = list(processor.iter_batches(df, batch_size=10))

        assert len(batches) == 3
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5

    def test_process_batches_simple(self, processor, sample_df):
        """Test processing batches with simple transformation."""

        def double_values(batch):
            batch["value"] = batch["value"] * 2
            return batch

        results = processor.process_batches(data=sample_df, transform_func=double_values)

        assert results["batches_processed"] == 10
        assert results["batches_failed"] == 0

    def test_process_batches_with_sink(self, processor, sample_df):
        """Test processing batches with sink function."""
        processed_batches = []

        def transform(batch):
            return batch["value"].sum()

        def sink(result):
            processed_batches.append(result)

        results = processor.process_batches(data=sample_df, transform_func=transform, sink=sink)

        assert len(processed_batches) == 10
        assert results["batches_processed"] == 10

    def test_process_batches_with_error_continue(self, processor, sample_df):
        """Test processing continues on error when fail_fast=False."""
        call_count = {"count": 0}

        def failing_transform(batch):
            call_count["count"] += 1
            if call_count["count"] == 3:
                raise ValueError("Batch 3 failed!")
            return batch

    def test_process_batches_with_parallel_workers(self, processor, sample_df):
        """Test processing batches with parallel workers."""
        processor = PandasBatchProcessor(batch_size=10, max_workers=2)

        def transform(batch):
            return len(batch)

        results = processor.process_batches(data=sample_df, transform_func=transform)

        assert results["batches_processed"] > 0

    def test_process_batches_with_tqdm_progress(self, processor, sample_df):
        """Test processing batches with progress bar."""
        processor = PandasBatchProcessor(batch_size=10, show_progress=True)

        def transform(batch):
            return batch

        results = processor.process_batches(data=sample_df, transform_func=transform)

        assert results["batches_processed"] == 10

    def test_list_batch_processor(self):
        """Test ListBatchProcessor."""
        processor = ListBatchProcessor(batch_size=5)
        data = list(range(15))

        batches = list(processor.iter_batches(data))

        assert len(batches) == 3
        assert len(batches[0]) == 5
        assert len(batches[2]) == 5

    def test_list_batch_processor_process(self):
        """Test ListBatchProcessor process_batches."""
        processor = ListBatchProcessor(batch_size=5)
        data = list(range(15))

        def transform(batch):
            return sum(batch)

        results = processor.process_batches(data=data, transform_func=transform)

        assert results["batches_processed"] == 3
        assert results["batches_failed"] == 0

    def test_list_batch_processor_with_errors(self, processor, sample_df):
        """Test ListBatchProcessor with errors."""
        call_count = {"count": 0}

        def failing_transform(batch):
            call_count["count"] += 1
            if call_count["count"] == 3:
                raise ValueError("Batch 3 failed!")
            return batch

        results = processor.process_batches(
            data=sample_df, transform_func=failing_transform, fail_fast=False
        )

        assert results["batches_processed"] == 9
        assert results["batches_failed"] == 1
        assert len(results["errors"]) == 1

    def test_process_batches_fail_fast(self, processor, sample_df):
        """Test processing stops on error when fail_fast=True."""
        call_count = {"count": 0}

        def failing_transform(batch):
            call_count["count"] += 1
            if call_count["count"] == 3:
                raise ValueError("Batch 3 failed!")
            return batch

        with pytest.raises(ValueError, match="Batch 3 failed!"):
            processor.process_batches(
                data=sample_df, transform_func=failing_transform, fail_fast=True
            )


class TestListBatchProcessor:
    """Test ListBatchProcessor implementation."""

    def test_iter_batches_list(self):
        """Test batching a list."""
        processor = ListBatchProcessor(batch_size=5)
        data = list(range(1, 21))  # 20 items

        batches = list(processor.iter_batches(data))

        assert len(batches) == 4
        assert batches[0] == [1, 2, 3, 4, 5]
        assert batches[-1] == [16, 17, 18, 19, 20]

    def test_process_list_batches(self):
        """Test processing list batches."""
        processor = ListBatchProcessor(batch_size=5)
        data = list(range(1, 21))

        def sum_batch(batch):
            return sum(batch)

        sums = []

        results = processor.process_batches(
            data=data, transform_func=sum_batch, sink=lambda x: sums.append(x)
        )

        assert len(sums) == 4
        assert results["batches_processed"] == 4


class TestBatchProcessorFactory:
    """Test BatchProcessorFactory."""

    def test_create_pandas_processor(self):
        """Test creating Pandas processor explicitly."""
        processor = BatchProcessorFactory.create(DataEngine.PANDAS, batch_size=10)

        assert isinstance(processor, PandasBatchProcessor)
        assert processor.batch_size == 10

    def test_create_auto_pandas(self):
        """Test auto-detection of Pandas engine."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        processor = BatchProcessorFactory.create_auto(df, batch_size=10)

        assert isinstance(processor, PandasBatchProcessor)

    def test_create_auto_list(self):
        """Test auto-detection of list."""
        data = [1, 2, 3, 4, 5]
        processor = BatchProcessorFactory.create_auto(data, batch_size=2)

        assert isinstance(processor, ListBatchProcessor)


# ============================================================================
# Format Conversion Tests
# ============================================================================


class TestFormatConverter:
    """Test FormatConverter class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [100, 200, 300]}
        )

    @pytest.fixture
    def converter(self):
        """Create FormatConverter instance."""
        return FormatConverter()

    def test_convert_to_csv_bytes(self, converter, sample_df):
        """Test converting DataFrame to CSV bytes."""
        result = converter.convert(
            sample_df, from_format=DataFormat.CSV, to_format=DataFormat.CSV, output_path=None
        )

        assert isinstance(result, bytes)
        assert b"Alice" in result
        assert b"Bob" in result

    def test_convert_to_json_bytes(self, converter, sample_df):
        """Test converting DataFrame to JSON bytes."""
        result = converter.convert(
            sample_df, from_format=DataFormat.CSV, to_format=DataFormat.JSON, output_path=None
        )

        assert isinstance(result, bytes)
        assert b"Alice" in result

    def test_convert_to_parquet_bytes(self, converter, sample_df):
        """Test converting DataFrame to Parquet bytes."""
        result = converter.convert(
            sample_df, from_format=DataFormat.CSV, to_format=DataFormat.PARQUET, output_path=None
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_convert_csv_to_file(self, converter, sample_df, tmp_path):
        """Test converting to file."""
        output_file = tmp_path / "output.csv"

        result = converter.convert(
            sample_df,
            from_format=DataFormat.CSV,
            to_format=DataFormat.CSV,
            output_path=str(output_file),
        )

        assert result == str(output_file)
        assert output_file.exists()

    def test_csv_to_parquet_convenience(self, tmp_path):
        """Test csv_to_parquet convenience method."""
        # Create CSV file
        csv_file = tmp_path / "data.csv"
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        df.to_csv(csv_file, index=False)

        # Convert
        parquet_file = tmp_path / "data.parquet"
        result = FormatConverter.csv_to_parquet(str(csv_file), str(parquet_file))

        assert result == str(parquet_file)
        assert parquet_file.exists()

    def test_parquet_to_csv_convenience(self, tmp_path):
        """Test parquet_to_csv convenience method."""
        # Create Parquet file
        parquet_file = tmp_path / "data.parquet"
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        df.to_parquet(parquet_file)

        # Convert
        csv_file = tmp_path / "data.csv"
        result = FormatConverter.parquet_to_csv(str(parquet_file), str(csv_file))

        assert result == str(csv_file)
        assert csv_file.exists()


# ============================================================================
# Lineage Tracking Tests
# ============================================================================


class TestPandasLineageTracker:
    """Test PandasLineageTracker implementation."""

    @pytest.fixture
    def tracker(self):
        """Create lineage tracker."""
        return PandasLineageTracker()

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

    def test_track_transformation_basic(self, tracker, sample_df):
        """Test tracking a basic transformation."""
        record = tracker.track_transformation(
            job_name="test_job",
            input_sources=["raw_data"],
            output_targets=["processed_data"],
            transformation="Clean and filter data",
            data_in=sample_df,
            data_out=sample_df,
        )

        assert record.job_name == "test_job"
        assert record.input_sources == ["raw_data"]
        assert record.output_targets == ["processed_data"]
        assert record.row_count_in == 3
        assert record.row_count_out == 3

    def test_track_transformation_with_metadata(self, tracker, sample_df):
        """Test tracking with metadata."""
        metadata = {"version": "1.0", "environment": "dev", "user": "test_user"}

        record = tracker.track_transformation(
            job_name="test_job",
            input_sources=["input"],
            output_targets=["output"],
            transformation="Transform",
            metadata=metadata,
        )

        assert record.metadata == metadata

    def test_get_lineage_records(self, tracker, sample_df):
        """Test retrieving lineage records."""
        tracker.track_transformation(
            job_name="job1",
            input_sources=["input1"],
            output_targets=["output1"],
            transformation="Transform 1",
        )

        tracker.track_transformation(
            job_name="job2",
            input_sources=["input2"],
            output_targets=["output2"],
            transformation="Transform 2",
        )

        records = tracker.get_lineage_records()

        assert len(records) == 2

    def test_get_lineage_records_filtered(self, tracker, sample_df):
        """Test retrieving filtered lineage records."""
        tracker.track_transformation(
            job_name="job1",
            input_sources=["input1"],
            output_targets=["output1"],
            transformation="Transform 1",
        )

        tracker.track_transformation(
            job_name="job2",
            input_sources=["input2"],
            output_targets=["output2"],
            transformation="Transform 2",
        )

        records = tracker.get_lineage_records(job_name="job1")

        assert len(records) == 1
        assert records[0].job_name == "job1"

    def test_get_upstream_sources(self, tracker):
        """Test getting upstream sources."""
        tracker.track_transformation(
            job_name="job1",
            input_sources=["source_a", "source_b"],
            output_targets=["intermediate"],
            transformation="Combine",
        )

        tracker.track_transformation(
            job_name="job2",
            input_sources=["intermediate"],
            output_targets=["final"],
            transformation="Aggregate",
        )

        upstream = tracker.get_upstream_sources("final")

        assert "intermediate" in upstream

    def test_get_downstream_targets(self, tracker):
        """Test getting downstream targets."""
        tracker.track_transformation(
            job_name="job1",
            input_sources=["raw"],
            output_targets=["intermediate"],
            transformation="Clean",
        )

        tracker.track_transformation(
            job_name="job2",
            input_sources=["intermediate"],
            output_targets=["final"],
            transformation="Aggregate",
        )

        downstream = tracker.get_downstream_targets("intermediate")

        assert "final" in downstream


class TestLineageTrackerFactory:
    """Test LineageTrackerFactory."""

    def test_create_pandas_tracker(self):
        """Test creating Pandas tracker explicitly."""
        tracker = LineageTrackerFactory.create(DataEngine.PANDAS)

        assert isinstance(tracker, PandasLineageTracker)

    def test_create_auto_pandas(self):
        """Test auto-detection of Pandas engine."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        tracker = LineageTrackerFactory.create_auto(df)

        assert isinstance(tracker, PandasLineageTracker)


# ============================================================================
# Integration Tests
# ============================================================================


class TestDataIntegration:
    """Integration tests combining multiple modules."""

    def test_etl_with_lineage_and_quality(self):
        """Test ETL workflow with lineage tracking and quality checks."""
        from axiompy.data.processing.quality import DataProfilerFactory
        from axiompy.data.processing.transform import DataTransformerFactory

        # Source data
        raw_df = pd.DataFrame({"id": [1, 2, 3, 3, 5], "value": [10, None, 30, 30, 50]})

        # Track lineage
        tracker = LineageTrackerFactory.create_auto(raw_df)

        # Profile initial quality
        profiler = DataProfilerFactory.create_auto(raw_df)
        initial_report = profiler.profile(raw_df)

        assert initial_report.duplicate_count > 0
        assert initial_report.null_counts["value"] > 0

        # Transform
        transformer = DataTransformerFactory.create_auto(raw_df)
        clean_df = transformer.fill_nulls(raw_df, strategy="mean", columns=["value"])
        clean_df = transformer.deduplicate(clean_df, subset=["id"])

        # Track transformation
        tracker.track_transformation(
            job_name="clean_data",
            input_sources=["raw_data"],
            output_targets=["clean_data"],
            transformation="Fill nulls and deduplicate",
            data_in=raw_df,
            data_out=clean_df,
        )

        # Verify improvements
        final_report = profiler.profile(clean_df)

        assert final_report.duplicate_count == 0
        assert final_report.null_counts["value"] == 0
        assert len(clean_df) < len(raw_df)

        # Check lineage
        records = tracker.get_lineage_records()
        assert len(records) == 1
        assert records[0].row_count_in == 5
        assert records[0].row_count_out == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
