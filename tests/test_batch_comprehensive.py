"""
Comprehensive tests for batch processing module.

Tests all batch processor implementations and parallel processing.
"""

import time

import pytest

from axiompy.data.batch import (
    BatchProcessorFactory,
    ListBatchProcessor,
    PandasBatchProcessor,
    SparkBatchProcessor,
)
from axiompy.data.types import DataEngine


class TestPandasBatchProcessorComprehensive:
    """Comprehensive tests for PandasBatchProcessor."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        return pd.DataFrame(
            {
                "id": range(1, 101),
                "value": range(100, 200),
                "category": ["A" if i % 2 == 0 else "B" for i in range(100)],
            }
        )

    @pytest.fixture
    def processor(self):
        """Create processor instance."""
        return PandasBatchProcessor(batch_size=10)

    # ====== iter_batches Tests ======

    def test_iter_batches_basic(self, processor, sample_df):
        """Test basic batch iteration."""
        batches = list(processor.iter_batches(sample_df))

        assert len(batches) == 10  # 100 / 10
        assert len(batches[0]) == 10
        assert len(batches[-1]) == 10

    def test_iter_batches_uneven(self, processor, sample_df):
        """Test batch iteration with uneven division."""
        pytest.importorskip("pandas")
        import pandas as pd

        df = pd.DataFrame({"id": range(1, 26)})  # 25 rows
        batches = list(processor.iter_batches(df, batch_size=10))

        assert len(batches) == 3
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5

    def test_iter_batches_single_batch(self, processor, sample_df):
        """Test iteration with batch size larger than data."""
        batches = list(processor.iter_batches(sample_df, batch_size=200))

        assert len(batches) == 1
        assert len(batches[0]) == 100

    def test_iter_batches_small_batch(self, processor, sample_df):
        """Test iteration with small batch size."""
        batches = list(processor.iter_batches(sample_df, batch_size=5))

        assert len(batches) == 20
        assert all(len(batch) == 5 for batch in batches)

    def test_iter_batches_empty_dataframe(self, processor):
        """Test iteration with empty DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        df = pd.DataFrame()
        batches = list(processor.iter_batches(df))

        assert len(batches) == 0

    # ====== process_batches Sequential Tests ======

    def test_process_batches_sequential_basic(self, processor, sample_df):
        """Test sequential processing."""

        def transform(batch):
            return batch["value"].sum()

        results = processor.process_batches(sample_df, transform)

        assert results["batches_processed"] == 10
        assert results["batches_failed"] == 0
        assert len(results["errors"]) == 0

    def test_process_batches_sequential_with_sink(self, processor, sample_df):
        """Test sequential processing with sink function."""
        processed_batches = []

        def transform(batch):
            return batch["value"].sum()

        def sink(result):
            processed_batches.append(result)

        results = processor.process_batches(sample_df, transform, sink=sink)

        assert len(processed_batches) == 10
        assert results["batches_processed"] == 10

    def test_process_batches_sequential_error_continue(self, processor, sample_df):
        """Test sequential processing continues on error (fail_fast=False)."""
        call_count = {"count": 0}

        def failing_transform(batch):
            call_count["count"] += 1
            if call_count["count"] == 3:
                raise ValueError("Batch 3 failed!")
            return batch

        results = processor.process_batches(sample_df, failing_transform, fail_fast=False)

        assert results["batches_processed"] == 9
        assert results["batches_failed"] == 1
        assert len(results["errors"]) == 1
        assert results["errors"][0]["batch"] == 2

    def test_process_batches_sequential_error_fail_fast(self, processor, sample_df):
        """Test sequential processing stops on error (fail_fast=True)."""
        call_count = {"count": 0}

        def failing_transform(batch):
            call_count["count"] += 1
            if call_count["count"] == 3:
                raise ValueError("Batch 3 failed!")
            return batch

        with pytest.raises(ValueError, match="Batch 3 failed!"):
            processor.process_batches(sample_df, failing_transform, fail_fast=True)

    def test_process_batches_sequential_with_batch_size_override(self, processor, sample_df):
        """Test sequential processing with batch size override."""

        def transform(batch):
            return len(batch)

        results = processor.process_batches(sample_df, transform, batch_size=25)

        assert results["batches_processed"] == 4  # 100 / 25

    # ====== process_batches Parallel Tests ======

    def test_process_batches_parallel_basic(self, sample_df):
        """Test parallel processing."""
        processor = PandasBatchProcessor(batch_size=10, max_workers=2)

        def transform(batch):
            time.sleep(0.01)  # Simulate work
            return len(batch)

        results = processor.process_batches(sample_df, transform)

        assert results["batches_processed"] == 10
        assert results["batches_failed"] == 0

    def test_process_batches_parallel_with_sink(self, sample_df):
        """Test parallel processing with sink."""
        processor = PandasBatchProcessor(batch_size=10, max_workers=2)
        processed_results = []

        def transform(batch):
            return len(batch)

        def sink(result):
            processed_results.append(result)

        results = processor.process_batches(sample_df, transform, sink=sink)

        assert len(processed_results) == 10
        assert results["batches_processed"] == 10

    def test_process_batches_parallel_error_continue(self, sample_df):
        """Test parallel processing continues on error."""
        processor = PandasBatchProcessor(batch_size=10, max_workers=2)
        call_count = {"count": 0}
        lock = __import__("threading").Lock()

        def failing_transform(batch):
            with lock:
                call_count["count"] += 1
                batch_num = call_count["count"]
            if batch_num == 3:
                raise ValueError(f"Batch {batch_num} failed!")
            return batch

        results = processor.process_batches(sample_df, failing_transform, fail_fast=False)

        assert results["batches_failed"] == 1
        assert len(results["errors"]) == 1

    def test_process_batches_parallel_error_fail_fast(self, sample_df):
        """Test parallel processing fails fast on error."""
        processor = PandasBatchProcessor(batch_size=10, max_workers=2)

        def failing_transform(batch):
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            processor.process_batches(sample_df, failing_transform, fail_fast=True)

    def test_process_batches_parallel_many_workers(self, sample_df):
        """Test parallel processing with many workers."""
        processor = PandasBatchProcessor(batch_size=10, max_workers=4)

        def transform(batch):
            return len(batch)

        results = processor.process_batches(sample_df, transform)

        assert results["batches_processed"] == 10

    # ====== progress bar Tests ======

    def test_process_batches_with_progress(self, sample_df):
        """Test processing with progress bar enabled."""
        processor = PandasBatchProcessor(batch_size=10, show_progress=True)

        def transform(batch):
            return len(batch)

        results = processor.process_batches(sample_df, transform)

        assert results["batches_processed"] == 10


class TestListBatchProcessorComprehensive:
    """Comprehensive tests for ListBatchProcessor."""

    @pytest.fixture
    def processor(self):
        """Create list batch processor."""
        return ListBatchProcessor(batch_size=5)

    def test_iter_batches_list(self, processor):
        """Test batch iteration over list."""
        data = list(range(15))
        batches = list(processor.iter_batches(data))

        assert len(batches) == 3
        assert batches[0] == [0, 1, 2, 3, 4]
        assert batches[1] == [5, 6, 7, 8, 9]
        assert batches[2] == [10, 11, 12, 13, 14]

    def test_iter_batches_uneven_list(self, processor):
        """Test batch iteration with uneven division."""
        data = list(range(13))
        batches = list(processor.iter_batches(data))

        assert len(batches) == 3
        assert len(batches[0]) == 5
        assert len(batches[1]) == 5
        assert len(batches[2]) == 3

    def test_iter_batches_generator(self, processor):
        """Test batch iteration over generator."""

        def gen():
            for i in range(15):
                yield i

        batches = list(processor.iter_batches(gen()))

        assert len(batches) == 3
        assert batches[0] == [0, 1, 2, 3, 4]

    def test_iter_batches_tuple(self, processor):
        """Test batch iteration over tuple."""
        data = tuple(range(10))
        batches = list(processor.iter_batches(data))

        assert len(batches) == 2
        assert batches[0] == [0, 1, 2, 3, 4]

    def test_process_batches_list(self, processor):
        """Test processing list batches."""
        data = list(range(15))

        def transform(batch):
            return sum(batch)

        results = processor.process_batches(data, transform)

        assert results["batches_processed"] == 3
        assert results["batches_failed"] == 0

    def test_process_batches_list_parallel(self):
        """Test parallel processing of lists."""
        processor = ListBatchProcessor(batch_size=5, max_workers=2)
        data = list(range(15))

        def transform(batch):
            return len(batch)

        results = processor.process_batches(data, transform)

        assert results["batches_processed"] == 3


class TestSparkBatchProcessorComprehensive:
    """Comprehensive tests for SparkBatchProcessor."""

    def test_spark_processor_creation(self):
        """Test Spark processor creation."""
        pytest.importorskip("pyspark")

        processor = SparkBatchProcessor(batch_size=1000)

        assert processor.batch_size == 1000
        assert processor.engine == DataEngine.SPARK

    def test_spark_iter_batches(self):
        """Test Spark DataFrame batching."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local").getOrCreate()

        try:
            # Create test data
            data = [(i, f"val_{i}") for i in range(100)]
            sdf = spark.createDataFrame(data, ["id", "value"])

            processor = SparkBatchProcessor(batch_size=25)
            batches = list(processor.iter_batches(sdf))

            # Should have 4 batches (100 / 25)
            assert len(batches) == 4
        finally:
            spark.stop()


class TestBatchProcessorFactoryComprehensive:
    """Comprehensive tests for BatchProcessorFactory."""

    def test_create_pandas_processor(self):
        """Test creating Pandas processor."""
        processor = BatchProcessorFactory.create(DataEngine.PANDAS, batch_size=500)

        assert isinstance(processor, PandasBatchProcessor)
        assert processor.batch_size == 500

    def test_create_with_workers(self):
        """Test creating processor with workers."""
        processor = BatchProcessorFactory.create(DataEngine.PANDAS, batch_size=500, max_workers=4)

        assert processor.max_workers == 4

    def test_create_with_progress(self):
        """Test creating processor with progress bar."""
        processor = BatchProcessorFactory.create(DataEngine.PANDAS, show_progress=True)

        assert processor.show_progress is True

    def test_create_with_settings(self):
        """Test creating processor with settings."""
        settings = {"option1": "value1"}
        processor = BatchProcessorFactory.create(DataEngine.PANDAS, settings=settings)

        assert processor.settings == settings

    def test_create_unsupported_engine(self):
        """Test creating processor with unsupported engine."""
        mock_engine = type("MockEngine", (), {"value": "mock"})()

        with pytest.raises(ValueError, match="Unsupported engine"):
            BatchProcessorFactory.create(mock_engine)

    def test_create_auto_pandas_dataframe(self):
        """Test auto-detecting Pandas DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3]})
        processor = BatchProcessorFactory.create_auto(df)

        assert isinstance(processor, PandasBatchProcessor)

    def test_create_auto_list(self):
        """Test auto-detecting list."""
        data = [1, 2, 3, 4, 5]
        processor = BatchProcessorFactory.create_auto(data)

        assert isinstance(processor, ListBatchProcessor)

    def test_create_auto_tuple(self):
        """Test auto-detecting tuple."""
        data = (1, 2, 3, 4, 5)
        processor = BatchProcessorFactory.create_auto(data)

        assert isinstance(processor, ListBatchProcessor)

    def test_create_auto_generator(self):
        """Test auto-detecting generator."""

        def gen():
            for i in range(5):
                yield i

        processor = BatchProcessorFactory.create_auto(gen())

        assert isinstance(processor, ListBatchProcessor)

    def test_create_auto_unknown_type(self):
        """Test auto-detecting unknown type raises error."""
        with pytest.raises(ValueError, match="Cannot auto-detect engine"):
            BatchProcessorFactory.create_auto({"dict": "value"})

    def test_register_processor(self):
        """Test registering custom processor."""

        class CustomProcessor(PandasBatchProcessor):
            pass

        BatchProcessorFactory.register_processor(DataEngine.PANDAS, CustomProcessor)

        processor = BatchProcessorFactory.create(DataEngine.PANDAS)
        assert isinstance(processor, CustomProcessor)

    def test_register_invalid_processor(self):
        """Test registering invalid processor."""

        class NotAProcessor:
            pass

        with pytest.raises(TypeError, match="must inherit from BatchProcessor"):
            BatchProcessorFactory.register_processor(DataEngine.PANDAS, NotAProcessor)

    # ====== Integration Tests ======

    def test_batch_processor_workflow(self):
        """Test complete batch processing workflow."""
        pytest.importorskip("pandas")
        import pandas as pd

        # Create data
        df = pd.DataFrame({"id": range(1, 51), "value": range(100, 150)})

        # Create processor
        processor = BatchProcessorFactory.create_auto(df, batch_size=10)

        # Process batches
        results = []

        def transform(batch):
            return batch["value"].mean()

        def sink(result):
            results.append(result)

        proc_results = processor.process_batches(df, transform, sink=sink)

        assert proc_results["batches_processed"] == 5
        assert len(results) == 5

    def test_multiple_engines(self):
        """Test creating processors for multiple engines."""
        pytest.importorskip("pandas")
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3]})

        # Pandas processor
        pandas_proc = BatchProcessorFactory.create_auto(df)
        assert isinstance(pandas_proc, PandasBatchProcessor)

        # List processor
        list_proc = BatchProcessorFactory.create_auto([1, 2, 3])
        assert isinstance(list_proc, ListBatchProcessor)
