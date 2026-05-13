"""
Unit tests for Spark implementations in axiompy.data module.

Tests all Spark data processing implementations with local Spark session.
These tests verify correctness of Spark implementations without requiring
a distributed cluster.
"""

import pytest

# Import Spark-related modules
pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from axiompy.data.cdc import ChangeDetectorFactory, SparkChangeDetector
from axiompy.data.lineage import LineageTrackerFactory, SparkLineageTracker
from axiompy.data.quality import DataProfilerFactory, SparkDataProfiler
from axiompy.data.transform import DataTransformerFactory, SparkDataTransformer
from axiompy.data.types import DataEngine, DataExpectation

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def spark():
    """Create local Spark session for testing."""
    # Note: PYSPARK_PYTHON is set in conftest.py to fix version mismatch
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("axiompy-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


@pytest.fixture
def sample_spark_df(spark):
    """Create sample Spark DataFrame for testing."""
    data = [
        (1, "Alice", 25, 85.5, "active"),
        (2, "Bob", 30, 92.0, "active"),
        (3, "Charlie", None, 78.5, "inactive"),
        (4, "David", 45, None, "active"),
        (5, "Eve", 28, 95.0, "pending"),
        (None, "Frank", 35, 88.0, "active"),
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("score", DoubleType(), True),
            StructField("status", StringType(), False),
        ]
    )

    return spark.createDataFrame(data, schema)


# ============================================================================
# Spark Data Quality Tests
# ============================================================================


class TestSparkDataProfiler:
    """Test SparkDataProfiler implementation."""

    def test_profile_basic(self, sample_spark_df):
        """Test basic Spark DataFrame profiling."""
        profiler = SparkDataProfiler()
        report = profiler.profile(sample_spark_df)

        assert report.row_count == 6
        assert report.column_count == 5
        assert report.metadata["engine"] == "spark"

    def test_profile_null_counts(self, sample_spark_df):
        """Test null count detection in Spark."""
        profiler = SparkDataProfiler()
        report = profiler.profile(sample_spark_df)

        assert report.null_counts["id"] == 1
        assert report.null_counts["age"] == 1
        assert report.null_counts["score"] == 1
        assert report.null_counts["name"] == 0
        assert report.null_counts["status"] == 0

    def test_profile_duplicates(self, spark):
        """Test duplicate detection in Spark."""
        data = [(1, 10), (2, 20), (3, 30), (2, 20)]
        df = spark.createDataFrame(data, ["id", "value"])

        profiler = SparkDataProfiler()
        report = profiler.profile(df)

        assert report.duplicate_count == 1

    def test_profile_statistics(self, sample_spark_df):
        """Test statistical calculations in Spark."""
        profiler = SparkDataProfiler()
        report = profiler.profile(sample_spark_df)

        # Check age statistics
        age_stats = report.statistics["age"]
        assert "min" in age_stats
        assert "max" in age_stats
        assert "mean" in age_stats
        assert age_stats["min"] == 25
        assert age_stats["max"] == 45

    def test_validate_expectations_not_null(self, spark):
        """Test not_null expectation with Spark."""
        data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        df = spark.createDataFrame(data, ["id", "name"])

        profiler = SparkDataProfiler()
        expectations = [DataExpectation(name="name_not_null", column="name", condition="not_null")]

        results = profiler.validate_expectations(df, expectations)

        assert results["passed"] == 1
        assert results["failed"] == 0
        assert results["success"] is True

    def test_validate_expectations_unique(self, spark):
        """Test unique expectation with Spark."""
        data = [(1, "a@ex.com"), (2, "b@ex.com"), (3, "a@ex.com")]
        df = spark.createDataFrame(data, ["id", "email"])

        profiler = SparkDataProfiler()
        expectations = [
            DataExpectation(name="id_unique", column="id", condition="unique"),
            DataExpectation(name="email_unique", column="email", condition="unique"),
        ]

        results = profiler.validate_expectations(df, expectations)

        assert results["passed"] == 1  # id is unique
        assert results["failed"] == 1  # email has duplicates

    def test_validate_expectations_in_range(self, spark):
        """Test in_range expectation with Spark."""
        data = [(1, 25), (2, 30), (3, 45), (4, 28)]
        df = spark.createDataFrame(data, ["id", "age"])

        profiler = SparkDataProfiler()
        expectations = [
            DataExpectation(
                name="age_range", column="age", condition="in_range", params={"min": 0, "max": 100}
            )
        ]

        results = profiler.validate_expectations(df, expectations)

        assert results["passed"] == 1
        assert results["success"] is True

    def test_validate_expectations_in_set(self, spark):
        """Test in_set expectation with Spark."""
        data = [(1, "active"), (2, "inactive"), (3, "pending")]
        df = spark.createDataFrame(data, ["id", "status"])

        profiler = SparkDataProfiler()
        expectations = [
            DataExpectation(
                name="status_valid",
                column="status",
                condition="in_set",
                params={"values": ["active", "inactive", "pending"]},
            )
        ]

        results = profiler.validate_expectations(df, expectations)

        assert results["passed"] == 1
        assert results["success"] is True

    def test_check_schema(self, spark):
        """Test schema validation with Spark."""
        data = [(1, "Alice", 25)]
        df = spark.createDataFrame(data, ["id", "name", "age"])

        profiler = SparkDataProfiler()
        expected_schema = {"id": "IntegerType", "name": "StringType", "age": "IntegerType"}

        result = profiler.check_schema(df, expected_schema)

        assert result["valid"] is True
        assert len(result["issues"]) == 0


class TestSparkDataProfilerFactory:
    """Test factory with Spark DataFrames."""

    def test_create_spark_profiler(self):
        """Test creating Spark profiler explicitly."""
        profiler = DataProfilerFactory.create(DataEngine.SPARK)

        assert isinstance(profiler, SparkDataProfiler)

    def test_create_auto_spark(self, spark):
        """Test auto-detection of Spark engine."""
        df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        profiler = DataProfilerFactory.create_auto(df)

        assert isinstance(profiler, SparkDataProfiler)


# ============================================================================
# Spark Data Transformation Tests
# ============================================================================


class TestSparkDataTransformer:
    """Test SparkDataTransformer implementation."""

    def test_rename_columns(self, spark):
        """Test column renaming in Spark."""
        data = [(1, "Alice", "Smith")]
        df = spark.createDataFrame(data, ["id", "first_name", "last_name"])

        transformer = SparkDataTransformer()
        result = transformer.rename_columns(df, {"first_name": "fname", "last_name": "lname"})

        assert "fname" in result.columns
        assert "lname" in result.columns
        assert "first_name" not in result.columns

    def test_select_columns(self, spark):
        """Test column selection in Spark."""
        data = [(1, "Alice", 25, 85.5)]
        df = spark.createDataFrame(data, ["id", "name", "age", "score"])

        transformer = SparkDataTransformer()
        result = transformer.select_columns(df, ["id", "name"])

        assert result.columns == ["id", "name"]
        assert result.count() == 1

    def test_drop_columns(self, spark):
        """Test column dropping in Spark."""
        data = [(1, "Alice", 25, 85.5)]
        df = spark.createDataFrame(data, ["id", "name", "age", "score"])

        transformer = SparkDataTransformer()
        result = transformer.drop_columns(df, ["age", "score"])

        assert "age" not in result.columns
        assert "score" not in result.columns
        assert len(result.columns) == 2

    def test_fill_nulls_value(self, spark):
        """Test filling nulls with value in Spark."""
        data = [(1, "Alice", 25), (2, "Bob", None), (3, "Charlie", 35)]
        df = spark.createDataFrame(data, ["id", "name", "age"])

        transformer = SparkDataTransformer()
        result = transformer.fill_nulls(df, strategy="value", value=0, columns=["age"])

        ages = [row.age for row in result.collect()]
        assert None not in ages
        assert 0 in ages

    def test_fill_nulls_mean(self, spark):
        """Test filling nulls with mean in Spark."""
        data = [(1, 25), (2, None), (3, 35)]
        df = spark.createDataFrame(data, ["id", "age"])

        transformer = SparkDataTransformer()
        result = transformer.fill_nulls(df, strategy="mean", columns=["age"])

        ages = [row.age for row in result.collect()]
        assert None not in ages

    def test_drop_nulls(self, spark):
        """Test dropping rows with nulls in Spark."""
        data = [(1, "Alice", 25), (2, "Bob", None), (3, "Charlie", 35)]
        df = spark.createDataFrame(data, ["id", "name", "age"])

        transformer = SparkDataTransformer()
        result = transformer.drop_nulls(df, how="any")

        assert result.count() == 2

    def test_deduplicate(self, spark):
        """Test deduplication in Spark."""
        data = [(1, 10), (2, 20), (3, 30), (2, 20), (4, 40)]
        df = spark.createDataFrame(data, ["id", "value"])

        transformer = SparkDataTransformer()
        result = transformer.deduplicate(df)

        assert result.count() == 4

    def test_filter_rows(self, spark):
        """Test filtering rows in Spark."""
        data = [(1, 25), (2, 30), (3, 35), (4, 40)]
        df = spark.createDataFrame(data, ["id", "age"])

        transformer = SparkDataTransformer()
        result = transformer.filter_rows(df, "age >= 30")

        assert result.count() == 3
        ages = [row.age for row in result.collect()]
        assert all(age >= 30 for age in ages)

    def test_cast_column(self, spark):
        """Test casting column type in Spark."""
        data = [("1", 10.5), ("2", 20.8), ("3", 30.2)]
        df = spark.createDataFrame(data, ["id", "value"])

        transformer = SparkDataTransformer()
        result = transformer.cast_column(df, "id", "int")

        # Verify the column was cast
        assert result.schema["id"].dataType.typeName() == "integer"

    def test_add_computed_column(self, spark):
        """Test adding computed column in Spark."""
        from pyspark.sql.functions import col

        data = [(1, 10), (2, 20), (3, 30)]
        df = spark.createDataFrame(data, ["id", "value"])

        transformer = SparkDataTransformer()
        result = transformer.add_computed_column(df, "value_doubled", lambda d: col("value") * 2)

        assert "value_doubled" in result.columns
        values = [row.value_doubled for row in result.collect()]
        assert values == [20, 40, 60]


class TestSparkDataTransformerFactory:
    """Test factory with Spark DataFrames."""

    def test_create_spark_transformer(self):
        """Test creating Spark transformer explicitly."""
        transformer = DataTransformerFactory.create(DataEngine.SPARK)

        assert isinstance(transformer, SparkDataTransformer)

    def test_create_auto_spark(self, spark):
        """Test auto-detection of Spark engine."""
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        transformer = DataTransformerFactory.create_auto(df)

        assert isinstance(transformer, SparkDataTransformer)


# ============================================================================
# Spark Change Data Capture Tests
# ============================================================================


class TestSparkChangeDetector:
    """Test SparkChangeDetector implementation."""

    def test_detect_inserts(self, spark):
        """Test detecting inserted records in Spark."""
        old_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        new_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]

        old_df = spark.createDataFrame(old_data, ["id", "name"])
        new_df = spark.createDataFrame(new_data, ["id", "name"])

        detector = SparkChangeDetector(key_columns=["id"])
        inserts = detector.get_inserts(old_df, new_df)

        assert inserts.count() == 1
        assert inserts.collect()[0].id == 4

    def test_detect_deletes(self, spark):
        """Test detecting deleted records in Spark."""
        old_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        new_data = [(1, "Alice"), (2, "Bob")]

        old_df = spark.createDataFrame(old_data, ["id", "name"])
        new_df = spark.createDataFrame(new_data, ["id", "name"])

        detector = SparkChangeDetector(key_columns=["id"])
        deletes = detector.get_deletes(old_df, new_df)

        assert deletes.count() == 1
        assert deletes.collect()[0].id == 3

    def test_detect_updates(self, spark):
        """Test detecting updated records in Spark."""
        old_data = [(1, "Alice", 85), (2, "Bob", 90), (3, "Charlie", 75)]
        new_data = [(1, "Alice", 85), (2, "Bob Smith", 90), (3, "Charlie", 80)]

        old_df = spark.createDataFrame(old_data, ["id", "name", "score"])
        new_df = spark.createDataFrame(new_data, ["id", "name", "score"])

        detector = SparkChangeDetector(key_columns=["id"])
        updates = detector.get_updates(old_df, new_df)

        # Bob's name and Charlie's score changed
        assert updates.count() == 2

    def test_detect_all_changes(self, spark):
        """Test detecting all types of changes in Spark."""
        old_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
        new_data = [(1, "Alice"), (2, "Bob Updated"), (3, "Charlie"), (5, "Eve")]

        old_df = spark.createDataFrame(old_data, ["id", "name"])
        new_df = spark.createDataFrame(new_data, ["id", "name"])

        detector = SparkChangeDetector(key_columns=["id"])
        changes = detector.detect_changes(old_df, new_df)

        assert changes["summary"]["inserts_count"] == 1  # Eve
        assert changes["summary"]["deletes_count"] == 1  # David
        assert changes["summary"]["updates_count"] == 1  # Bob
        assert changes["summary"]["unchanged_count"] == 2  # Alice, Charlie


class TestSparkChangeDetectorFactory:
    """Test factory with Spark DataFrames."""

    def test_create_spark_detector(self):
        """Test creating Spark detector explicitly."""
        detector = ChangeDetectorFactory.create(DataEngine.SPARK, key_columns=["id"])

        assert isinstance(detector, SparkChangeDetector)

    def test_create_auto_spark(self, spark):
        """Test auto-detection of Spark engine."""
        df = spark.createDataFrame([(1, 2)], ["id", "value"])
        detector = ChangeDetectorFactory.create_auto(df, key_columns=["id"])

        assert isinstance(detector, SparkChangeDetector)


# ============================================================================
# Spark Lineage Tracking Tests
# ============================================================================


class TestSparkLineageTracker:
    """Test SparkLineageTracker implementation."""

    def test_track_transformation(self, spark):
        """Test tracking transformation in Spark."""
        data_in = [(1, 10), (2, 20), (3, 30)]
        data_out = [(1, 20), (2, 40), (3, 60)]

        df_in = spark.createDataFrame(data_in, ["id", "value"])
        df_out = spark.createDataFrame(data_out, ["id", "value"])

        tracker = SparkLineageTracker()
        record = tracker.track_transformation(
            job_name="double_values",
            input_sources=["raw_data"],
            output_targets=["processed_data"],
            transformation="Double all values",
            data_in=df_in,
            data_out=df_out,
        )

        assert record.job_name == "double_values"
        assert record.row_count_in == 3
        assert record.row_count_out == 3

    def test_track_with_metadata(self, spark):
        """Test tracking with metadata in Spark."""
        df = spark.createDataFrame([(1, 2)], ["a", "b"])

        tracker = SparkLineageTracker()
        metadata = {"version": "1.0", "env": "test"}

        record = tracker.track_transformation(
            job_name="test_job",
            input_sources=["input"],
            output_targets=["output"],
            transformation="Test",
            metadata=metadata,
        )

        assert record.metadata == metadata

    def test_get_lineage_records(self, spark):
        """Test retrieving lineage records."""
        tracker = SparkLineageTracker()

        tracker.track_transformation(
            job_name="job1", input_sources=["a"], output_targets=["b"], transformation="T1"
        )

        tracker.track_transformation(
            job_name="job2", input_sources=["c"], output_targets=["d"], transformation="T2"
        )

        records = tracker.get_lineage_records()

        assert len(records) == 2


class TestSparkLineageTrackerFactory:
    """Test factory with Spark DataFrames."""

    def test_create_spark_tracker(self):
        """Test creating Spark tracker explicitly."""
        tracker = LineageTrackerFactory.create(DataEngine.SPARK)

        assert isinstance(tracker, SparkLineageTracker)

    def test_create_auto_spark(self, spark):
        """Test auto-detection of Spark engine."""
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        tracker = LineageTrackerFactory.create_auto(df)

        assert isinstance(tracker, SparkLineageTracker)


# ============================================================================
# Spark Integration Tests
# ============================================================================


class TestSparkIntegration:
    """Integration tests for Spark workflows."""

    def test_complete_etl_workflow(self, spark):
        """Test complete ETL workflow with Spark."""
        # Create source data
        data = [(1, "Alice", 25, 85), (2, None, 30, 90), (3, "Charlie", None, 75)]
        df = spark.createDataFrame(data, ["id", "name", "age", "score"])

        # Profile initial quality
        profiler = DataProfilerFactory.create(DataEngine.SPARK)
        initial_report = profiler.profile(df)

        assert initial_report.null_counts["name"] == 1
        assert initial_report.null_counts["age"] == 1

        # Transform
        transformer = DataTransformerFactory.create(DataEngine.SPARK)
        clean_df = transformer.fill_nulls(df, strategy="value", value="Unknown", columns=["name"])
        clean_df = transformer.fill_nulls(clean_df, strategy="mean", columns=["age"])
        clean_df = transformer.drop_nulls(clean_df, subset=["id"])

        # Track lineage
        tracker = LineageTrackerFactory.create(DataEngine.SPARK)
        tracker.track_transformation(
            job_name="clean_data",
            input_sources=["raw_data"],
            output_targets=["clean_data"],
            transformation="Fill nulls and clean",
            data_in=df,
            data_out=clean_df,
        )

        # Verify improvements
        final_report = profiler.profile(clean_df)
        assert final_report.null_counts["name"] == 0
        assert final_report.row_count == 3

        # Check lineage
        records = tracker.get_lineage_records()
        assert len(records) == 1
        assert records[0].row_count_in == 3

    def test_cdc_workflow(self, spark):
        """Test CDC workflow with Spark."""
        # Yesterday's data
        old_data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 150)]
        old_df = spark.createDataFrame(old_data, ["id", "name", "amount"])

        # Today's data
        new_data = [(1, "Alice", 100), (2, "Bob", 250), (4, "David", 300)]
        new_df = spark.createDataFrame(new_data, ["id", "name", "amount"])

        # Detect changes
        detector = ChangeDetectorFactory.create(DataEngine.SPARK, key_columns=["id"])
        changes = detector.detect_changes(old_df, new_df)

        # Verify
        assert changes["summary"]["inserts_count"] == 1  # David added
        assert changes["summary"]["updates_count"] == 1  # Bob's amount changed
        assert changes["summary"]["deletes_count"] == 1  # Charlie removed
        assert changes["summary"]["unchanged_count"] == 1  # Alice unchanged


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
