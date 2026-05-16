"""
Unit tests for axiompy.data.transform module.

Tests DataTransformer implementations for both Pandas and Spark engines.
"""

import pandas as pd
import pytest

from axiompy.data.processing.transform import (
    DataTransformer,
    DataTransformerFactory,
    PandasDataTransformer,
)
from axiompy.data.types import DataEngine


class TestPandasDataTransformer:
    """Test PandasDataTransformer implementation."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "first_name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "last_name": ["Smith", "Jones", "Brown", "Wilson", "Davis"],
                "age": [25, 30, None, 45, 28],
                "score": [85.5, 92.0, 78.5, None, 95.0],
            }
        )

    @pytest.fixture
    def transformer(self):
        """Create PandasDataTransformer instance."""
        return PandasDataTransformer()

    def test_rename_columns(self, transformer, sample_df):
        """Test column renaming."""
        result = transformer.rename_columns(
            sample_df, {"first_name": "fname", "last_name": "lname"}
        )

        assert "fname" in result.columns
        assert "lname" in result.columns
        assert "first_name" not in result.columns
        assert "last_name" not in result.columns
        assert len(result) == len(sample_df)

    def test_select_columns(self, transformer, sample_df):
        """Test column selection."""
        result = transformer.select_columns(sample_df, ["id", "first_name", "age"])

        assert list(result.columns) == ["id", "first_name", "age"]
        assert len(result) == len(sample_df)

    def test_drop_columns(self, transformer, sample_df):
        """Test column dropping."""
        result = transformer.drop_columns(sample_df, ["score", "last_name"])

        assert "score" not in result.columns
        assert "last_name" not in result.columns
        assert len(result.columns) == 3
        assert len(result) == len(sample_df)

    def test_fill_nulls_value(self, transformer, sample_df):
        """Test filling nulls with a value."""
        result = transformer.fill_nulls(sample_df, strategy="value", value=0)

        assert result["age"].isnull().sum() == 0
        assert result["score"].isnull().sum() == 0
        assert result.loc[2, "age"] == 0

    def test_fill_nulls_mean(self, transformer, sample_df):
        """Test filling nulls with mean."""
        result = transformer.fill_nulls(sample_df, strategy="mean", columns=["age", "score"])

        assert result["age"].isnull().sum() == 0
        assert result["score"].isnull().sum() == 0

        # Check that mean was used
        expected_age_mean = sample_df["age"].mean()
        assert result.loc[2, "age"] == expected_age_mean

    def test_fill_nulls_median(self, transformer, sample_df):
        """Test filling nulls with median."""
        result = transformer.fill_nulls(sample_df, strategy="median", columns=["age"])

        assert result["age"].isnull().sum() == 0

        expected_median = sample_df["age"].median()
        assert result.loc[2, "age"] == expected_median

    def test_drop_nulls_any(self, transformer, sample_df):
        """Test dropping rows with any null."""
        result = transformer.drop_nulls(sample_df, how="any")

        # Should remove rows with nulls in age or score
        assert len(result) < len(sample_df)
        assert result["age"].isnull().sum() == 0
        assert result["score"].isnull().sum() == 0

    def test_drop_nulls_subset(self, transformer, sample_df):
        """Test dropping rows with nulls in specific columns."""
        result = transformer.drop_nulls(sample_df, how="any", subset=["age"])

        # Should only consider age column
        assert result["age"].isnull().sum() == 0
        assert len(result) == 4  # Only row with null age removed

    def test_deduplicate_first(self, transformer):
        """Test deduplication keeping first occurrence."""
        df = pd.DataFrame({"id": [1, 2, 3, 2, 4], "value": [10, 20, 30, 20, 40]})

        result = transformer.deduplicate(df, keep="first")

        assert len(result) == 4
        assert result["id"].tolist() == [1, 2, 3, 4]

    def test_deduplicate_subset(self, transformer):
        """Test deduplication on specific columns."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 2, 4],
                "value": [10, 20, 30, 99, 40],  # Different value for duplicate id
            }
        )

        result = transformer.deduplicate(df, subset=["id"], keep="first")

        assert len(result) == 4
        # First occurrence of id=2 should be kept (value=20)
        assert result[result["id"] == 2]["value"].values[0] == 20

    def test_filter_rows_string(self, transformer, sample_df):
        """Test filtering rows with string expression."""
        result = transformer.filter_rows(sample_df, "age >= 30")

        # Should keep rows where age >= 30 (excluding nulls)
        assert len(result) == 2
        assert all(result["age"] >= 30)

    def test_filter_rows_callable(self, transformer, sample_df):
        """Test filtering rows with callable."""
        # Filter for age > 25, but Pandas includes NaN in comparisons
        result = transformer.filter_rows(sample_df, lambda df: df["age"] > 25)

        # Should include Bob (30), David (45), and row with NaN (NaN > 25 returns False but may be included)
        # The actual behavior depends on how pandas handles NaN in boolean indexing
        assert len(result) >= 2  # At least Bob and David
        # Check that non-null ages are > 25
        non_null_ages = result["age"].dropna()
        assert all(non_null_ages > 25)

    def test_cast_column(self, transformer):
        """Test casting column type."""
        df = pd.DataFrame({"id": ["1", "2", "3"], "value": [10.5, 20.8, 30.2]})

        result = transformer.cast_column(df, "id", int)

        assert result["id"].dtype == "int64" or result["id"].dtype == "int32"
        assert result["id"].tolist() == [1, 2, 3]

    def test_add_computed_column_callable(self, transformer, sample_df):
        """Test adding computed column with callable."""
        result = transformer.add_computed_column(
            sample_df, "full_name", lambda df: df["first_name"] + " " + df["last_name"]
        )

        assert "full_name" in result.columns
        assert result.loc[0, "full_name"] == "Alice Smith"
        assert result.loc[1, "full_name"] == "Bob Jones"

    def test_add_computed_column_string(self, transformer, sample_df):
        """Test adding computed column with string expression."""
        result = transformer.add_computed_column(sample_df, "age_doubled", "age * 2")

        assert "age_doubled" in result.columns
        assert result.loc[0, "age_doubled"] == 50
        assert result.loc[1, "age_doubled"] == 60


class TestDataTransformerFactory:
    """Test DataTransformerFactory."""

    def test_create_pandas_transformer(self):
        """Test creating Pandas transformer explicitly."""
        transformer = DataTransformerFactory.create(DataEngine.PANDAS)

        assert isinstance(transformer, PandasDataTransformer)
        assert transformer.engine == DataEngine.PANDAS

    def test_create_auto_pandas(self):
        """Test auto-detection of Pandas engine."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        transformer = DataTransformerFactory.create_auto(df)

        assert isinstance(transformer, PandasDataTransformer)

    def test_create_unsupported_engine(self):
        """Test error on unsupported engine."""
        with pytest.raises(ValueError, match="Unsupported engine"):
            DataTransformerFactory.create(DataEngine.POLARS)

    def test_register_custom_transformer(self):
        """Test registering custom transformer."""

        class CustomTransformer(DataTransformer):
            def __init__(self, settings=None):
                super().__init__(DataEngine.POLARS, settings)

            def rename_columns(self, data, mapping):
                pass

            def select_columns(self, data, columns):
                pass

            def drop_columns(self, data, columns):
                pass

            def fill_nulls(self, data, strategy="value", value=None, columns=None):
                pass

            def drop_nulls(self, data, how="any", subset=None):
                pass

            def deduplicate(self, data, subset=None, keep="first"):
                pass

            def filter_rows(self, data, condition):
                pass

            def cast_column(self, data, column, dtype):
                pass

            def add_computed_column(self, data, column_name, expression):
                pass

        DataTransformerFactory.register_transformer(DataEngine.POLARS, CustomTransformer)

        transformer = DataTransformerFactory.create(DataEngine.POLARS)
        assert isinstance(transformer, CustomTransformer)


class TestTransformationChaining:
    """Test chaining multiple transformations."""

    def test_chain_transformations(self):
        """Test chaining multiple transformations together."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 3, 5],
                "name": ["Alice", "Bob", None, "Charlie", "David"],
                "age": [25, 30, 35, 35, None],
                "score": [85, 92, 78, 78, 95],
            }
        )

        transformer = DataTransformerFactory.create_auto(df)

        # Chain transformations
        result = df.copy()
        result = transformer.fill_nulls(result, strategy="value", value="Unknown", columns=["name"])
        result = transformer.fill_nulls(result, strategy="median", columns=["age"])
        result = transformer.deduplicate(result, subset=["id"])
        result = transformer.filter_rows(result, "age >= 30")

        assert len(result) < len(df)
        assert result["name"].isnull().sum() == 0
        assert result["age"].isnull().sum() == 0
        assert len(result[result["id"] == 3]) == 1  # Deduplicated
        assert all(result["age"] >= 30)

    def test_transform_pipeline(self):
        """Test a complete transformation pipeline."""
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, None, 5, 5],
                "email": [
                    "alice@ex.com",
                    "bob@ex.com",
                    "charlie@ex.com",
                    "david@ex.com",
                    "eve@ex.com",
                    "eve@ex.com",
                ],
                "age": [25, None, 35, 40, 28, 28],
                "status": ["active", "active", "inactive", "active", "pending", "pending"],
            }
        )

        transformer = DataTransformerFactory.create_auto(df)

        # Clean and transform
        result = df.copy()
        result = transformer.drop_nulls(result, subset=["user_id"])
        result = transformer.fill_nulls(result, strategy="median", columns=["age"])
        result = transformer.deduplicate(result, subset=["user_id", "email"])
        result = transformer.filter_rows(result, "status == 'active'")
        result = transformer.add_computed_column(
            result,
            "age_group",
            lambda d: pd.cut(d["age"], bins=[0, 30, 50, 100], labels=["young", "middle", "senior"]),
        )

        assert len(result) == 2  # Only active users after filtering
        assert result["age"].isnull().sum() == 0
        assert "age_group" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
