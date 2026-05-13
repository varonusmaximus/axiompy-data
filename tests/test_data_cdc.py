"""
Unit tests for axiompy.data.cdc module.

Tests ChangeDetector implementations for detecting data changes.
"""

import pandas as pd
import pytest

from axiompy.data.cdc import (
    ChangeDetectorFactory,
    PandasChangeDetector,
)
from axiompy.data.types import DataEngine


class TestPandasChangeDetector:
    """Test PandasChangeDetector implementation."""

    @pytest.fixture
    def old_df(self):
        """Create old/baseline DataFrame."""
        return pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "score": [85, 90, 75, 88, 92],
                "status": ["active", "active", "inactive", "active", "pending"],
            }
        )

    @pytest.fixture
    def new_df(self):
        """Create new/current DataFrame with changes."""
        return pd.DataFrame(
            {
                "user_id": [1, 2, 3, 5, 6],  # 4 deleted, 6 added
                "name": ["Alice", "Bob Smith", "Charlie", "Eve", "Frank"],  # Bob's name changed
                "score": [85, 90, 80, 92, 78],  # Charlie's score changed
                "status": ["active", "active", "inactive", "active", "active"],
            }
        )

    @pytest.fixture
    def detector(self):
        """Create PandasChangeDetector instance."""
        return PandasChangeDetector(key_columns=["user_id"])

    def test_detect_changes_inserts(self, detector, old_df, new_df):
        """Test detecting inserted records."""
        inserts = detector.get_inserts(old_df, new_df)

        assert len(inserts) == 1
        assert inserts["user_id"].values[0] == 6
        assert inserts["name"].values[0] == "Frank"

    def test_detect_changes_deletes(self, detector, old_df, new_df):
        """Test detecting deleted records."""
        deletes = detector.get_deletes(old_df, new_df)

        assert len(deletes) == 1
        assert deletes["user_id"].values[0] == 4
        assert deletes["name"].values[0] == "David"

    def test_detect_changes_updates(self, detector, old_df, new_df):
        """Test detecting updated records."""
        updates = detector.get_updates(old_df, new_df)

        # Bob (user_id=2), Charlie (user_id=3), and Eve (user_id=5) changed
        # Eve's status field is different even though we didn't change it in the fixture
        assert len(updates) >= 2  # At least Bob and Charlie
        assert 2 in updates["user_id"].values  # Bob changed
        assert 3 in updates["user_id"].values  # Charlie changed

    def test_detect_all_changes(self, detector, old_df, new_df):
        """Test detecting all types of changes at once."""
        changes = detector.detect_changes(old_df, new_df)

        assert changes["summary"]["inserts_count"] == 1
        assert changes["summary"]["updates_count"] == 3  # Bob, Charlie, and Eve (status changed)
        assert changes["summary"]["deletes_count"] == 1
        assert changes["summary"]["unchanged_count"] == 1  # Only Alice unchanged

        # Verify change sets
        assert len(changes["inserts"]) == 1
        assert len(changes["updates"]) == 3
        assert len(changes["deletes"]) == 1
        assert len(changes["unchanged"]) == 1

    def test_detect_changes_no_changes(self, detector, old_df):
        """Test detecting changes when data is identical."""
        changes = detector.detect_changes(old_df, old_df)

        assert changes["summary"]["inserts_count"] == 0
        assert changes["summary"]["updates_count"] == 0
        assert changes["summary"]["deletes_count"] == 0
        assert changes["summary"]["unchanged_count"] == 5

    def test_detect_changes_all_new(self, detector, old_df, new_df):
        """Test when all records are new."""
        empty_df = pd.DataFrame(columns=old_df.columns)
        changes = detector.detect_changes(empty_df, new_df)

        assert changes["summary"]["inserts_count"] == len(new_df)
        assert changes["summary"]["updates_count"] == 0
        assert changes["summary"]["deletes_count"] == 0
        assert changes["summary"]["unchanged_count"] == 0

    def test_detect_changes_all_deleted(self, detector, old_df):
        """Test when all records are deleted."""
        empty_df = pd.DataFrame(columns=old_df.columns)
        changes = detector.detect_changes(old_df, empty_df)

        assert changes["summary"]["inserts_count"] == 0
        assert changes["summary"]["updates_count"] == 0
        assert changes["summary"]["deletes_count"] == len(old_df)
        assert changes["summary"]["unchanged_count"] == 0

    def test_detect_updates_specific_columns(self, detector):
        """Test detecting updates on specific columns only."""
        old = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [85, 90, 75],
                "updated_at": ["2024-01-01", "2024-01-01", "2024-01-01"],
            }
        )

        new = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob Smith", "Charlie"],  # Bob's name changed
                "score": [85, 90, 75],  # Scores unchanged
                "updated_at": ["2024-01-02", "2024-01-02", "2024-01-02"],  # All timestamps changed
            }
        )

        detector = PandasChangeDetector(key_columns=["id"])

        # Only consider name and score columns for updates
        updates = detector.get_updates(old, new, compare_columns=["name", "score"])

        # Should only detect Bob's name change, not timestamp changes
        assert len(updates) == 1
        assert updates["id"].values[0] == 2

    def test_composite_key(self):
        """Test change detection with composite key."""
        old = pd.DataFrame(
            {
                "user_id": [1, 1, 2, 2],
                "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
                "value": [10, 20, 30, 40],
            }
        )

        new = pd.DataFrame(
            {
                "user_id": [1, 1, 2, 2, 3],
                "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02", "2024-01-01"],
                "value": [10, 25, 30, 40, 50],  # (1, 2024-01-02) changed
            }
        )

        detector = PandasChangeDetector(key_columns=["user_id", "date"])

        changes = detector.detect_changes(old, new)

        assert changes["summary"]["inserts_count"] == 1  # New user_id=3
        assert changes["summary"]["updates_count"] == 1  # (1, 2024-01-02) value changed
        assert changes["summary"]["deletes_count"] == 0
        assert changes["summary"]["unchanged_count"] == 3


class TestChangeDetectorFactory:
    """Test ChangeDetectorFactory."""

    def test_create_pandas_detector(self):
        """Test creating Pandas detector explicitly."""
        detector = ChangeDetectorFactory.create(DataEngine.PANDAS, key_columns=["id"])

        assert isinstance(detector, PandasChangeDetector)
        assert detector.engine == DataEngine.PANDAS
        assert detector.key_columns == ["id"]

    def test_create_auto_pandas(self):
        """Test auto-detection of Pandas engine."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        detector = ChangeDetectorFactory.create_auto(df, key_columns=["id"])

        assert isinstance(detector, PandasChangeDetector)

    def test_create_unsupported_engine(self):
        """Test error on unsupported engine."""
        with pytest.raises(ValueError, match="Unsupported engine"):
            ChangeDetectorFactory.create(DataEngine.POLARS, key_columns=["id"])


class TestCDCIntegration:
    """Integration tests for CDC workflows."""

    def test_incremental_sync_workflow(self):
        """Test incremental data sync workflow."""
        # Simulate yesterday's data
        yesterday = pd.DataFrame(
            {
                "product_id": [101, 102, 103, 104],
                "name": ["Widget A", "Widget B", "Widget C", "Widget D"],
                "price": [10.0, 20.0, 30.0, 40.0],
                "inventory": [100, 200, 150, 75],
            }
        )

        # Simulate today's data
        today = pd.DataFrame(
            {
                "product_id": [101, 102, 103, 105],  # 104 discontinued, 105 new
                "name": ["Widget A", "Widget B Plus", "Widget C", "Widget E"],  # 102 renamed
                "price": [10.0, 25.0, 30.0, 15.0],  # 102 price increased
                "inventory": [90, 180, 150, 200],  # Inventory changed
            }
        )

        detector = ChangeDetectorFactory.create_auto(yesterday, key_columns=["product_id"])
        changes = detector.detect_changes(yesterday, today)

        # Verify changes for sync
        inserts = changes["inserts"]
        updates = changes["updates"]
        deletes = changes["deletes"]

        # New product
        assert len(inserts) == 1
        assert inserts["product_id"].values[0] == 105

        # Updated products (name or price changed)
        assert len(updates) >= 1
        assert 102 in updates["product_id"].values

        # Discontinued product
        assert len(deletes) == 1
        assert deletes["product_id"].values[0] == 104

    def test_audit_trail_workflow(self):
        """Test creating audit trail from changes."""
        old = pd.DataFrame({"id": [1, 2, 3], "status": ["draft", "active", "active"]})

        new = pd.DataFrame({"id": [1, 2, 3], "status": ["active", "active", "archived"]})

        detector = ChangeDetectorFactory.create_auto(old, key_columns=["id"])
        updates = detector.get_updates(old, new)

        # Generate audit records
        audit_records = []
        for _, new_row in updates.iterrows():
            id_val = new_row["id"]
            old_row = old[old["id"] == id_val].iloc[0]

            audit_records.append(
                {
                    "id": id_val,
                    "field": "status",
                    "old_value": old_row["status"],
                    "new_value": new_row["status"],
                }
            )

        assert len(audit_records) == 2
        assert audit_records[0]["old_value"] == "draft"
        assert audit_records[0]["new_value"] == "active"
        assert audit_records[1]["old_value"] == "active"
        assert audit_records[1]["new_value"] == "archived"

    def test_data_quality_comparison(self):
        """Test comparing data quality between datasets."""
        old = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, None, 30, 40, None],  # 2 nulls
            }
        )

        new = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": [10, 20, 30, 40, 50],  # No nulls - quality improved
            }
        )

        detector = ChangeDetectorFactory.create_auto(old, key_columns=["id"])
        updates = detector.get_updates(old, new)

        # Records where nulls were filled
        assert len(updates) == 2
        assert set(updates["id"].values) == {2, 5}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
