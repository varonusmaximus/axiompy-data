"""Pandas and Spark change detectors."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from axiompy.data.observability.ports import SignalKind, SignalSink
from axiompy.data.processing.cdc import ChangeDetector, logger
from axiompy.data.processing.signals import emit_signal
from axiompy.data.types import DataEngine


class PandasChangeDetector(ChangeDetector):
    """Change detector for Pandas DataFrames."""

    def __init__(
        self,
        key_columns: List[str],
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(DataEngine.PANDAS, key_columns, settings, signal_sink)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError as err:
            raise ImportError("Pandas is required. Install with: pip install pandas") from err

    def detect_changes(
        self,
        old_data: pd.DataFrame,
        new_data: pd.DataFrame,
        compare_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:  # pragma: no cover
        """Detect changes between Pandas DataFrames."""
        logger.info("Detecting changes between datasets")

        inserts = self.get_inserts(old_data, new_data)
        deletes = self.get_deletes(old_data, new_data)
        updates = self.get_updates(old_data, new_data, compare_columns)

        # Get unchanged (records that exist in both and haven't changed)
        old_keys = set(map(tuple, old_data[self.key_columns].values))
        new_keys = set(map(tuple, new_data[self.key_columns].values))
        update_keys = (
            set(map(tuple, updates[self.key_columns].values)) if len(updates) > 0 else set()
        )

        unchanged_keys = (old_keys & new_keys) - update_keys
        unchanged = new_data[new_data[self.key_columns].apply(tuple, axis=1).isin(unchanged_keys)]

        result = {
            "inserts": inserts,
            "updates": updates,
            "deletes": deletes,
            "unchanged": unchanged,
            "summary": {
                "inserts_count": len(inserts),
                "updates_count": len(updates),
                "deletes_count": len(deletes),
                "unchanged_count": len(unchanged),
            },
        }

        logger.info(
            f"Changes detected: {len(inserts)} inserts, {len(updates)} updates, "
            f"{len(deletes)} deletes, {len(unchanged)} unchanged"
        )

        emit_signal(
            self._signal_sink,
            SignalKind.CDC,
            "cdc.detect_changes",
            {
                "engine": "pandas",
                "inserts": len(inserts),
                "updates": len(updates),
                "deletes": len(deletes),
            },
        )

        return result

    def get_inserts(self, old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
        """Get inserted records."""
        old_keys = set(map(tuple, old_data[self.key_columns].values))
        new_keys_df = new_data[self.key_columns].apply(tuple, axis=1)

        inserts = new_data[~new_keys_df.isin(old_keys)]
        logger.debug(f"Found {len(inserts)} inserts")
        return inserts

    def get_deletes(self, old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
        """Get deleted records."""
        new_keys = set(map(tuple, new_data[self.key_columns].values))
        old_keys_df = old_data[self.key_columns].apply(tuple, axis=1)

        deletes = old_data[~old_keys_df.isin(new_keys)]
        logger.debug(f"Found {len(deletes)} deletes")
        return deletes

    def get_updates(
        self,
        old_data: pd.DataFrame,
        new_data: pd.DataFrame,
        compare_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Get updated records."""
        # Merge on keys
        merged = old_data.merge(
            new_data, on=self.key_columns, how="inner", suffixes=("_old", "_new")
        )

        if len(merged) == 0:
            return self.pd.DataFrame(columns=new_data.columns)

        # Determine columns to compare
        if compare_columns:
            cols_to_compare = [c for c in compare_columns if c not in self.key_columns]
        else:
            # Compare all columns except keys
            cols_to_compare = [c for c in old_data.columns if c not in self.key_columns]

        # Find rows where at least one value changed
        changed_mask = self.pd.Series([False] * len(merged))
        for col in cols_to_compare:
            old_col = f"{col}_old"
            new_col = f"{col}_new"

            if old_col in merged.columns and new_col in merged.columns:
                changed_mask |= merged[old_col] != merged[new_col]

        updates = merged[changed_mask]

        # Return only new values with key columns
        update_keys = updates[self.key_columns]
        result = new_data.merge(update_keys, on=self.key_columns, how="inner")

        logger.debug(f"Found {len(result)} updates")
        return result


class SparkChangeDetector(ChangeDetector):
    """Change detector for Spark DataFrames."""

    def __init__(
        self,
        key_columns: List[str],
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(DataEngine.SPARK, key_columns, settings, signal_sink)
        try:
            from pyspark.sql import DataFrame
            from pyspark.sql import functions as Func

            self.DataFrame = DataFrame
            self.F = Func
        except ImportError as err:
            raise ImportError("PySpark is required. Install with: pip install pyspark") from err

    def detect_changes(
        self,
        old_data: DataFrame,
        new_data: DataFrame,
        compare_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Detect changes between Spark DataFrames."""
        logger.info("Detecting changes between Spark datasets")

        inserts = self.get_inserts(old_data, new_data)
        deletes = self.get_deletes(old_data, new_data)
        updates = self.get_updates(old_data, new_data, compare_columns)

        # Unchanged: in both and not in updates
        both = old_data.join(new_data, on=self.key_columns, how="inner")
        update_keys = updates.select(self.key_columns).distinct() if updates.count() > 0 else None

        if update_keys:
            unchanged = both.join(update_keys, on=self.key_columns, how="left_anti")
        else:
            unchanged = both

        result = {
            "inserts": inserts,
            "updates": updates,
            "deletes": deletes,
            "unchanged": unchanged,
            "summary": {
                "inserts_count": inserts.count(),
                "updates_count": updates.count(),
                "deletes_count": deletes.count(),
                "unchanged_count": unchanged.count(),
            },
        }

        logger.info(
            f"Changes detected: {result['summary']['inserts_count']} inserts, "
            f"{result['summary']['updates_count']} updates, "
            f"{result['summary']['deletes_count']} deletes"
        )

        emit_signal(
            self._signal_sink,
            SignalKind.CDC,
            "cdc.detect_changes",
            {
                "engine": "spark",
                "inserts": result["summary"]["inserts_count"],
                "updates": result["summary"]["updates_count"],
                "deletes": result["summary"]["deletes_count"],
            },
        )

        return result

    def get_inserts(self, old_data: DataFrame, new_data: DataFrame) -> DataFrame:
        """Get inserted records using left anti join."""
        inserts = new_data.join(old_data, on=self.key_columns, how="left_anti")
        logger.debug("Found inserts")
        return inserts

    def get_deletes(self, old_data: DataFrame, new_data: DataFrame) -> DataFrame:
        """Get deleted records using left anti join."""
        deletes = old_data.join(new_data, on=self.key_columns, how="left_anti")
        logger.debug("Found deletes")
        return deletes

    def get_updates(
        self,
        old_data: DataFrame,
        new_data: DataFrame,
        compare_columns: Optional[List[str]] = None,
    ) -> DataFrame:
        """Get updated records."""
        # Join on keys
        joined = old_data.alias("old").join(new_data.alias("new"), on=self.key_columns, how="inner")

        # Determine columns to compare
        if compare_columns:
            cols_to_compare = [c for c in compare_columns if c not in self.key_columns]
        else:
            cols_to_compare = [c for c in old_data.columns if c not in self.key_columns]

        # Build condition for changed rows
        change_conditions = []
        for col in cols_to_compare:
            change_conditions.append(self.F.col(f"old.{col}") != self.F.col(f"new.{col}"))

        if not change_conditions:
            # No columns to compare
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            return spark.createDataFrame([], schema=new_data.schema)

        # Combine conditions with OR
        from functools import reduce

        change_condition = reduce(lambda a, b: a | b, change_conditions)

        # Filter to changed rows and select new values
        updates = joined.filter(change_condition).select(
            *[self.F.col(f"new.{c}").alias(c) for c in new_data.columns]
        )

        logger.debug("Found updates")
        return updates
