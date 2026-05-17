"""
Shared factory entry points for ``axiompy.data.processing``.

Domain modules still define behavior; this module aggregates ``*Factory`` classes
for a single import path when desired.
"""

from __future__ import annotations

from axiompy.data.processing.batch import BatchProcessorFactory
from axiompy.data.processing.cdc import ChangeDetectorFactory
from axiompy.data.processing.lineage import LineageTrackerFactory
from axiompy.data.processing.partition import DataPartitionerFactory
from axiompy.data.processing.quality import DataProfilerFactory
from axiompy.data.processing.transform import DataTransformerFactory

__all__ = [
    "BatchProcessorFactory",
    "ChangeDetectorFactory",
    "DataPartitionerFactory",
    "DataProfilerFactory",
    "DataTransformerFactory",
    "LineageTrackerFactory",
]
