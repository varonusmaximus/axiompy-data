"""
Pytest configuration and fixtures for axiompy tests.

Sets up common configuration needed across test modules.
"""

import os
import sys


def pytest_configure(config):
    """
    Configure pytest environment before tests run.

    Sets PYSPARK_PYTHON to fix Python version mismatch errors
    between Spark driver and worker processes.
    """
    # Fix PySpark Python version mismatch
    # Without this, Spark workers may use a different Python than the driver
    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
