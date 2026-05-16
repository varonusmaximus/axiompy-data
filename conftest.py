"""Root pytest configuration: Spark tests need a real JRE (macOS ``java`` stub is not enough)."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest


def _java_candidates():
    """Prefer JAVA_HOME and common Homebrew / Linux layouts before ``/usr/bin/java`` (macOS stub)."""
    jh = os.environ.get("JAVA_HOME")
    if jh:
        yield Path(jh) / "bin" / "java"
    for rel in (
        Path("/opt/homebrew/opt/openjdk@21/bin/java"),
        Path("/opt/homebrew/opt/openjdk/bin/java"),
        Path("/usr/lib/jvm/temurin-21-jdk/bin/java"),
        Path("/usr/lib/jvm/default-java/bin/java"),
    ):
        yield rel
    which = shutil.which("java")
    if which:
        yield Path(which)


def _java_version_ok(java: Path) -> bool:
    try:
        r = subprocess.run(
            [str(java), "-version"],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except OSError:
        return False
    return r.returncode == 0


def _init_java_for_tests() -> bool:
    """Pick a working ``java`` binary and set ``JAVA_HOME`` for PySpark (import-time)."""
    for java in _java_candidates():
        if not java.is_file():
            continue
        if not _java_version_ok(java):
            continue
        jdk_home = java.resolve().parent.parent
        os.environ.setdefault("JAVA_HOME", str(jdk_home))
        return True
    return False


_SPARK_RUNTIME_AVAILABLE = _init_java_for_tests()


def _spark_runtime_available() -> bool:
    return _SPARK_RUNTIME_AVAILABLE


# Module-level: skip collecting test_data_spark entirely when pyspark would start at import time.
collect_ignore = [] if _spark_runtime_available() else ["tests/test_data_spark.py"]


def pytest_collection_modifyitems(config, items):
    """Skip remaining PySpark tests when no usable JRE."""
    if _spark_runtime_available():
        return
    skip_spark = pytest.mark.skip(reason="Java runtime not usable (required for PySpark)")
    for item in items:
        path = str(item.fspath).lower()
        nodeid = item.nodeid
        tail = nodeid.split("::")[-1].lower() if "::" in nodeid else nodeid.lower()
        if "spark" in path or "testspark" in nodeid or "::test_spark" in nodeid or "spark" in tail:
            item.add_marker(skip_spark)
