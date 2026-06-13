"""Pytest fixtures for PySpark unit tests."""

import pytest
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, "/opt/spark/jobs")

from utils.spark_session import get_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Single SparkSession shared across all tests in the session."""
    session = get_spark("test-suite", local=True)
    yield session
    session.stop()
