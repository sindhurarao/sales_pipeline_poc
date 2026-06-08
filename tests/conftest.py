import os
import sys
import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession

# To import modules from src package, setting path to .../sales_pipeline_poc/src
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_PATH)

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("sales-pipeline-unit-tests")
        .getOrCreate()
    )
    yield session
    session.stop()

@pytest.fixture(scope="function")
def sample_df(spark):
    return spark.createDataFrame(
        [
            (1, " Test ", 10.126, "2026-06-07"),
            (2, None, 5.555, "2026-06-08"),
            (3, "", -1.0, "2026-06-06"),
        ],
        ["id", "name", "amount", "event_date"],
    )

@pytest.fixture(scope="function")
def mock_spark():
    return MagicMock()

@pytest.fixture(scope="function")
def mock_writer_chain():
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    return writer