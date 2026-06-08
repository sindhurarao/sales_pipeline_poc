import pytest
from unittest.mock import MagicMock
import ingestion.bronze_to_silver as bronze_to_silver

@pytest.fixture
def table_ingestion_config():
    return {
        "ingestion_type": "table",
        "source": {"table": "bronze_customers"},
    }

@pytest.fixture
def path_ingestion_config():
    return {
        "ingestion_type": "path",
        "source": {
            "path": "/mnt/bronze/customers",
            "format": "delta",
            "options": {"mergeSchema": "true"},
        },
    }

class DummyBronzeToSilverJob:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

def test_read_source_from_table(mock_spark, table_ingestion_config):
    job = DummyBronzeToSilverJob(mock_spark, table_ingestion_config)
    bronze_to_silver.read_source(job)
    mock_spark.table.assert_called_once_with("bronze_customers")

def test_read_source_from_path(mock_spark, path_ingestion_config):
    reader = mock_spark.read
    reader.option.return_value = reader
    reader.format.return_value = reader
    job = DummyBronzeToSilverJob(mock_spark, path_ingestion_config)
    bronze_to_silver.read_source(job)
    reader.option.assert_called_once_with("mergeSchema", "true")
    reader.format.assert_called_once_with("delta")
    reader.load.assert_called_once_with("/mnt/bronze/customers")


def test_read_source_raises_for_invalid_ingestion_type(mock_spark):
    job = DummyBronzeToSilverJob(
        mock_spark,
        {"ingestion_type": "bad", "source": {}},
    )
    with pytest.raises(ValueError, match="Unsupported ingestion_type"):
        bronze_to_silver.read_source(job)