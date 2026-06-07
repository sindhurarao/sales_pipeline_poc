import pytest
from pyspark.sql.types import StringType, TimestampType, DateType
from helpers import MetadataHelper

@pytest.mark.parametrize(
    "config,expected_columns",
    [
        ({"ingestion_timestamp": True}, ["id", "name", "ingestion_timestamp"]),
        ({"source_file_name": True}, ["id", "name", "source_file_name"]),
        ({"load_date": True}, ["id", "name", "load_date"]),
        ({"run_id": True}, ["id", "name", "run_id"]),
        ({"_rescued_data": True}, ["id", "name", "_rescued_data"]),
    ],
)

@pytest.fixture(scope="class")
def c(spark):
    return spark.createDataFrame(
        [
            ("test_id", "test_name")
        ],
        ["id", "name"],
    )


def test_enrich_adds_all_metadata_columns(spark):

    result = MetadataHelper.enrich(
        df=sample_df,
        metadata_config=config,
        run_id="test_run",
    )

    assert set(result.columns) == expected_columns

    schema = {field.name: field.dataType for field in result.schema.fields}

    assert isinstance(schema["ingestion_timestamp"], TimestampType)
    assert isinstance(schema["load_date"], DateType)
    assert isinstance(schema["run_id"], StringType)
    assert isinstance(schema["_rescued_data"], StringType)

    row = result.collect()[0]

    assert row.run_id == "test_run"
    assert row._rescued_data is None
    assert row.ingestion_timestamp is not None
    assert row.load_date is not None


def test_enrich_adds_only_requested_columns(spark):

    result = MetadataHelper.enrich(
        df=sample_df,
        metadata_config=config,
        run_id="test_run",
    )

    assert result.columns == ["id", "name", "run_id"]

    row = result.collect()[0]
    assert row.run_id == "test_run"


def test_enrich_with_empty_config_returns_original_schema(spark):

    result = MetadataHelper.enrich(
        df=sample_df,
        metadata_config={},
        run_id="test_run",
    )

    assert result.columns == ["id", "name"]


def test_enrich_adds_individual_metadata_column(spark,config,column_name,):

    result = MetadataHelper.enrich(
        df=sample_df,
        metadata_config=config,
        run_id="test_run",
    )

    assert column_name in result.columns