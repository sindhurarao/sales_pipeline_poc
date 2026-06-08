import pytest
from helpers.metadata_helper import MetadataHelper

@pytest.mark.parametrize(
    "metadata_config,expected_columns",
    [
        ({"run_id": True}, {"run_id"}),
        ({"load_date": True}, {"load_date"}),
        ({"_rescued_data": True}, {"_rescued_data"}),
        (
                {
                    "ingestion_timestamp": True,
                    "source_file_name": True,
                    "load_date": True,
                    "run_id": True,
                    "_rescued_data": True,
                },
                {
                    "ingestion_timestamp",
                    "source_file_name",
                    "load_date",
                    "run_id",
                    "_rescued_data",
                },
        ),
    ],
)
def test_metadata_enrich_adds_expected_columns(sample_df, metadata_config, expected_columns):
    result = MetadataHelper.enrich(sample_df, metadata_config, "test_run")
    assert expected_columns.issubset(set(result.columns))


def test_metadata_enrich_sets_run_id(sample_df):
    result = MetadataHelper.enrich(sample_df, {"run_id": True}, "test_run")
    assert result.select("run_id").first()["run_id"] == "test_run"