import pytest
from helpers.pre_processor import apply_pre_processing, perform_deduplication

def test_apply_pre_processing_returns_same_df_when_no_config(sample_df):
    result = apply_pre_processing(sample_df, {})
    assert result.collect() == sample_df.collect()

@pytest.mark.parametrize(
    "direction,expected_amount",
    [
        ("desc", 20),
        ("asc", 10),
    ],
)
def test_perform_deduplication_keeps_expected_record(spark, direction, expected_amount):
    df = spark.createDataFrame(
        [(1, 10), (1, 20), (2, 30)],
        ["id", "amount"],
    )

    result = perform_deduplication(
        df,
        {
            "strategy": "row_number",
            "keys": ["id"],
            "order_by": [{"column": "amount", "direction": direction}],
        },
    )

    row = result.filter("id = 1").first()
    assert row["amount"] == expected_amount


def test_perform_deduplication_raises_for_unsupported_strategy(sample_df):
    with pytest.raises(ValueError, match="Unsupported deduplication strategy"):
        perform_deduplication(sample_df, {"strategy": "hash"})