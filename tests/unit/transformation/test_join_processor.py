import pytest
from unittest.mock import MagicMock, patch
from transformation.join_processor import JoinProcessor

@pytest.fixture
def joins_config():
    return [
        {
            "type": "broadcast",
            "table": "customer_lookup",
            "alias": "c",
            "condition": "src.customer_id = c.customer_id",
            "join_type": "left",
        },
        {
            "type": "shuffle",
            "table": "region_lookup",
            "alias": "r",
            "condition": "src.region_id = r.region_id",
            "join_type": "inner",
        },
    ]

def test_join_processor_applies_joins_in_order(mock_spark, joins_config):
    source_df = MagicMock(name="source_df")
    first_result = MagicMock(name="first_result")
    second_result = MagicMock(name="second_result")

    first_strategy = MagicMock()
    first_strategy.join.return_value = first_result
    second_strategy = MagicMock()
    second_strategy.join.return_value = second_result

    with patch("transformation.join_processor.JoinFactory") as mock_factory:
        mock_factory.create.side_effect = [first_strategy, second_strategy]
        result = JoinProcessor(mock_spark).apply(source_df, joins_config)

    assert result == second_result

    mock_factory.create.assert_any_call("broadcast")
    mock_factory.create.assert_any_call("shuffle")

    first_strategy.join.assert_called_once_with(
        source_df,
        joins_config[0],
        mock_spark,
    )

    second_strategy.join.assert_called_once_with(
        first_result,
        joins_config[1],
        mock_spark,
    )


def test_join_processor_returns_source_df_when_no_joins(mock_spark):
    source_df = MagicMock(name="source_df")
    result = JoinProcessor(mock_spark).apply(source_df, [])
    assert result == source_df
