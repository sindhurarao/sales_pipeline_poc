import pytest
from transformation.join_strategies import BroadcastJoinStrategy, ShuffleJoinStrategy

@pytest.fixture
def source_df(spark):
    return spark.createDataFrame(
        [
            (1, "A"),
            (2, "B"),
            (3, "C"),
        ],
        ["customer_id", "customer_name"],
    ).alias("src")

@pytest.fixture
def lookup_df(spark):
    return spark.createDataFrame(
        [
            (1, "Gold"),
            (2, "Silver"),
        ],
        ["customer_id", "tier"],
    )

@pytest.fixture
def join_config():
    return {
        "table": "customer_lookup",
        "alias": "lkp",
        "condition": "src.customer_id = lkp.customer_id",
        "join_type": "left",
    }

@pytest.fixture
def spark_with_lookup(spark, lookup_df):
    lookup_df.createOrReplaceTempView("customer_lookup")
    return spark


@pytest.mark.parametrize(
    "strategy_class",
    [
        BroadcastJoinStrategy,
        ShuffleJoinStrategy,
    ],
)
def test_join_strategy_applies_left_join(
        strategy_class,
        source_df,
        spark_with_lookup,
        join_config,
):
    result = strategy_class().join(
        source_df=source_df,
        join_config=join_config,
        spark=spark_with_lookup,
    )

    rows = result.select(
        "src.customer_id",
        "customer_name",
        "tier",
    ).orderBy("customer_id").collect()

    assert len(rows) == 3
    assert rows[0]["tier"] == "Gold"
    assert rows[1]["tier"] == "Silver"
    assert rows[2]["tier"] is None


def test_shuffle_join_strategy_inner_join_filters_non_matches(
        source_df,
        spark_with_lookup,
        join_config,
):
    join_config = {
        **join_config,
        "join_type": "inner",
    }

    result = ShuffleJoinStrategy().join(
        source_df=source_df,
        join_config=join_config,
        spark=spark_with_lookup,
    )

    assert result.count() == 2