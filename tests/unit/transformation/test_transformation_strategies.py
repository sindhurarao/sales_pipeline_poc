import pytest
from transformation.transformation_strategies import *

def test_rename_transformation(sample_df):
    result = RenameTransformation().apply(
        sample_df,
        {"source_column": "name", "target_column": "customer_name"},
    )
    assert "customer_name" in result.columns
    assert "name" not in result.columns


def test_expression_transformation(sample_df):
    result = ExpressionTransformation().apply(
        sample_df,
        {"target_column": "amount_plus_one", "expression": "amount + 1"},
    )
    assert result.orderBy("id").first()["amount_plus_one"] == pytest.approx(11.126)


def test_select_transformation(sample_df):
    result = SelectTransformation().apply(sample_df, {"columns": ["id", "name"]})
    assert result.columns == ["id", "name"]


def test_drop_transformation(sample_df):
    result = DropTransformation().apply(sample_df, {"columns": ["event_date"]})
    assert "event_date" not in result.columns


def test_cast_transformation(sample_df):
    result = CastTransformation().apply(
        sample_df,
        {"target_column": "id", "datatype": "string"},
    )
    assert dict(result.dtypes)["id"] == "string"


def test_literal_transformation(sample_df):
    result = LiteralTransformation().apply(
        sample_df,
        {"target_column": "country", "value": "US"},
    )
    assert result.first()["country"] == "US"


def test_round_transformation(sample_df):
    result = RoundTransformation().apply(
        sample_df,
        {"target_column": "amount", "scale": 2},
    )
    assert result.orderBy("id").first()["amount"] == 10.13

def test_alias_transformation(sample_df):
    result = AliasTransformation().apply(
        sample_df,
        {"source_column": "name", "alias": "customer_name"},
    )
    assert "customer_name" in result.columns
    assert "name" in result.columns

def test_try_cast_transformation(sample_df):
    result = TryCastTransformation().apply(
        sample_df,
        {"target_column": "id", "datatype": "string"},
    )
    assert dict(result.dtypes)["id"] == "string"