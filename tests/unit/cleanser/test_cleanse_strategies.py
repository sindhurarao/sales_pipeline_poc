import pytest
from cleanser.cleanse_strategies import *

def test_null_to_default_cleanse_replaces_null_and_blank(sample_df):
    result = NullToDefaultCleanse().apply(
        sample_df,
        {"field_name": "name", "format": "UNKNOWN", "datatype": "string"},
    )
    values = [r["name"] for r in result.orderBy("id").collect()]
    assert values == [" Test ", "UNKNOWN", "UNKNOWN"]


@pytest.mark.parametrize(
    "case_type,expected",
    [
        ("lower", "test"),
        ("upper", "TEST"),
        ("title", "Test"),
    ],
)
def test_case_cleanse(sample_df, case_type, expected):
    result = CaseCleanse().apply(
        sample_df,
        {"field_name": "name", "format": case_type},
    )
    assert result.orderBy("id").first()["name"] == expected


def test_regex_cleanse_removes_pattern(spark):
    df = spark.createDataFrame([("A-123",)], ["code"])
    result = RegexCleanse().apply(df, {"field_name": "code", "format": "[^0-9]"})
    assert result.first()["code"] == "123"


def test_date_format_cleanse(spark):
    df = spark.createDataFrame([("06-06-2026",)], ["dt"])
    result = DateFormatCleanse().apply(df, {"field_name": "dt", "format": "MM-dd-yyyy"})
    assert str(result.first()["dt"]) == "2026-06-06"


def test_decimal_precision_cleanse(spark):
    df = spark.createDataFrame([(10.126,)], ["amount"])
    result = DecimalPrecisionCleanse().apply(
        df,
        {"field_name": "amount", "format": "10,2"},
    )
    assert float(result.first()["amount"]) == 10.13


def test_integer_cleanse(spark):
    df = spark.createDataFrame([("10",)], ["id"])
    result = IntegerCleanse().apply(df, {"field_name": "id"})
    assert result.first()["id"] == 10