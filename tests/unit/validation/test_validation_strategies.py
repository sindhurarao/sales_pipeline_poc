import pytest
from validation.validation_strategies import *

def test_not_null_validation_marks_null_and_blank(sample_df):
    result = NotNullValidation().apply(sample_df, {"field_name": "name"})
    failures = result.filter("_dq_error is not null").count()
    assert failures == 2

def test_min_validation_marks_values_below_min(sample_df):
    result = MinValidation().apply(sample_df, {"field_name": "amount", "format": "0"})
    assert result.filter("_dq_error is not null").count() == 1

def test_range_validation_marks_out_of_range_values(sample_df):
    result = RangeValidation().apply(sample_df, {"field_name": "id", "format": "0,2"})
    assert result.filter("_dq_error is not null").count() == 1

def test_duplicate_key_validation_marks_duplicates(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    result = DuplicateKeyValidation().apply(df, {"field_name": "id"})
    assert result.filter("_dq_error is not null").count() == 2

def test_conflicting_key_validation_marks_conflicts(spark):
    df = spark.createDataFrame(
        [(1, "A"), (1, "B"), (2, "C")],
        ["id", "name"],
    )
    result = ConflictingKeyValidation().apply(
        df,
        {"field_name": "id", "rule_params": "name"},
    )
    assert result.filter("_dq_error is not null").count() == 2