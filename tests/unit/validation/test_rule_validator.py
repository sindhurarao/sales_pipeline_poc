import pytest

from tests.conftest import sample_df
from validation.rule_validator import RuleValidator

def test_rule_validator_quarantines_failed_rows(spark, sample_df):
    rules_df = spark.createDataFrame(
        [
            {
                "action": "quarantine",
                "datatype": "string",
                "ruletype": "not_null",
                "field_name": "name"
            }
        ]
    )
    clean_df, quarantine_df = RuleValidator().apply(sample_df, rules_df)
    assert clean_df.count() == 1
    assert quarantine_df.count() == 2