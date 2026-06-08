from validation.rule_validator import RuleValidator

def test_rule_validator_quarantines_failed_rows(spark):
    df = spark.createDataFrame([(1, "A"), (2, None)], ["id", "name"])
    rules_df = spark.createDataFrame(
        [
            {
                "action": "quarantine",
                "ruletype": "not_null",
                "field_name": "name",
                "format": None,
                "rule_params": None,
            }
        ]
    )
    clean_df, quarantine_df = RuleValidator().apply(df, rules_df)
    assert clean_df.count() == 1
    assert quarantine_df.count() == 1