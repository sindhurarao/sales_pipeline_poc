from pyspark.sql import functions as F

from tests.conftest import spark
from validation.validation_factory import ValidationFactory

class RuleValidator:
    def apply(self, df, rules_df):
        clean_df = df
        quarantine_df = None
        quarantine_rules = (rules_df.filter(F.col("action") == "quarantine").collect())

        for rule in quarantine_rules:
            rule_dict = rule.asDict()
            strategy = ValidationFactory.create(rule_dict)
            validated_df = strategy.apply(clean_df, rule_dict)
            failed_df = validated_df.filter(F.col("_dq_error").isNotNull())
            if quarantine_df is None:
                quarantine_df = failed_df
            else:
                quarantine_df = quarantine_df.unionByName(failed_df, allowMissingColumns=True)
            clean_df = validated_df.filter(F.col("_dq_error").isNull()).drop("_dq_error")
        return clean_df, quarantine_df