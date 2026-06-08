from pyspark.sql import functions as F
from cleanser.cleanse_factory import CleanseFactory

class RuleDrivenCleanser:
    def apply(self, df, rules_df):
        result = df
        clean_rules = (rules_df.filter(F.col("action") == "clean").collect())
        for rule in clean_rules:
            rule_dict = rule.asDict()
            strategy = CleanseFactory.create(rule_dict)
            result = strategy.apply(result, rule_dict)
        return result