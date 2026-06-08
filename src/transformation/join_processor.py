from transformation.join_factory import JoinFactory

class JoinProcessor:

    def __init__(self, spark):
        self.spark = spark

    def apply(self, source_df, joins):
        result = source_df
        for join_config in joins:
            strategy = JoinFactory.create(join_config["type"])
            result = strategy.join(result,join_config,self.spark)
        return result