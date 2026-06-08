from abc import ABC, abstractmethod
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

class JoinStrategy(ABC):

    @abstractmethod
    def join(self, source_df, join_config, spark):
        pass

class BroadcastJoinStrategy(JoinStrategy):

    def join(self, source_df, join_config, spark):
        lookup_df = (
            spark.table(join_config["table"])
            .alias(join_config["alias"])
        )
        return source_df.join(
            broadcast(lookup_df),
            F.expr(join_config["condition"]),
            join_config["join_type"]
        )

class ShuffleJoinStrategy(JoinStrategy):

    def join(self, source_df, join_config, spark):
        lookup_df = (
            spark.table(join_config["table"])
            .alias(join_config["alias"])
        )
        return source_df.join(
            lookup_df,
            F.expr(join_config["condition"]),
            join_config["join_type"]
        )
