from abc import ABC, abstractmethod
from pyspark.sql import functions as F

class TransformationStrategy(ABC):
    @abstractmethod
    def apply(self, df, transformation):
        pass

class RenameTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.withColumnRenamed(
            transformation["source_column"],
            transformation["target_column"]
        )

class ExpressionTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.withColumn(
            transformation["target_column"],
            F.expr(transformation["expression"])
        )

class SelectTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.select(*transformation["columns"])

class DropTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.drop(*transformation["columns"])

class CastTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.withColumn(
            transformation["target_column"],
            F.col(transformation["target_column"]).cast(transformation["datatype"])
        )

class LiteralTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.withColumn(
            transformation["target_column"],
            F.lit(transformation["value"])
        )

class RoundTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.withColumn(
            transformation["target_column"],
            F.round(
                F.col(transformation["target_column"]),
                int(transformation["scale"])
            )
        )

class AliasTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        return df.withColumn(
            transformation["alias"],
            F.col(transformation["source_column"])
        )

class TryCastTransformation(TransformationStrategy):
    def apply(self, df, transformation):
        target_column = transformation["target_column"]
        datatype = transformation["datatype"]
        return df.withColumn(
            target_column,
            F.expr(f"try_cast({target_column} AS {datatype})")
        )