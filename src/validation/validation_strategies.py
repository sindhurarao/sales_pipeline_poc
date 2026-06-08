from abc import ABC, abstractmethod
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class ValidationStrategy(ABC):
    @abstractmethod
    def apply(self, df, rule):
        pass

class NotNullValidation(ValidationStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        return df.withColumn(
            "_dq_error",
            F.when(
                F.col(col_name).isNull() |
                (F.trim(F.col(col_name).cast("string")) == ""),
                F.lit(f"{col_name} is null")
            )
        )

class MinValidation(ValidationStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        min_value = float(rule["format"])
        return df.withColumn(
            "_dq_error",
            F.when(
                F.col(col_name).cast("double") < min_value,
                F.lit(f"{col_name} below minimum")
            )
        )

class RangeValidation(ValidationStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        low, high = [float(x) for x in rule["format"].split(",")]
        return df.withColumn(
            "_dq_error",
            F.when(
                (F.col(col_name).cast("double") < low) |
                (F.col(col_name).cast("double") > high),
                F.lit(f"{col_name} outside allowed range")
            )
        )

class DuplicateKeyValidation(ValidationStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        w = Window.partitionBy(col_name)

        return (
            df.withColumn("_dup_count", F.count("*").over(w))
            .withColumn(
                "_dq_error",
                F.when(F.col("_dup_count") > 1, F.lit(f"{col_name} duplicate key"))
            )
            .drop("_dup_count")
        )

class RefIntegrityValidation(ValidationStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        ref_dataset, ref_col = rule["rule_params"].split(".")
        ref_df = self.spark.table(ref_dataset)
        return (
            df.join(
                ref_df.select(F.col(ref_col).alias("_ref_key")).distinct(),
                df[col_name] == F.col("_ref_key"),
                "left"
            )
            .withColumn(
                "_dq_error",
                F.when(F.col("_ref_key").isNull(), F.lit(f"{col_name} missing reference"))
            )
            .drop("_ref_key")
        )

class ConflictingKeyValidation(ValidationStrategy):
    def apply(self, df, rule):
        key_col = rule["field_name"]
        compare_cols = [c.strip() for c in rule["rule_params"].split(",")]
        agg_exprs = [F.countDistinct(c).alias(f"{c}_distinct_count") for c in compare_cols]
        conflict_df = df.groupBy(key_col).agg(*agg_exprs)
        condition = None
        for c in compare_cols:
            expr = F.col(f"{c}_distinct_count") > 1
            condition = expr if condition is None else condition | expr
        conflict_df = conflict_df.withColumn("_has_conflict", condition)
        return (
            df.join(conflict_df.select(key_col, "_has_conflict"), key_col, "left")
            .withColumn(
                "_dq_error",
                F.when(F.col("_has_conflict"), F.lit(f"{key_col} conflicting definition"))
            )
            .drop("_has_conflict")
        )