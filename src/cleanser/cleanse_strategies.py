from abc import ABC, abstractmethod
from pyspark.sql import functions as F

class CleanseStrategy(ABC):
    @abstractmethod
    def apply(self, df, rule):
        pass

class NullToDefaultCleanse(CleanseStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        default_value = rule["format"] or self.default_for(rule["datatype"])
        return df.withColumn(
            col_name,
            F.when(
                F.col(col_name).isNull() |
                (F.trim(F.col(col_name).cast("string")) == ""),
                F.lit(default_value)
            ).otherwise(F.col(col_name))
        )

    def default_for(self, datatype):
        return {
            "string": "UNKNOWN",
            "integer": 0,
            "decimal": 0.0,
            "date": "9999-12-31"
        }.get(datatype, None)

class RegexCleanse(CleanseStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        pattern = rule["format"]
        return df.withColumn(
            col_name,
            F.trim(F.regexp_replace(F.col(col_name), pattern, ""))
        )

class CaseCleanse(CleanseStrategy):
    def apply(self, df, rule):
        col_name = rule["field_name"]
        case_type = rule["format"]
        if case_type == "lower":
            return df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))
        if case_type == "upper":
            return df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))
        if case_type == "title":
            return df.withColumn(col_name, F.initcap(F.trim(F.col(col_name))))
        return df

class DateFormatCleanse(CleanseStrategy):
    def apply(self, df, rule):
        return df.withColumn(
            rule["field_name"],
            F.to_date(F.col(rule["field_name"]), rule["format"])
        )

class DecimalPrecisionCleanse(CleanseStrategy):
    def apply(self, df, rule):
        precision, scale = rule["format"].split(",")
        return df.withColumn(
            rule["field_name"],
            F.round(
                F.col(rule["field_name"]).cast(f"decimal({precision},{scale})"),
                int(scale)
            )
        )

class IntegerCleanse(CleanseStrategy):
    def apply(self, df, rule):
        return df.withColumn(
            rule["field_name"],
            F.col(rule["field_name"]).cast("int")
        )