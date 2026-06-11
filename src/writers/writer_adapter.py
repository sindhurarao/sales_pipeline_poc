from delta.tables import DeltaTable
from pyspark.sql.functions import *
from functools import reduce

class WriterAdapter:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def write_table(self, df, table_name, mode="append"):
        if df is not None:
            df.write.mode(mode).format("delta").saveAsTable(table_name)

    def write(self, df, target, write_options):
        mode = write_options.get("mode", "append")
        if mode == "append":
            df.write.mode("append").format("delta").saveAsTable(target["table"])
        elif mode == "overwrite":
            df.write.mode("overwrite").format("delta").saveAsTable(target["table"])
        elif mode == "merge":
            self.merge(df, target["table"], write_options["mergeKeys"])
        elif mode == "scd2_merge":
            self.scd2_merge(
                df,
                target["table"],
                write_options
            )
        else:
            raise ValueError(f"Unsupported write mode: {mode}")

    def merge(self, df, table_name, merge_keys):
        target = DeltaTable.forName(self.spark, table_name)
        condition = " AND ".join([
            f"target.{key} = source.{key}"
            for key in merge_keys
        ])
        self.logger.info(f"Merge condition: {condition}")
        (
            target.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def scd2_merge(
            self,
            df,
            table_name,
            options
    ):

        business_keys = options["businessKeys"]
        change_columns = options["changeColumns"]

        effective_col = options.get("effectiveDateColumn","effective_date")
        expiry_col = options.get("expiryDateColumn","expiry_date")
        current_col = options.get("currentFlagColumn","current_flag")
        active_expiry = options.get("activeExpiryDate","9999-12-31")

        #Load existing silver table
        delta_table = DeltaTable.forName(self.spark,table_name)

        #Matches only the current active records
        merge_condition = " AND ".join(
            [
                f"target.{k}=source.{k}"
                for k in business_keys
            ]
            + [f"target.{current_col}=true"]
        )
        self.logger.info(f"SCD2 Merge condition: {merge_condition}")

        #Detects only changed attributes in existing records
        change_condition = " OR ".join(
            [
                f"""
                COALESCE(CAST(target.{c} AS STRING),'')
                <>
                COALESCE(CAST(source.{c} AS STRING),'')
                """
                for c in change_columns
            ]
        )

        self.logger.info(f"SCD2 Change condition: {change_condition}")

        # Expire old active records by setting flag false and merge existing active records
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition=change_condition,
                set={
                    current_col: "false",
                    expiry_col: "date_sub(current_date(),1)"
                }
            )
            .execute()
        )

        #Now load only active records from silver table
        current_target = (
            self.spark.table(table_name)
            .filter(col(current_col) == True)
        )

        #Check if each incoming source has active target by joining source to active target on business keys
        join_condition = reduce(
            lambda a, b: a & b,
            [
                col(f"source.{k}") == col(f"target.{k}")
                for k in business_keys
            ]
        )

        self.logger.info(f"SCD2 Join condition: {join_condition}")

        joined = (
            df.alias("source")
            .join(
                current_target.alias("target"),
                join_condition,
                "left"
            )
        )

        #Select rows that need insertion new business key or new values which are now active for which old was expired
        changed_condition = reduce(
            lambda a, b: a | b,
            [
                col(f"target.{c}").isNull()
                |
                (
                        col(f"source.{c}").cast("string")
                        !=
                        col(f"target.{c}").cast("string")
                )
                for c in change_columns
            ]
        )

        self.logger.info(f"SCD2 Changed condition: {changed_condition}")

        #Write selected rows to silver
        rows_to_insert = (
            joined
            .filter(changed_condition)
            .select("source.*")
            .withColumn(
                effective_col,
                current_date()
            )
            .withColumn(
                expiry_col,
                lit(active_expiry).cast("date")
            )
            .withColumn(
                current_col,
                lit(True)
            )
        )

        self.logger.info(f"Schema of dataframe to be merged: {rows_to_insert.printSchema()}")

        (
            rows_to_insert.write
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )