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
        if df is not None:
            writer = df.write.format("delta")
            write_mode = write_options.get("mode", "append")
            if write_mode in ["append","overwrite"]:
                writer.mode(write_mode).saveAsTable(target["table"])
            elif write_mode == "merge":
                self.merge(df, target["table"], write_options["mergeKeys"])
            elif write_mode == "scd2_merge":
                self.scd2_merge(
                    df,
                    target["table"],
                    write_options
                )
            else:
                raise ValueError(f"Unsupported write mode: {write_mode}")

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

    def scd2_merge(self,source_df, target_table, options):
        business_keys = options["businessKeys"]
        change_columns = options["changeColumns"]
        effective_col = options.get("effectiveDateColumn", "effective_date")
        expiry_col = options.get("expiryDateColumn", "expiry_date")
        current_col = options.get("currentFlagColumn", "current_flag")
        active_expiry = options.get("activeExpiryDate", "9999-12-31")

        source_df.createOrReplaceTempView("source_view")

        join_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in business_keys]
            + [f"target.{current_col} = true"]
        )

        change_condition = " OR ".join(
            [
                f"NOT (source.{col} <=> target.{col})"
                for col in change_columns
            ]
        )

        self.spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING global_temp.source_view AS source
            ON {join_condition}
    
            WHEN MATCHED AND ({change_condition})
            THEN UPDATE SET
                target.{current_col} = false,
                target.{expiry_col} = date_sub(current_date(), 1)
        """)

        source_columns = source_df.columns

        insert_columns = (
                source_columns
                + [effective_col, expiry_col, current_col]
        )

        insert_select = (
                [f"source.{col}" for col in source_columns]
                + [
                    f"current_date() AS {effective_col}",
                    f"DATE('{active_expiry}') AS {expiry_col}",
                    f"true AS {current_col}",
                ]
        )

        business_key_null_check = f"target.{business_keys[0]} IS NULL"

        self.spark.sql(f"""
            INSERT INTO {target_table} (
                {", ".join(insert_columns)}
            )
            SELECT
                {", ".join(insert_select)}
            FROM global_temp.source_view source
            LEFT JOIN {target_table} target
                ON {" AND ".join([f"source.{key} = target.{key}" for key in business_keys])}
               AND target.{current_col} = true
            WHERE {business_key_null_check}
               OR {change_condition}
        """)