from delta.tables import DeltaTable

class WriterAdapter:
    def __init__(self, spark):
        self.spark = spark

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

        delta_table = DeltaTable.forName(self.spark,table_name)

        merge_condition = " AND ".join(
            [
                f"target.{k}=source.{k}"
                for k in business_keys
            ]
            + [f"target.{current_col}=true"]
        )

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

        # Expire current record
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

        current_target = (
            self.spark.table(table_name)
            .filter(col(current_col) == True)
        )

        join_condition = [
            df[k] == current_target[k]
            for k in business_keys
        ]

        joined = (
            df.alias("source")
            .join(
                current_target.alias("target"),
                join_condition,
                "left"
            )
        )

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

        (
            rows_to_insert.write
            .format("delta")
            .mode("append")
            .saveAsTable(table_name)
        )