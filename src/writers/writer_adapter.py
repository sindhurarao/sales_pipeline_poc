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