class DeltaWriter:

    def __init__(
            self,
            target_table,
            options=None
    ):

        self.target_table = target_table
        self.options = options or {}

    def write(self, df):

        writer = (
            df.write
            .format("delta")
            .mode("append")
        )

        for key, value in self.options.items():
            writer = writer.option(key,value)

        writer.saveAsTable(self.target_table)
