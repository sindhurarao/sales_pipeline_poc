class ValidationHelper:

    class ValidationHelper:
        def __init__(self, spark, dbutils):
            self.spark = spark
            self.dbutils = dbutils

    def validate_path_exists(self, path):
        try:
            if self.dbutils is None:
                raise RuntimeError("dbutils is required for path validation")
            files = self.dbutils.fs.ls(path)
            if not files:
                raise Exception(f"No files found under {path}")
            return files
        except Exception as exception:
            raise Exception(f"Path validation failed: {str(exception)}")

    def validate_table_exists(self, table_name):
        if not self.spark.catalog.tableExists(table_name):
            raise Exception(f"Target table does not exist: {table_name}")
        return True

    def validate_non_empty_files(self, files):
        non_zero_files = [f for f in files if f.size > 0]
        if len(non_zero_files) == 0:
            raise Exception("All files are empty")
        return non_zero_files
