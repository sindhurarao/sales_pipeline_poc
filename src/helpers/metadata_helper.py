from pyspark.sql.functions import *

class MetadataHelper:

    @staticmethod
    def enrich(df, metadata_config, run_id, source_path):
        if metadata_config.get("ingestion_timestamp",False):
            df = df.withColumn("ingestion_timestamp",current_timestamp())
        if metadata_config.get("source_file_name",False):
            df = df.withColumn("source_file_name",lit(source_path))
        if metadata_config.get("load_date",False):
             df = df.withColumn("load_date",current_date())
        if metadata_config.get("run_id",False):
            df = df.withColumn("run_id",lit(run_id))
        if metadata_config.get("_rescued_data",False):
            df = df.withColumn("_rescued_data",lit(None).cast("string"))
        return df
