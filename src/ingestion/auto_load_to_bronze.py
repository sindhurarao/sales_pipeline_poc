from pyspark.sql.functions import *
from datetime import datetime
import json
import uuid
import logging
from helpers.audit_helper import AuditLogger
from helpers.validation_helper import ValidationHelper

def process_batch(batch_df,
                  batch_id,
                  logger,
                  audit,
                  run_id,
                  source_path,
                  target_table):
    batch_start = datetime.now()
    try:
        record_count = batch_df.count()
        logger.info(
            f"Batch {batch_id} "
            f"records={record_count}"
        )
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .option(
                "mergeSchema",
                "true"
            )
            .saveAsTable(target_table)
        )

        logger.info(f"Successfully written {record_count} records from {source_path} to {target_table}")
        audit.log(
            run_id=f"{run_id}_{batch_id}",
            source=source_path,
            target=target_table,
            status="SUCCESS",
            start_time=batch_start,
            end_time=datetime.now(),
            records_ingested=record_count
        )

    except Exception as e:
        logger.exception(
            f"Batch {batch_id} failed"
        )
        audit.log(
            run_id=f"{run_id}",
            source=source_path,
            target=target_table,
            status="FAILED",
            start_time=batch_start,
            end_time=datetime.now(),
            records_ingested=0,
            error_message=str(e)
        )
        raise

def run(spark, config, dbutils):

    source_path = config["source"]["path"]
    source_format = config["source"]["format"].lower()
    target_table = config["target"]["table"]
    audit_table = config["audit"]["table"]
    checkpoint_path = config["streaming"]["checkpoint_path"]
    schema_location = config["streaming"]["schema_location"]
    schema_evolution_mode = (config["streaming"].get("schema_evolution_mode", "addNewColumns"))
    trigger_interval = (config["streaming"].get("trigger_interval", "5 minutes"))

    run_id = str(uuid.uuid4())
    start_time = datetime.now()
    audit = AuditLogger(spark=spark, audit_table=audit_table)
    validator = ValidationHelper(spark, dbutils)

    logger = logging.getLogger("auto_load_to_bronze")
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )
    logger.info(f"Run Id       : {run_id}")
    logger.info(f"Source Path  : {source_path}")
    logger.info(f"Target Table : {target_table}")

    files = validator.validate_path_exists(source_path)
    validator.validate_non_empty_files(files)
    validator.validate_table_exists(target_table)
    stream = (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            source_format
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_location
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            schema_evolution_mode
        )
        .load(source_path)
    )

    stream_df = (stream
                .withColumn(
                    "ingestion_timestamp",
                    current_timestamp()
                )
                .withColumn(
                    "source_file_name",
                    lit(source_path)
                )
                .withColumn(
                    "run_id",
                    lit(run_id)
                )
                .withColumn(
                    "load_date",
                    current_date()
                )
    )

    query = (
        stream_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df,
                batch_id,
                logger,
                audit,
                run_id,
                source_path,
                target_table,
            )
        )
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .trigger(
            processingTime=trigger_interval
        )
        .start()
    )
    query.awaitTermination()