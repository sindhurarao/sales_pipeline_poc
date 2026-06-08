import json
import uuid
import logging
from datetime import datetime
from helpers.audit_helper import AuditLogger
from helpers.validation_helper import ValidationHelper

def build_copy_into_sql(format_options, copy_options, source_path, target_table, file_format):
    format_clause = ",\n".join( [f"'{k}' = '{v}'" for k, v in format_options.items()])
    copy_clause = ",\n".join([f"'{k}' = '{v}'" for k, v in copy_options.items()])

    sql = f""" COPY INTO {target_table}
    FROM '{source_path}'
    FILE_FORMAT = '{file_format}'
    """
    if format_clause:
        sql += f"""
        FORMAT_OPTIONS (
            {format_clause}
        )
        """
    if copy_clause:
        sql += f"""
        COPY_OPTIONS (
            {copy_clause}
        )
        """
    return sql

def run(spark, config):

    source_path = config["source"]["path"]
    file_format = config["source"]["format"].upper()
    target_table = config["target"]["table"]
    audit_table = config["audit"]["table"]
    format_options = config.get("format_options", {})
    copy_options = config.get("copy_options", {})

    run_id = str(uuid.uuid4())
    start_time = datetime.now()
    audit = AuditLogger(spark=spark, audit_table=audit_table)
    validator = ValidationHelper(spark)
    logger = logging.getLogger("copy_into_bronze")

    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )

    logger.info(f"Run Id        : {run_id}")
    logger.info(f"Source Path   : {source_path}")
    logger.info(f"Target Table  : {target_table}")

    try:

        files = validator.validate_path_exists(source_path)
        validator.validate_non_empty_files(files)
        validator.validate_table_exists(target_table)
        copy_sql = build_copy_into_sql(format_options, copy_options, source_path, target_table, file_format)

        logger.info("Executing COPY INTO")
        logger.info(copy_sql)

        result_df = spark.sql(copy_sql)
        metrics = result_df.collect()[0]
        rows_ingested = metrics.get("num_rows_copied", 0)
        files_copied = metrics.get("num_files_copied", 0)
        end_time = datetime.now()

        logger.info(
            f"Successfully copied "
            f"{files_copied} files and "
            f"{rows_ingested} rows."
        )
        audit.log(
            run_id=run_id,
            source=source_path,
            target=target_table,
            status="SUCCESS",
            start_time=start_time,
            end_time=end_time,
            records_ingested=rows_ingested
        )


    except Exception as exception:
        logger.exception("Pipeline failed")
        try:
            audit.log(
                run_id=f"{run_id}",
                source=source_path,
                target=target_table,
                status="FAILED",
                start_time=start_time,
                end_time=datetime.now(),
                records_ingested=0,
                error_message=str(exception)
            )
        except Exception as audit_error:
            logger.error(f"Audit logging failed: {audit_error}")
        raise