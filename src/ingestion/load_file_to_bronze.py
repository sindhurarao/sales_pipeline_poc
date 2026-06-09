import json
import uuid
import logging
from datetime import datetime

from readers.reader_factory import ReaderFactory
from writers.delta_writer import DeltaWriter
from helpers.audit_helper import AuditLogger
from helpers.validation_helper import ValidationHelper
from helpers.metadata_helper import MetadataHelper

def run(spark, config, dbutils):

    source_path = config["source"]["path"]
    file_format = config["source"]["format"]
    target_table = config["target"]["table"]
    audit_table = config["audit"]["table"]
    read_options = config.get("format_options",{})
    write_options = config.get("write_options",{})
    metadata_config = config.get("metadata_columns",{})

    run_id = str(uuid.uuid4())
    start_time = datetime.now()
    audit = AuditLogger(spark=spark, audit_table=audit_table)
    validator = ValidationHelper(spark, dbutils)
    logger = logging.getLogger("load_file_to_bronze")

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

        reader = ReaderFactory.get_reader(
            spark=spark,
            file_format=file_format,
            source_path=source_path,
            options=read_options
        )

        source_df = reader.read()
        enriched_df = MetadataHelper.enrich(
            source_df,
            metadata_config,
            run_id
        )
        record_count = enriched_df.count()

        writer = DeltaWriter(target_table,write_options)
        writer.write(enriched_df)

        logger.info(f"Successfully written {record_count} records from {source_path} to {target_table}")
        audit.log(
            run_id=run_id,
            source=source_path,
            target=target_table,
            status="SUCCESS",
            start_time=start_time,
            end_time=datetime.now(),
            records_ingested=record_count
        )

    except Exception as e:
        logger.exception("Pipeline failed")
        audit.log(
            run_id=run_id,
            source=source_path,
            target=target_table,
            status="FAILED",
            start_time=start_time,
            end_time=datetime.now(),
            records_ingested=0,
            error_message=str(e)
        )
        raise
