import json
import uuid
import logging
from datetime import datetime

from readers.reader_factory import ReaderFactory
from writers.delta_writer import DeltaWriter

from common.audit_helper import AuditLogger
from common.validation_helper import ValidationHelper
from common.metadata_helper import MetadataHelper

dbutils.widgets.text("config", "")
config = json.loads(dbutils.widgets.get("config"))

source_path = config["source"]["path"]
file_format = config["source"]["format"]
target_table = config["target"]["table"]
audit_table = config["audit"]["table"]
read_options = config.get("read_options",{})
write_options = config.get("write_options",{})
metadata_config = config.get("metadata_columns",{})

run_id = str(uuid.uuid4())
start_time = datetime.now()

audit = AuditLogger(spark=spark, audit_table=audit_table)
validator = ValidationHelper(spark)
logger = logging.getLogger("load_file_to_bronze")

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
    record_count = source_df.count()

    enriched_df = MetadataHelper.enrich(
        source_df,
        metadata_config,
        run_id
    )

    writer = DeltaWriter(target_table,write_options)
    writer.write(enriched_df)

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
