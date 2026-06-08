import pytest
from unittest.mock import MagicMock, patch
from ingestion.auto_load_to_bronze import process_batch, run

@pytest.fixture
def autoloader_config():
    return {
        "source": {"path": "/mnt/raw/orders", "format": "csv"},
        "target": {"table": "bronze_orders"},
        "audit": {"table": "silver.ingestion_audit"},
        "streaming": {
            "checkpoint_path": "/mnt/checkpoints/orders",
            "schema_location": "/mnt/schema/orders",
            "trigger_interval": "1 minute",
            "schema_evolution_mode": "addNewColumns",
        },
    }

def test_process_batch_writes_delta_and_logs_success(mock_writer_chain):
    batch_df = MagicMock()
    batch_df.count.return_value = 10
    batch_df.write = mock_writer_chain

    audit = MagicMock()
    logger = MagicMock()

    process_batch(
        batch_df=batch_df,
        batch_id=7,
        logger=logger,
        audit=audit,
        run_id="test_run",
        source_path="/mnt/raw/orders",
        target_table="bronze_orders",
    )

    mock_writer_chain.format.assert_called_once_with("delta")
    mock_writer_chain.mode.assert_called_once_with("append")
    mock_writer_chain.option.assert_called_once_with("mergeSchema", "true")
    mock_writer_chain.saveAsTable.assert_called_once_with("bronze_orders")

    audit.log.assert_called_once()
    assert audit.log.call_args.kwargs["run_id"] == "test_run_7"
    assert audit.log.call_args.kwargs["status"] == "SUCCESS"
    assert audit.log.call_args.kwargs["records_ingested"] == 10


def test_process_batch_logs_failure_and_reraises(mock_writer_chain):
    batch_df = MagicMock()
    batch_df.count.return_value = 10
    batch_df.write = mock_writer_chain
    mock_writer_chain.saveAsTable.side_effect = RuntimeError("write failed")

    audit = MagicMock()
    logger = MagicMock()

    with pytest.raises(RuntimeError, match="write failed"):
        process_batch(
            batch_df=batch_df,
            batch_id=1,
            logger=logger,
            audit=audit,
            run_id="run-1",
            source_path="/mnt/raw/orders",
            target_table="bronze_orders",
        )

    audit.log.assert_called_once()
    assert audit.log.call_args.kwargs["status"] == "FAILED"
    assert audit.log.call_args.kwargs["records_ingested"] == 0


@patch("ingestion.auto_load_to_bronze.ValidationHelper")
@patch("ingestion.auto_load_to_bronze.AuditLogger")
def test_run_validates_source_and_target_before_stream(
        mock_audit_logger,
        mock_validation_helper,
        mock_spark,
        autoloader_config,
):
    validator = mock_validation_helper.return_value
    validator.validate_path_exists.return_value = [MagicMock(size=10)]

    read_stream = mock_spark.readStream
    read_stream.format.return_value = read_stream
    read_stream.option.return_value = read_stream

    stream_df = MagicMock()
    stream_df.withColumn.return_value = stream_df
    read_stream.load.return_value = stream_df

    write_stream = stream_df.writeStream
    write_stream.foreachBatch.return_value = write_stream
    write_stream.option.return_value = write_stream
    write_stream.trigger.return_value = write_stream

    query = MagicMock()
    write_stream.start.return_value = query

    with pytest.raises(TypeError):
        run(mock_spark, autoloader_config)

    validator.validate_path_exists.assert_called_once_with("/mnt/raw/orders")
    validator.validate_non_empty_files.assert_called_once()
    validator.validate_table_exists.assert_called_once_with("bronze_orders")