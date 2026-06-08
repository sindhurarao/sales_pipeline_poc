from datetime import datetime
from helpers.audit_helper import AuditLogger

def test_audit_logger_executes_insert_sql(mock_spark):
    logger = AuditLogger(mock_spark, "audit_table")

    logger.log(
        run_id="test_run",
        source="/input",
        target="target_table",
        status="SUCCESS",
        start_time=datetime(2026, 6, 7, 1, 0, 0),
        end_time=datetime(2026, 6, 7, 1, 1, 0),
        records_ingested=10,
    )

    sql = mock_spark.sql.call_args.args[0]
    assert "INSERT INTO audit_table" in sql
    assert "'test_run'" in sql
    assert "'SUCCESS'" in sql
    assert "10" in sql
    assert "NULL" in sql


def test_audit_logger_escapes_single_quotes(mock_spark):
    logger = AuditLogger(mock_spark, "audit_table")

    logger.log(
        "test_run",
        "/input",
        "target",
        "FAILED",
        datetime(2026, 6, 7),
        datetime(2026, 6, 7),
        error_message="can't load file",
    )

    sql = mock_spark.sql.call_args.args[0]
    assert "can''t load file" in sql