import pytest
from helpers.audit_helper import AuditLogger

@pytest.fixture(scope="module")
def audit_table(spark):
    table_name = "test_audit_table"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name} (
            run_id STRING,
            source STRING,
            target STRING,
            status STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            records_ingested BIGINT,
            error_message STRING
        )
        USING PARQUET
    """)

    yield table_name

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

@pytest.fixture(scope="module")
def audit_logger(spark, audit_table):
    yield AuditLogger(spark, audit_table)


def test_log_success(spark, audit_table, audit_logger):

    audit_logger.log(
        run_id="test_run",
        source="test_source",
        target="test_target",
        status="SUCCESS",
        start_time="2026-06-07 06:00:00",
        end_time="2026-06-07 06:06:00",
        records_ingested=100,
    )

    row = spark.table(audit_table).collect()[0]

    assert row.run_id == "test_run"
    assert row.source == "test_source"
    assert row.target == "test_target"
    assert row.status == "SUCCESS"
    assert row.records_ingested == 100
    assert row.error_message is None


def test_log_with_error_message(spark, audit_table, audit_logger):

    audit_logger.log(
        run_id="test_run",
        source="test_source",
        target="test_target",
        status="FAILED",
        start_time="2026-06-07 06:00:00",
        end_time="2026-06-07 06:06:00",
        error_message="Something went wrong",
    )

    row = spark.table(audit_table).collect()[0]

    assert row.run_id == "test_run"
    assert row.status == "FAILED"
    assert row.records_ingested == 0
    assert row.error_message == "Something went wrong"


def test_log_escapes_single_quotes(spark, audit_table, audit_logger):

    audit_logger.log(
        run_id="test_run",
        source="test_source",
        target="test_target",
        status="FAILED",
        start_time="2026-06-07 06:00:00",
        end_time="2026-06-07 06:06:00",
        error_message="can't connect to db",
    )

    row = spark.table(audit_table).collect()[0]
    assert row.error_message == "can't connect to db"