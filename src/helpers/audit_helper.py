from datetime import datetime

class AuditLogger:

    def __init__(self, spark, audit_table):
        self.spark = spark
        self.audit_table = audit_table

    def log(
            self,
            run_id,
            source,
            target,
            status,
            start_time,
            end_time,
            records_ingested=0,
            error_message=None
    ):

        escaped_error = (error_message.replace("'", "''") if error_message else None)

        self.spark.sql(f"""
        INSERT INTO {self.audit_table}
        VALUES (
            '{run_id}',
            '{source}',
            '{target}',
            '{status}',
            TIMESTAMP('{start_time}'),
            TIMESTAMP('{end_time}'),
            {records_ingested},
            {f"'{escaped_error}'" if escaped_error else "NULL"}
        )
        """)