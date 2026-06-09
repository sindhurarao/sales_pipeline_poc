from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

class FreshnessCheckFailed(Exception):
    pass

def apply_pre_processing(df: DataFrame, config: dict) -> DataFrame:
    pre_processing = config.get("pre_processing", {})
    if not pre_processing:
        return df

    freshness_config = pre_processing.get("freshness_check")
    if freshness_config and freshness_config.get("enabled", False):
        perform_freshness_check(df, freshness_config)

    dedup_config = pre_processing.get("deduplication")
    if dedup_config and dedup_config.get("enabled", False):
        df = perform_deduplication(df, dedup_config)

    return df

def perform_freshness_check(df: DataFrame, config: dict):
    timestamp_column = config["timestamp_column"]
    max_delay_hours = config["max_delay_hours"]
    fail_on_stale_data = config.get("fail_on_stale_data", True)
    max_timestamp = (
        df.select(F.max(F.col(timestamp_column)).alias("max_ts"))
        .collect()[0]["max_ts"]
    )
    if max_timestamp is None:
        raise FreshnessCheckFailed(
            f"No value found in column '{timestamp_column}'"
        )
    age_hours = (datetime.now() - max_timestamp).total_seconds() / 3600
    if age_hours > max_delay_hours:
        message = (
            f"Freshness check failed. "
            f"Latest timestamp={max_timestamp}, "
            f"Age={round(age_hours, 2)} hours, "
            f"Allowed={max_delay_hours} hours"
        )
        if fail_on_stale_data:
            raise FreshnessCheckFailed(message)
        else:
            print(f"WARNING: {message}")


def perform_deduplication(df: DataFrame, config: dict) -> DataFrame:
    strategy = config.get("strategy", "row_number")
    if strategy != "row_number":
        raise ValueError(f"Unsupported deduplication strategy: {strategy}")
    keys = config["keys"]
    order_by = config["order_by"]
    sort_columns = []
    for item in order_by:
        column_name = item["column"]
        direction = item.get("direction", "desc").lower()
        if direction == "desc":
            sort_columns.append(F.col(column_name).desc())
        else:
            sort_columns.append(F.col(column_name).asc())

    window_spec = Window.partitionBy(*keys).orderBy(*sort_columns)
    return (
        df.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )