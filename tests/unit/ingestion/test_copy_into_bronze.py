from ingestion.copy_into_bronze import build_copy_into_sql

def test_build_copy_into_sql_with_options():
    sql = build_copy_into_sql(
        {"header": "true"},
        {"force": "false"},
        "/mnt/source",
        "bronze_orders",
        "CSV",
    )
    assert "COPY INTO bronze_orders" in sql
    assert "FROM '/mnt/source'" in sql
    assert "FILE_FORMAT = 'CSV'" in sql
    assert "FORMAT_OPTIONS" in sql
    assert "'header' = 'true'" in sql
    assert "COPY_OPTIONS" in sql
    assert "'force' = 'false'" in sql

def test_build_copy_into_sql_without_options():
    sql = build_copy_into_sql({}, {}, "/mnt/source",
                              "bronze_orders", "CSV")
    assert "FORMAT_OPTIONS" not in sql
    assert "COPY_OPTIONS" not in sql