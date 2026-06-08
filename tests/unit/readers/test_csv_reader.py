from readers.csv_reader import CsvReader

def test_csv_reader_applies_options_and_loads_path(mock_spark):
    reader_chain = mock_spark.read
    reader_chain.option.return_value = reader_chain
    reader_chain.format.return_value = reader_chain

    reader = CsvReader(mock_spark, "/mnt/input.csv", {"header": "true", "inferSchema": "true"})
    reader.read()

    reader_chain.option.assert_any_call("header", "true")
    reader_chain.option.assert_any_call("inferSchema", "true")
    reader_chain.format.assert_called_once_with("csv")
    reader_chain.load.assert_called_once_with("/mnt/input.csv")