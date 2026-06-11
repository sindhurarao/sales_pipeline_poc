from readers.json_reader import JsonReader

def test_json_reader_applies_options_and_loads_path(mock_spark):
    reader_chain = mock_spark.read
    reader_chain.option.return_value = reader_chain
    reader_chain.format.return_value = reader_chain

    reader = JsonReader(mock_spark, "/mnt/input.json")
    reader.read()
    reader_chain.load.assert_called_once_with("/mnt/input.json")