from writers.delta_writer import DeltaWriter

def test_delta_writer_writes_delta_append(mock_writer_chain):
    df = type("FakeDf", (), {"write": mock_writer_chain})()
    DeltaWriter("target_table", {"mergeSchema": "true"}).write(df)
    mock_writer_chain.format.assert_called_once_with("delta")
    mock_writer_chain.mode.assert_called_once_with("append")
    mock_writer_chain.option.assert_called_once_with("mergeSchema", "true")
    mock_writer_chain.saveAsTable.assert_called_once_with("target_table")