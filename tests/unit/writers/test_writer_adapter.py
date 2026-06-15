import pytest
from writers.writer_adapter import WriterAdapter

@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_writer_adapter_write_table_modes(mock_writer_chain, mock_spark,
                                          mode):
    df = type("MockedDf", (), {"write": mock_writer_chain})()
    adapter = WriterAdapter(mock_spark, None)
    adapter.write(df, {"table": "target_table"}, {"mode": mode})
    mock_writer_chain.mode.assert_called_once_with(mode)
    mock_writer_chain.format.assert_called_once_with("delta")
    mock_writer_chain.saveAsTable.assert_called_once_with("target_table")
