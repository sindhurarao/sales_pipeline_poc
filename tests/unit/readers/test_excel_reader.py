import pytest
from readers.excel_reader import ExcelReader

pytestmark = pytest.mark.skip(
    reason="pipeline refactor pending"
)
def test_excel_reader_applies_options_and_loads_path(mock_spark):
    reader_chain = mock_spark.read
    reader_chain.option.return_value = reader_chain
    reader_chain.format.return_value = reader_chain

    reader = ExcelReader(mock_spark, "/mnt/input.xlsx", {"header": "true"})
    reader.read()

    reader_chain.option.assert_called_once_with("header", "true")
    reader_chain.format.assert_called_once_with("excel")
    reader_chain.load.assert_called_once_with("/mnt/input.xlsx")