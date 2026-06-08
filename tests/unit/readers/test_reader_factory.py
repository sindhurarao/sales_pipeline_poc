import pytest
from readers.reader_factory import ReaderFactory
from readers.csv_reader import CsvReader
from readers.excel_reader import ExcelReader

@pytest.mark.parametrize(
    "file_format,expected_class",
    [
        ("csv", CsvReader),
        ("CSV", CsvReader),
        ("excel", ExcelReader),
        ("EXCEL", ExcelReader),
    ],
)
def test_get_reader_supported_formats(mock_spark, file_format, expected_class):
    reader = ReaderFactory.get_reader(mock_spark, file_format, "/tmp/file", {"header": True})

    assert isinstance(reader, expected_class)
    assert reader.spark == mock_spark
    assert reader.source_path == "/tmp/file"
    assert reader.options == {"header": True}


def test_get_reader_raises_for_unsupported_format(mock_spark):
    with pytest.raises(ValueError, match="Unsupported format"):
        ReaderFactory.get_reader(mock_spark, "json", "/tmp/file")
