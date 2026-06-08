import pytest
from unittest.mock import MagicMock, patch
from helpers.validation_helper import ValidationHelper

def test_validate_table_exists_returns_true(mock_spark):
    mock_spark.catalog.tableExists.return_value = True
    assert ValidationHelper(mock_spark).validate_table_exists("db.table") is True


def test_validate_table_exists_raises_when_missing(mock_spark):
    mock_spark.catalog.tableExists.return_value = False
    with pytest.raises(Exception, match="Target table does not exist"):
        ValidationHelper(mock_spark).validate_table_exists("db.missing")


@patch("helpers.validation_helper.dbutils")
def test_validate_path_exists_returns_files(mock_dbutils, mock_spark):
    mock_dbutils.fs.ls.return_value = [MagicMock(size=10)]
    files = ValidationHelper(mock_spark).validate_path_exists("/input")
    assert len(files) == 1


def test_validate_non_empty_files_filters_zero_size(mock_spark):
    files = [MagicMock(size=0), MagicMock(size=10)]
    result = ValidationHelper(mock_spark).validate_non_empty_files(files)
    assert len(result) == 1
    assert result[0].size == 10


def test_validate_non_empty_files_raises_when_all_empty(mock_spark):
    files = [MagicMock(size=0)]
    with pytest.raises(Exception, match="All files are empty"):
        ValidationHelper(mock_spark).validate_non_empty_files(files)