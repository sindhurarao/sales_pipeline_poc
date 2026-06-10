import pytest
from unittest.mock import MagicMock
from helpers.validation_helper import ValidationHelper

@pytest.fixture(scope="function")
def validator(mock_spark, mock_dbutils):
    return ValidationHelper(spark=mock_spark,dbutils=mock_dbutils)

@pytest.fixture(scope="function")
def validator_with_dbutils(mock_spark, mock_dbutils):
    return ValidationHelper(spark=mock_spark,dbutils=mock_dbutils)

def test_validate_table_exists_returns_true(validator,mock_spark,):
    mock_spark.catalog.tableExists.return_value = True
    result = validator.validate_table_exists("db.table")
    assert result is True
    mock_spark.catalog.tableExists.assert_called_once_with("db.table")

def test_validate_table_exists_raises_when_missing(validator,mock_spark,):
    mock_spark.catalog.tableExists.return_value = False
    with pytest.raises(Exception, match="Target table does not exist"):
        validator.validate_table_exists("db.missing")
    mock_spark.catalog.tableExists.assert_called_once_with("db.missing")

def test_validate_path_exists_returns_files(validator,mock_dbutils,):
    mock_file = MagicMock(size=10)
    mock_dbutils.fs.ls.return_value = [mock_file]
    files = validator.validate_path_exists("/input")
    assert files == [mock_file]
    mock_dbutils.fs.ls.assert_called_once_with("/input")

def test_validate_path_exists_raises_when_no_files_found(validator,mock_dbutils):
    mock_dbutils.fs.ls.return_value = []
    with pytest.raises(Exception, match="No files found"):
        validator.validate_path_exists("/empty")
    mock_dbutils.fs.ls.assert_called_once_with("/empty")

def test_validate_non_empty_files_filters_zero_size(validator):
    empty_file = MagicMock(size=0)
    non_empty_file = MagicMock(size=10)
    result = validator.validate_non_empty_files([empty_file, non_empty_file])
    assert result == [non_empty_file]

def test_validate_non_empty_files_raises_when_all_empty(validator):
    files = [MagicMock(size=0),MagicMock(size=0),]
    with pytest.raises(Exception, match="All files are empty"):
        validator.validate_non_empty_files(files)