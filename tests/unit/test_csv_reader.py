import pytest
from readers.csv_reader import CsvReader

@pytest.fixture(scope="class")
def csv_file_path(sample_df, tmp_path):
    path = str(tmp_path / "input_csv")

    (
        sample_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )

    return path

def test_read_csv_returns_dataframe(spark,sample_df,csv_file_path,):
        reader = CsvReader(
            spark=spark,
            source_path=csv_file_path,
            options={"header": "true"},
        )

        result = reader.read()

        assert result.count() == sample_df.count()
        assert result.columns == sample_df.columns

def test_read_csv_contents(spark,sample_df,csv_file_path,):

        reader = CsvReader(
            spark=spark,
            source_path=csv_file_path,
            options={"header": "true"},
        )

        result = reader.read()
        expected = {tuple(row) for row in sample_df.collect()}
        actual = {tuple(row) for row in result.collect()}

        assert actual == expected

def test_read_csv_without_options(spark,csv_file_path):

        reader = CsvReader(
            spark=spark,
            source_path=csv_file_path,
        )

        result = reader.read()

        assert result.count() > 0