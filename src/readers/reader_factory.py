from readers.csv_reader import CsvReader
from readers.excel_reader import ExcelReader

class ReaderFactory:

    @staticmethod
    def get_reader(
            spark,
            file_format,
            source_path,
            options=None
    ):

        file_format = file_format.upper()

        readers = {
            "CSV": CsvReader,
            "EXCEL": ExcelReader
        }

        if file_format not in readers:
            raise ValueError(f"Unsupported format: {file_format}")

        return readers[file_format](
            spark,
            source_path,
            options
        )
