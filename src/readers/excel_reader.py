from readers.base_reader import BaseReader

class ExcelReader(BaseReader):

    def read(self):
        reader = self.spark.read
        for key, value in self.options.items():
            reader = reader.option(key,value)
        return (
            reader
            .format("com.crealytics.spark.excel")
            .load(self.source_path)
        )
