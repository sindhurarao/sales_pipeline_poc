from readers.base_reader import BaseReader

class ExcelReader(BaseReader):

    def read(self):

        reader = self.spark.read

        for key, value in self.options.items():
            reader = reader.option(key,value)

        return (
            reader
            .format("excel")
            .load(self.source_path)
        )
