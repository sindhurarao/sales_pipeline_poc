from readers.base_reader import BaseReader

class CsvReader(BaseReader):

    def read(self):
        reader = self.spark.read
        for key, value in self.options.items():
            reader = reader.option(key,value)
        return (
            reader
            .format("csv")
            .load(self.source_path)
        )
