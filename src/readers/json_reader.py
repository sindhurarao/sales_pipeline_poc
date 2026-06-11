from readers.base_reader import BaseReader

class JsonReader(BaseReader):

    def read(self):
        reader = self.spark.read
        for key, value in self.options.items():
            reader = reader.option(key,value)
        return (
            reader
            .format("json")
            .load(self.source_path)
        )
