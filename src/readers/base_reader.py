from abc import ABC, abstractmethod

class BaseReader(ABC):

    def __init__(
            self,
            spark,
            source_path,
            options=None
    ):
        self.spark = spark
        self.source_path = source_path
        self.options = options or {}

    @abstractmethod
    def read(self):
        pass
