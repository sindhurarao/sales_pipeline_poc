import pytest
from readers.base_reader import BaseReader

class DummyReader(BaseReader):
    def read(self):
        return "data"

def test_base_reader_cannot_be_instantiated(spark):
    with pytest.raises(
            TypeError,
            match="Can't instantiate abstract class"
    ):
        BaseReader(
            spark=spark,
            source_path="/tmp/data"
        )

def test_base_reader_initializes_attributes(spark):
    reader = DummyReader(
        spark=spark,
        source_path="/tmp/data",
        options={"header": "true"}
    )

    assert reader.spark is spark
    assert reader.source_path == "/tmp/data"
    assert reader.options == {"header": "true"}

def test_base_reader_defaults_options_to_empty_dict(spark):
    reader = DummyReader(
        spark=spark,
        source_path="/tmp/data"
    )

    assert reader.options == {}

def test_read_is_implemented_by_subclass(spark):
    reader = DummyReader(
        spark=spark,
        source_path="/tmp/data"
    )

    assert reader.read() == "data"