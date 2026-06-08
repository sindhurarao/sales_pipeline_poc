import pytest
from transformation.transformation_factory import TransformationFactory

@pytest.mark.parametrize(
    "transformation_type",
    ["rename", "expression", "select", "drop", "cast", "literal", "round"],
)
def test_transformation_factory_supported_types(transformation_type):
    strategy = TransformationFactory.create({"type": transformation_type})
    assert strategy is not None

def test_transformation_factory_raises_for_unsupported_type():
    with pytest.raises(ValueError, match="Unsupported transformation type"):
        TransformationFactory.create({"type": "bad"})