import pytest
from validation.validation_factory import ValidationFactory

@pytest.mark.parametrize(
    "ruletype",
    ["not_null", "min", "range", "duplicate_key", "conflicting_key"],
)
def test_validation_factory_supported_rules(ruletype):
    strategy = ValidationFactory.create({"ruletype": ruletype})
    assert strategy is not None

def test_validation_factory_raises_for_unsupported_rule():
    with pytest.raises(ValueError, match="Unsupported validation rule"):
        ValidationFactory.create({"ruletype": "bad"})
