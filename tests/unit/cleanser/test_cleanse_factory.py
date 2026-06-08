import pytest
from cleanser.cleanse_factory import CleanseFactory
from cleanser.cleanse_strategies import *

@pytest.mark.parametrize(
    "rule,expected_class",
    [
        ({"datatype": "integer", "ruletype": "anything"}, IntegerCleanse),
        ({"datatype": "string", "ruletype": "not_null"}, NullToDefaultCleanse),
        ({"datatype": "string", "ruletype": "regex"}, RegexCleanse),
        ({"datatype": "string", "ruletype": "case"}, CaseCleanse),
        ({"datatype": "date", "ruletype": "date_format"}, DateFormatCleanse),
        ({"datatype": "decimal", "ruletype": "precision"}, DecimalPrecisionCleanse),
    ],
)
def test_cleanse_factory_creates_strategy(rule, expected_class):
    assert isinstance(CleanseFactory.create(rule), expected_class)

def test_cleanse_factory_raises_for_unsupported_rule():
    with pytest.raises(ValueError, match="Unsupported cleanse rule"):
        CleanseFactory.create({"datatype": "string", "ruletype": "bad_rule"})