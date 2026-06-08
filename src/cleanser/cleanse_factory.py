from cleanser.cleanse_strategies import *

class CleanseFactory:
    STRATEGIES = {
        "not_null": NullToDefaultCleanse,
        "regex": RegexCleanse,
        "case": CaseCleanse,
        "date_format": DateFormatCleanse,
        "precision": DecimalPrecisionCleanse
    }

    @classmethod
    def create(cls, rule):
        if rule["datatype"] == "integer":
            return IntegerCleanse()
        strategy = cls.STRATEGIES.get(rule["ruletype"])
        if not strategy:
            raise ValueError(f"Unsupported cleanse rule: {rule['ruletype']}")
        return strategy()