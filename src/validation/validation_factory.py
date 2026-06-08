from validation.validation_strategies import *

class ValidationFactory:
    STRATEGIES = {
        "not_null": NotNullValidation,
        "min": MinValidation,
        "range": RangeValidation,
        "duplicate_key": DuplicateKeyValidation,
        "ref_integrity": RefIntegrityValidation,
        "conflicting_key": ConflictingKeyValidation
    }

    @classmethod
    def create(cls, rule):
        strategy = cls.STRATEGIES.get(rule["ruletype"])
        if not strategy:
            raise ValueError(f"Unsupported validation rule: {rule['ruletype']}")
        return strategy()