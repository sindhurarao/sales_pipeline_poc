from transformation.transformation_strategies import *

class TransformationFactory:
    STRATEGIES = {
        "rename": RenameTransformation,
        "expression": ExpressionTransformation,
        "select": SelectTransformation,
        "drop": DropTransformation,
        "cast": CastTransformation,
        "literal": LiteralTransformation,
        "round": RoundTransformation,
        "try_cast": TryCastTransformation,
        "alias": AliasTransformation
    }

    @classmethod
    def create(cls, transformation):
        transformation_type = transformation["type"]
        strategy = cls.STRATEGIES.get(transformation_type)
        if not strategy:
            raise ValueError(f"Unsupported transformation type: {transformation_type}")
        return strategy()