from transformation.join_strategies import *

class JoinFactory:

    STRATEGIES = {
        "broadcast": BroadcastJoinStrategy,
        "shuffle": ShuffleJoinStrategy
    }

    @classmethod
    def create(cls, join_type):
        strategy = cls.STRATEGIES.get(join_type)
        if strategy is None:
            raise ValueError(f"Unsupported join strategy {join_type}")
        return strategy()