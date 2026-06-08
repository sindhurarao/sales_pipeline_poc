import pytest
from transformation.join_factory import JoinFactory
from transformation.join_strategies import *

@pytest.mark.parametrize(
    "join_type,expected_class",
    [
        ("broadcast", BroadcastJoinStrategy),
        ("shuffle", ShuffleJoinStrategy),
    ],
)
def test_join_factory_creates_supported_strategy(join_type, expected_class):
    strategy = JoinFactory.create(join_type)
    assert isinstance(strategy, expected_class)

def test_join_factory_raises_for_unsupported_strategy():
    with pytest.raises(ValueError, match="Unsupported join strategy invalid"):
        JoinFactory.create("invalid")