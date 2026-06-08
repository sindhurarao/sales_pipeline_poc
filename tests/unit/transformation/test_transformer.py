from transformation.transformer import Transformer

def test_transformer_applies_transformations_in_order(sample_df):
    result = Transformer().apply(
        sample_df,
        [
            {"type": "rename", "source_column": "name", "target_column": "customer_name"},
            {"type": "select", "columns": ["id", "customer_name"]},
        ],
    )
    assert result.columns == ["id", "customer_name"]
