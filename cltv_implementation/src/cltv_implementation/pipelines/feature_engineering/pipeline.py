from kedro.pipeline import Pipeline, node, pipeline
from cltv_implementation.nodes.feature_engineering import merge_user_level

def create_pipeline() -> Pipeline:
    return pipeline([
        node(
            func=merge_user_level,
            inputs=dict(
                transactional="transaction_with_bgnbd",
                customer="preprocessed_customer",
                behavioral="preprocessed_behavioral",
                selected_tables="params:selected_tables"
            ),
            outputs="user_level_table",
            name="merge_user_level_node"
        )
    ])
