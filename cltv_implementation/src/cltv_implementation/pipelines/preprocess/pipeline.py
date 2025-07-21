from kedro.pipeline import Pipeline, node, pipeline
from cltv_implementation.nodes.preprocess import (
    preprocess_transactional,
    preprocess_customer,
    preprocess_behavioral,
)

def create_pipeline() -> Pipeline:
    return pipeline([
        node(
    func=preprocess_transactional,
    inputs=dict(
        df="raw_transactional",
        selected_tables="params:selected_tables"
    ),
    outputs="preprocessed_transactional",
    name="preprocess_transactional_node"
),
        node(
            func=preprocess_customer,
            inputs=dict(
                df="raw_customer",
                selected_tables="params:selected_tables"
            ),
            outputs="preprocessed_customer",
            name="preprocess_customer_node"
        ),
        node(
            func=preprocess_behavioral,
            inputs=dict(
                df="raw_behavioral",
                selected_tables="params:selected_tables"
            ),
            outputs="preprocessed_behavioral",
            name="preprocess_behavioral_node"
        ),
    ])
