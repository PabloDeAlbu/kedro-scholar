from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    oai_extract_item_by_col,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="oai_extract_item_by_col",
            func=oai_extract_item_by_col,
            inputs=[
                "params:oai_extract_item_by_col_options.base_url",
                "params:oai_extract_item_by_col_options.context",
                "dv_oai/dim_oai_col",
                "params:extract_options.env",
                "params:oai_extract_item_by_col_options.col_iteration_limit",
            ],
            outputs=["raw/oai/item#parquet" , "raw/oai/item_dev#csv"]
        ),
    ], tags="oai_extract_by_col"
)
