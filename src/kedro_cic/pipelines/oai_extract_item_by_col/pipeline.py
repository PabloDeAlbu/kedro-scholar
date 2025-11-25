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
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "dv_oai/dim_oai_set",
                "params:extract_options.env",
            ],
            outputs=["raw/oai/item#parquet" , "raw/oai/item_dev#csv"]
        ),
    ], tags="oai_extract_by_col"
)
