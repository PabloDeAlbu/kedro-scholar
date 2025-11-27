from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    oai_extract_identifiers,
    oai_extract_records,
    oai_extract_sets,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="oai_extract_sets",
            func=oai_extract_sets,
            inputs=[
                "params:oai_extract_set_options.base_url",
                "params:oai_extract_set_options.context",
                "params:oai_extract_options.env",
            ],
            outputs="raw/oai/sets#csv"
        ),

        node(
            name="oai_extract_identifiers",
            func=oai_extract_identifiers,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "raw/oai/set#csv",
                "params:oai_extract_options.env",
            ],
            outputs=["raw/oai/item#parquet" , "raw/oai/item_dev#csv"]
        ),

        node(
            name="oai_extract_records",
            func=oai_extract_records,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "raw/oai/sets#csv",
                "params:extract_options.env",
            ],
            outputs=["raw/oai/item#parquet" , "raw/oai/item_dev#csv"]
        ),
    ], tags="oai_extract"
)
