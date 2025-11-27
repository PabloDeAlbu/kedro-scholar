from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    oai_extract_records,
    oai_extract_sets,
    oai_filter_col,    
    oai_intermediate_sets,
    oai_extract_identifiers,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="oai_extract_sets",
            func=oai_extract_sets,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "params:oai_extract_options.env",
            ],
            outputs="raw/oai/sets"
        ),

        node(
            name="oai_intermediate_sets",
            func=oai_intermediate_sets,
            inputs="raw/oai/sets",
            outputs="intermediate/oai/sets"
        ),

        node(
            name="oai_filter_col",
            func=oai_filter_col,
            inputs=[
                "intermediate/oai/sets",
                "params:oai_extract_options.env",
            ],
            outputs="intermediate/oai/cols"
        ),

        node(
            name="oai_extract_identifiers",
            func=oai_extract_identifiers,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "params:oai_extract_options.env",
                "intermediate/oai/cols",
            ],
            outputs=["raw/oai/ids#parquet" , "raw/oai/ids_dev"]
        ),

        node(
            name="oai_extract_records",
            func=oai_extract_records,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "intermediate/oai/cols",
                "params:oai_extract_options.env",
            ],
            outputs=["raw/oai/records#parquet" , "raw/oai/records_dev"]
        ),
    ], tags="oai_extract"
)
