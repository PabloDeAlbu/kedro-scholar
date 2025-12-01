from kedro.pipeline import Node, Pipeline
from .nodes import (
    oai_extract_records,
    oai_extract_sets,
    oai_filter_col,    
    oai_intermediate_sets,
    oai_extract_identifiers_by_sets,
)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            name="oai_extract_sets",
            func=oai_extract_sets,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "params:oai_extract_options.env",
            ],
            outputs="raw/oai/sets"
        ),

        Node(
            name="oai_intermediate_sets",
            func=oai_intermediate_sets,
            inputs="raw/oai/sets",
            outputs="intermediate/oai/sets"
        ),

        Node(
            name="oai_filter_col",
            func=oai_filter_col,
            inputs=[
                "intermediate/oai/sets",
                "params:oai_extract_options.env",
            ],
            outputs="intermediate/oai/cols"
        ),

        Node(
            name="oai_extract_identifiers_by_sets",
            func=oai_extract_identifiers_by_sets,
            inputs=[
                "params:oai_extract_options.base_url",
                "params:oai_extract_options.context",
                "params:oai_extract_options.env",
                "intermediate/oai/cols",
                "params:oai_extract_options.iteration_limit",
            ],
            outputs=["primary/oai/identifiers#parquet","primary/oai/cols#parquet" , "primary/oai/identifiers_dev"]
        ),

        # Node(
        #     name="oai_extract_records",
        #     func=oai_extract_records,
        #     inputs=[
        #         "params:oai_extract_options.base_url",
        #         "params:oai_extract_options.context",
        #         "intermediate/oai/cols",
        #         "params:oai_extract_options.env",
        #     ],
        #     outputs=["raw/oai/records#parquet" , "raw/oai/records_dev"]
        # ),
    ], tags="oai_extract")
