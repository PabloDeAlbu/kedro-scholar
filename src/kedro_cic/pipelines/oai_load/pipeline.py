from kedro.pipeline import Node, Pipeline
from .nodes import (
    oai_load_identifiers,
    oai_load_records,
    oai_load_sets,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            name="oai_load_identifiers",
            func=oai_load_identifiers,
            inputs="raw/oai/identifiers#parquet",
            outputs=[
                "ldg/oai/identifiers",
                "ldg/oai/identifiers_sets",
            ],
        ),
        Node(
            name="oai_load_records",
            func=oai_load_records,
            inputs=[
                "raw/oai/records#parquet",
                "params:oai_load_options.env",                
            ],
            outputs=[
                "ldg/oai/records",
                "ldg/oai/record_creators",
                "ldg/oai/record_descriptions",
                "ldg/oai/record_types",
                "ldg/oai/record_identifiers",
                "ldg/oai/record_languages",
                "ldg/oai/record_subjects",
                "ldg/oai/record_publishers",
                "ldg/oai/record_relations",
                "ldg/oai/record_rights",
                "ldg/oai/record_formats",
                "ldg/oai/record_sets",
           ],
        ),
        Node(
            name="oai_load_sets",
            func=oai_load_sets,
            inputs="raw/oai/sets",
            outputs="ldg/oai/sets"
        ),
    ], tags="oai_load")
