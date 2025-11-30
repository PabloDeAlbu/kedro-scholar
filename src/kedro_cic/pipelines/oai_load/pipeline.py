from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    oai_load_identifiers,
    oai_load_records,
    oai_load_sets,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="oai_load_identifiers",
            func=oai_load_identifiers,
            inputs="raw/oai/identifiers#parquet",
            outputs=[
                "ldg/oai/identifiers",
                "ldg/oai/identifiers_sets",
            ],
        ),
        node(
            name="oai_load_records",
            func=oai_load_records,
            inputs=[
                "raw/oai/records#parquet"
            ],
            outputs=[
                "ldg/oai/records",
                "ldg/oai/records_creators",
                "ldg/oai/records_types",
                "ldg/oai/records_identifiers",
                "ldg/oai/records_languages",
                "ldg/oai/records_subjects",
                "ldg/oai/records_publishers",
                "ldg/oai/records_relations",
                "ldg/oai/records_rights",
            ],
        ),
        node(
            name="oai_load_sets",
            func=oai_load_sets,
            inputs="primary/oai/sets",
            outputs="ldg/oai/sets"
        ),
    ], tags="oai_load"
)
