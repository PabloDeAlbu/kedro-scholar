from kedro.pipeline import Node, Pipeline
from .nodes import (
    oai_load_identifiers,
    oai_load_item,
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
            name="oai_load_item",
            func=oai_load_item,
            inputs=[
                "raw/oai/item#parquet"
            ],
            outputs=[
                "ldg/oai/item",
                "ldg/oai/item_creators",
                "ldg/oai/item_types",
                "ldg/oai/item_identifiers",
                "ldg/oai/item_languages",
                "ldg/oai/item_subjects",
                "ldg/oai/item_publishers",
                "ldg/oai/item_relations",
                "ldg/oai/item_rights",
            ],
        ),
    ], tags="oai_load")
