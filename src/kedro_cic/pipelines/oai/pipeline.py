from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_oai_items,
    fetch_oai_item_by_set,
    fetch_oai_sets,
    land_items_oai,
    land_oai_item_by_set,
    )


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_oai_items",
            func=fetch_oai_items,
            inputs=[
                "params:oai_fetch_options.base_url",
                "params:oai_fetch_options.context",
                "params:fetch_options.env",
            ],
            outputs="raw/oai/item#parquet"
        ),
        node(
            name="fetch_oai_sets",
            func=fetch_oai_sets,
            inputs=[
                "params:oai_fetch_options.base_url",
                "params:oai_fetch_options.context",
                "params:fetch_options.env",
            ],
            outputs="raw/oai/item#csv"
        ),
        node(
            name="fetch_oai_item_by_set",
            func=fetch_oai_item_by_set,
            inputs=[
                "params:oai_fetch_options.base_url",
                "params:oai_fetch_options.context",
                "params:oai_fetch_options.set_id",
                "params:oai_fetch_options.metadata_format",
                "params:fetch_options.env",
            ],
            outputs=[
                "raw/oai/item_by_set#parquet",
                "raw/oai/item_by_set_dev#csv"
                ]
        ),
        node(
            name="land_items_oai",
            func=land_items_oai,
            inputs="raw/oai/item#parquet",
            outputs=[
                "ldg/oai/item",
                "ldg/oai/item2creator",
                "ldg/oai/item2contributor",
                "ldg/oai/item2language",
                "ldg/oai/item2subject",
                "ldg/oai/item2relation"
            ],
        ),
        node(
            name="land_oai_item_by_set",
            func=land_oai_item_by_set,
            inputs="raw/oai/item_by_set#parquet",
            outputs=[
                "ldg/oai/item_by_set",
                "ldg/oai/item_creator",
                "ldg/oai/item_language",
                "ldg/oai/item_subject",
                "ldg/oai/item_relation",
                "ldg/oai/item_publisher"
            ],
        ),
    ], tags="oai"

)