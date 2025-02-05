from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_item_oai, 
    land_item_oai
    )


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_item_oai",
            func=fetch_item_oai,
            inputs=[
                "params:oai_fetch_options.base_url",
                "params:oai_fetch_options.context",
                "params:fetch_options.env",
            ],
            outputs="raw/oai/item#parquet"
        ),
        node(
            name="land_item_oai",
            func=land_item_oai,
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
    ], tags="oai"

)