from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    oai_extract_sets,
    )


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="oai_extract_sets",
            func=oai_extract_sets,
            inputs=[
                "params:oai_fetch_options.base_url",
                "params:oai_fetch_options.context",
                "params:fetch_options.env",
            ],
            outputs="raw/oai/set#csv"
        ),
    ], tags="oai_extract"

)