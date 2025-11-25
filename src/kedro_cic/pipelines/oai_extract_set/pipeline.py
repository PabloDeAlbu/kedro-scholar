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
                "params:oai_extract_set_options.base_url",
                "params:oai_extract_set_options.context",
                "params:extract_options.env",
            ],
            outputs="ldg/oai/set"
        ),
    ], tags="oai_extract"

)