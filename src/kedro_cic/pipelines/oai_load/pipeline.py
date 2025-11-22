from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    oai_load_set,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="oai_load_set",
            func=oai_load_set,
            inputs="raw/oai/set#csv",
            outputs="ldg/oai/set"
        ),
    ], tags="oai_load"
)
