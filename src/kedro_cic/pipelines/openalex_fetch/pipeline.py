from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openalex_fetch_author,
    openalex_fetch_work,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_openalex_work",
            func=openalex_fetch_work,
            inputs=[
                "params:openalex_fetch_options.institution_ror",
                "params:fetch_options.env",
            ],
            outputs=["raw/openalex/work#parquet","raw/openalex/work_dev#parquet"],
            ),
        node(
            name="fetch_openalex_author",
            func=openalex_fetch_author,
            inputs=[
                "params:openalex_fetch_options.institution_ror",
                "params:fetch_options.env",
            ],
            outputs="raw/openalex/author#parquet",
            )
    ], tags="openalex_fetch"
)
