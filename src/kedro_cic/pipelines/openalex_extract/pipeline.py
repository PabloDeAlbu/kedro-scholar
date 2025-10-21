from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openalex_extract,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="openalex_extract_work",
            func=openalex_extract,
            inputs=[
                "params:openalex_extract_options.institution_ror",
                "params:openalex_extract_options.work_filter",
                "params:openalex_extract_options.work_endpoint",
                "params:openalex_extract_options.env",
            ],
            outputs=["raw/openalex/work#parquet","raw/openalex/work_dev#parquet"],
        ),
        node(
            name="openalex_extract_author",
            func=openalex_extract,
            inputs=[
                "params:openalex_extract_options.institution_ror",
                "params:openalex_extract_options.author_filter",
                "params:openalex_extract_options.author_endpoint",
                "params:openalex_extract_options.env",
            ],
            outputs=[
                "raw/openalex/author#parquet",
                "raw/openalex/author_dev#parquet"
                ],
        ),
        node(
            name="openalex_extract_institution",
            func=openalex_extract,
            inputs=[
                "params:openalex_extract_options.institution_ror",
                "params:openalex_extract_options.institution_filter",
                "params:openalex_extract_options.institution_endpoint",
                "params:openalex_extract_options.env",
            ],
            outputs=[
                "raw/openalex/institution#parquet",
                "raw/openalex/institution_dev#parquet"
                ],
            ),
        node(
            name="openalex_extract_funder",
            func=openalex_extract,
            inputs=[
                "params:openalex_extract_options.institution_ror",
                "params:openalex_extract_options.funder_filter",
                "params:openalex_extract_options.funder_endpoint",
                "params:openalex_extract_options.env",
            ],
            outputs=[
                "raw/openalex/funder#parquet",
                "raw/openalex/funder_dev#parquet"
                ],
            )
    ], tags="openalex_extract"
)
