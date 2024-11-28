from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    fetch_work_openalex,
    fetch_work_dimensions_openalex,
    land_work_openalex,
    land_work2authorship_openalex,
    land_work2location_openalex,
    land_work_dimensions_openalex,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_work_dimensions_openalex",
            func=fetch_work_dimensions_openalex,
            inputs=[],
            outputs=[
                "raw/openalex/worktype#parquet",
                "raw/openalex/language#parquet",
                "raw/openalex/license#parquet",
                ],
            ),
        node(
            name="land_work_dimensions_openalex",
            func=land_work_dimensions_openalex,
            inputs=[
                "raw/openalex/worktype#parquet",
                "raw/openalex/language#parquet",
                "raw/openalex/license#parquet",
            ],
            outputs=[
                "ldg/openalex/worktype",
                "ldg/openalex/language",
                "ldg/openalex/license",
                ],
            ),
        node(
            name="fetch_work_openalex",
            func=fetch_work_openalex,
            inputs=[
                "params:openalex_fetch_options.institution_ror",
                "params:fetch_options.env",
            ],
            outputs="raw/openalex/work#parquet",
            ),
        node(
            name="land_work_openalex",
            func=land_work_openalex,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/work'
            ),
        node(
            name="land_work2authorship_openalex",
            func=land_work2authorship_openalex,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/work2authorship'
            ),
        node(
            name="land_work2location_openalex",
            func=land_work2location_openalex,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/work2location'
            )
], tags="openalex"
)
