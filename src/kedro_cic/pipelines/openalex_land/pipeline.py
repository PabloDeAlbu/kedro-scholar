from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    land_openalex_author,
    land_openalex_author_affiliation,
    land_openalex_author_topic,
    land_openalex_work,
    land_openalex_work_authorships,
    land_openalex_work_concept,
    land_openalex_work_corresponding_author_ids,
    land_openalex_work_referenced_works,
    land_openalex_work_topics,
    land_openalex_work_location,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="land_openalex_author",
            func=land_openalex_author,
            inputs='raw/openalex/author#parquet',
            outputs='ldg/openalex/author'
            ),
        node(
            name="land_openalex_author_affiliation",
            func=land_openalex_author_affiliation,
            inputs='raw/openalex/author#parquet',
            outputs='ldg/openalex/map_author_affiliation'
            ),
        node(
            name="land_openalex_author_topic",
            func=land_openalex_author_topic,
            inputs='raw/openalex/author#parquet',
            outputs='ldg/openalex/map_author_topic'
            ),
        node(
            name="land_openalex_work",
            func=land_openalex_work,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/work'
            ),
        node(
            name="land_openalex_work_authorships",
            func=land_openalex_work_authorships,
            inputs='raw/openalex/work#parquet',
            outputs=['ldg/openalex/map_work_author', 'ldg/openalex/map_work_institution', 'ldg/openalex/map_author_institution']
            ),
        node(
            name="land_openalex_work_concept",
            func=land_openalex_work_concept,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_concepts'
            ),
        node(
            name="land_openalex_work_corresponding_author_ids",
            func=land_openalex_work_corresponding_author_ids,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_corresponding_author_ids'
            ),
        node(
            name="land_openalex_work_referenced_works",
            func=land_openalex_work_referenced_works,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_referenced_works'
            ),
        node(
            name="land_openalex_work_topics",
            func=land_openalex_work_topics,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_topics'
            ),
        node(
            name="land_openalex_work_location",
            func=land_openalex_work_location,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_location'
            )
], tags="openalex_land"
)
