from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openalex_land_author,
    openalex_land_author_affiliation,
    openalex_land_author_topic,
    openalex_land_work,
    openalex_land_work_authorships,
    openalex_land_work_concept,
    openalex_land_work_corresponding_author_ids,
    openalex_land_work_referenced_works,
    openalex_land_work_topics,
    openalex_land_work_location,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="openalex_land_author",
            func=openalex_land_author,
            inputs='raw/openalex/author#parquet',
            outputs='ldg/openalex/author'
            ),
        node(
            name="openalex_land_author_affiliation",
            func=openalex_land_author_affiliation,
            inputs='raw/openalex/author#parquet',
            outputs='ldg/openalex/map_author_affiliation'
            ),
        node(
            name="openalex_land_author_topic",
            func=openalex_land_author_topic,
            inputs='raw/openalex/author#parquet',
            outputs='ldg/openalex/map_author_topic'
            ),
        node(
            name="openalex_land_work",
            func=openalex_land_work,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/work'
            ),
        node(
            name="openalex_land_work_authorships",
            func=openalex_land_work_authorships,
            inputs='raw/openalex/work#parquet',
            outputs=['ldg/openalex/map_work_author', 'ldg/openalex/map_work_institution', 'ldg/openalex/map_author_institution']
            ),
        node(
            name="openalex_land_work_concept",
            func=openalex_land_work_concept,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_concepts'
            ),
        node(
            name="openalex_land_work_corresponding_author_ids",
            func=openalex_land_work_corresponding_author_ids,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_corresponding_author_ids'
            ),
        node(
            name="openalex_land_work_referenced_works",
            func=openalex_land_work_referenced_works,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_referenced_works'
            ),
        node(
            name="openalex_land_work_topics",
            func=openalex_land_work_topics,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_topics'
            ),
        node(
            name="openalex_land_work_location",
            func=openalex_land_work_location,
            inputs='raw/openalex/work#parquet',
            outputs='ldg/openalex/map_work_location'
            )
], tags="openalex_land"
)
