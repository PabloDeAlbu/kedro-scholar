from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openaire_load_researchproduct,
    openaire_load_researchproduct_authors,
    openaire_load_researchproduct_collectedfrom,
    openaire_load_researchproduct_contributors,
    # openaire_load_researchproduct_descriptions,
    openaire_load_researchproduct_instances,
    openaire_load_researchproduct_organizations,
    openaire_load_researchproduct_originalid,
    openaire_load_researchproduct_pids,
    openaire_load_researchproduct_sources,
    openaire_load_researchproduct_subjects,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="openaire_load_researchproduct",
            func=openaire_load_researchproduct,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct"
        ),
        node(
            name="openaire_load_researchproduct_authors",
            func=openaire_load_researchproduct_authors,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_authors",
        ),
        node(
            name="openaire_load_researchproduct_collectedfrom",
            func=openaire_load_researchproduct_collectedfrom,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_collectedfrom"
        ),
        node(
            name="openaire_load_researchproduct_contributors",
            func=openaire_load_researchproduct_contributors,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_contributors"
        ),
        # node(
        #     name="openaire_load_researchproduct_descriptions",
        #     func=openaire_load_researchproduct_descriptions,
        #     inputs="raw/openaire/researchproduct#parquet",
        #     outputs="ldg/openaire/researchproduct_descriptions"
        # ),
        node(
            name="openaire_load_researchproduct_instances",
            func=openaire_load_researchproduct_instances,
            inputs="raw/openaire/researchproduct#parquet",
            outputs=[
                "ldg/openaire/researchproduct_instances",
                "ldg/openaire/researchproduct_alternateidentifiers",
            ]
        ),
        node(
            name="openaire_load_researchproduct_organizations",
            func=openaire_load_researchproduct_organizations,
            inputs="raw/openaire/researchproduct#parquet",
            outputs=[
                "ldg/openaire/organization",
                "ldg/openaire/researchproduct_organizations",
                "ldg/openaire/organization_pids",
            ]
        ),
        node(
            name="openaire_load_researchproduct_originalid",
            func=openaire_load_researchproduct_originalid,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_originalid"
        ),
        node(
            name="openaire_load_researchproduct_pids",
            func=openaire_load_researchproduct_pids,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_pids"
        ),
        node(
            name="openaire_load_researchproduct_sources",
            func=openaire_load_researchproduct_sources,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_sources"
        ),
        node(
            name="openaire_load_researchproduct_subjects",
            func=openaire_load_researchproduct_subjects,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_subjects"
        ),
    ], tags="openaire_load"
)
