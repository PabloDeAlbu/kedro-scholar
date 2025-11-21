from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    openaire_land_researchproduct,
    openaire_land_researchproduct_authors,
    openaire_land_researchproduct_collectedfrom,
    openaire_land_researchproduct_contributors,
    # openaire_land_researchproduct_descriptions,
    openaire_land_researchproduct_instances,
    openaire_land_researchproduct_organizations,
    openaire_land_researchproduct_originalid,
    openaire_land_researchproduct_pids,
    openaire_land_researchproduct_sources,
    openaire_land_researchproduct_subjects,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="openaire_land_researchproduct",
            func=openaire_land_researchproduct,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy"
        ),
        node(
            name="openaire_land_researchproduct_authors",
            func=openaire_land_researchproduct_authors,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_authors",
        ),
        node(
            name="openaire_land_researchproduct_collectedfrom",
            func=openaire_land_researchproduct_collectedfrom,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_collectedfrom"
        ),
        node(
            name="openaire_land_researchproduct_contributors",
            func=openaire_land_researchproduct_contributors,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_contributors"
        ),
        # node(
        #     name="openaire_land_researchproduct_descriptions",
        #     func=openaire_land_researchproduct_descriptions,
        #     inputs="raw/openaire/researchproduct#parquet",
        #     outputs="ldg/openaire/researchproduct_legacy_descriptions"
        # ),
        node(
            name="openaire_land_researchproduct_instances",
            func=openaire_land_researchproduct_instances,
            inputs="raw/openaire/researchproduct#parquet",
            outputs=[
                "ldg/openaire/researchproduct_legacy_instances",
                "ldg/openaire/researchproduct_legacy_alternateidentifiers",
            ]
        ),
        node(
            name="openaire_land_researchproduct_organizations",
            func=openaire_land_researchproduct_organizations,
            inputs="raw/openaire/researchproduct#parquet",
            outputs=[
                "ldg/openaire/organization_legacy",
                "ldg/openaire/researchproduct_legacy_organizations",
                "ldg/openaire/organization_legacy_pids",
            ]
        ),
        node(
            name="openaire_land_researchproduct_originalid",
            func=openaire_land_researchproduct_originalid,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_originalid"
        ),
        node(
            name="openaire_land_researchproduct_pids",
            func=openaire_land_researchproduct_pids,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_pids"
        ),
        node(
            name="openaire_land_researchproduct_sources",
            func=openaire_land_researchproduct_sources,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_sources"
        ),
        node(
            name="openaire_land_researchproduct_subjects",
            func=openaire_land_researchproduct_subjects,
            inputs="raw/openaire/researchproduct#parquet",
            outputs="ldg/openaire/researchproduct_legacy_subjects"
        ),
    ], tags="openaire_land"
)
