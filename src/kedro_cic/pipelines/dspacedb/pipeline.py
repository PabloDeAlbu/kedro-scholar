"""
This is a boilerplate pipeline 'dspacedb'
generated using Kedro 0.19.8
"""


from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_dspacedb, land_dspacedb


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            name="fetch_dspacedb",
            func=fetch_dspacedb,
            inputs=[
                "dspacedb/public/item",
                "dspacedb/public/metadatavalue",
                "dspacedb/public/metadatafieldregistry",
                "dspacedb/public/metadataschemaregistry",
                #dspacedb/public/community,
                #dspacedb/public/collection,
            ],
            outputs=[
                "raw/dspacedb/item#parquet",
                "raw/dspacedb/metadatavalue#parquet",
                "raw/dspacedb/metadatafieldregistry#parquet",
                "raw/dspacedb/metadataschemaregistry#parquet"
                ]
        ),
        node(
            name="land_dspacedb",
            func=land_dspacedb,
            inputs=[
                "raw/dspacedb/item#parquet",
                "raw/dspacedb/metadatavalue#parquet",
                "raw/dspacedb/metadatafieldregistry#parquet",
                "raw/dspacedb/metadataschemaregistry#parquet"
            ],
            outputs=[
                "ldg/dspacedb/item",
                "ldg/dspacedb/metadatavalue",
                "ldg/dspacedb/metadatafieldregistry",
                "ldg/dspacedb/metadataschemaregistry"
            ],
        ),
    ], tags="dspacedb"
)