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
                "dspacedb/public/bitstream", 
                "dspacedb/public/bundle", 
                "dspacedb/public/bundle2bitstream", 
                "dspacedb/public/collection", 
                "dspacedb/public/collection2item", 
                "dspacedb/public/community", 
                "dspacedb/public/community2collection", 
                "dspacedb/public/community2community", 
                "dspacedb/public/doi", 
                "dspacedb/public/eperson", 
                "dspacedb/public/epersongroup", 
                "dspacedb/public/epersongroup2eperson", 
                "dspacedb/public/group2group", 
                "dspacedb/public/handle", 
                "dspacedb/public/item", 
                "dspacedb/public/item2bundle", 
                "dspacedb/public/metadatafieldregistry", 
                "dspacedb/public/metadataschemaregistry", 
                "dspacedb/public/metadatavalue",
            ],
            outputs=[
                "raw/public/bitstream#parquet", 
                "raw/public/bundle#parquet", 
                "raw/public/bundle2bitstream#parquet", 
                "raw/public/collection#parquet", 
                "raw/public/collection2item#parquet", 
                "raw/public/community#parquet", 
                "raw/public/community2collection#parquet", 
                "raw/public/community2community#parquet", 
                "raw/public/doi#parquet", 
                "raw/public/eperson#parquet", 
                "raw/public/epersongroup#parquet", 
                "raw/public/epersongroup2eperson#parquet", 
                "raw/public/group2group#parquet", 
                "raw/public/handle#parquet", 
                "raw/public/item#parquet", 
                "raw/public/item2bundle#parquet", 
                "raw/public/metadatafieldregistry#parquet", 
                "raw/public/metadataschemaregistry#parquet", 
                "raw/public/metadatavalue#parquet",
            ]
        ),
        node(
            name="land_dspacedb",
            func=land_dspacedb,
            inputs=[
                "raw/public/bitstream#parquet",
                "raw/public/bundle#parquet",
                "raw/public/bundle2bitstream#parquet",
                "raw/public/collection#parquet",
                "raw/public/collection2item#parquet",
                "raw/public/community#parquet",
                "raw/public/community2collection#parquet",
                "raw/public/community2community#parquet",
                "raw/public/doi#parquet",
                "raw/public/eperson#parquet",
                "raw/public/epersongroup#parquet",
                "raw/public/epersongroup2eperson#parquet",
                "raw/public/group2group#parquet",
                "raw/public/handle#parquet",
                "raw/public/item#parquet",
                "raw/public/item2bundle#parquet",
                "raw/public/metadatafieldregistry#parquet",
                "raw/public/metadataschemaregistry#parquet",
                "raw/public/metadatavalue#parquet",
            ],
            outputs=[
                "ldg/dspacedb/bitstream",
                "ldg/dspacedb/bundle",
                "ldg/dspacedb/bundle2bitstream",
                "ldg/dspacedb/collection",
                "ldg/dspacedb/collection2item",
                "ldg/dspacedb/community",
                "ldg/dspacedb/community2collection",
                "ldg/dspacedb/community2community",
                "ldg/dspacedb/doi",
                "ldg/dspacedb/eperson",
                "ldg/dspacedb/epersongroup",
                "ldg/dspacedb/epersongroup2eperson",
                "ldg/dspacedb/group2group",
                "ldg/dspacedb/handle",
                "ldg/dspacedb/item",
                "ldg/dspacedb/item2bundle",
                "ldg/dspacedb/metadatafieldregistry",
                "ldg/dspacedb/metadataschemaregistry",
                "ldg/dspacedb/metadatavalue",
            ]
        )
    ], tags="dspacedb"
)