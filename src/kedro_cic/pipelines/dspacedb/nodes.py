"""
This is a boilerplate pipeline 'dspacedb'
generated using Kedro 0.19.8
"""
from datetime import date

def fetch_dspacedb(df_bitstream, df_bundle, df_bundle2bitstream, df_collection, df_collection2item, df_community, df_community2collection, df_community2community, df_doi, df_eperson, df_epersongroup, df_epersongroup2eperson, df_group2group, df_handle, df_item, df_item2bundle, df_metadatafieldregistry, df_metadataschemaregistry, df_metadatavalue):

    df_bitstream['uuid'] = df_item['uuid'].astype(str)
    df_bitstream['load_datetime'] = date.today()

    df_bundle['uuid'] = df_bundle['uuid'].astype(str)
    df_bundle['primary_bitstream_id'] = df_bundle['primary_bitstream_id'].astype(str)
    df_bundle['load_datetime'] = date.today()

    df_bundle2bitstream['load_datetime'] = date.today()
    df_bundle2bitstream['bundle_id'] = df_bundle2bitstream['bundle_id'].astype(str)
    df_bundle2bitstream['bitstream_id'] = df_bundle2bitstream['bitstream_id'].astype(str)

    df_collection['load_datetime'] = date.today()
    df_collection['uuid'] = df_collection['uuid'].astype(str)
    df_collection['submitter'] = df_collection['submitter'].astype(str)
    df_collection['admin'] = df_collection['admin'].astype(str)
    df_collection.drop(columns=['template_item_id','logo_bitstream_id'], inplace=True)

    df_collection2item['collection_id'] = df_collection2item['collection_id'].astype(str)
    df_collection2item['item_id'] = df_collection2item['item_id'].astype(str)
    df_collection2item['load_datetime'] = date.today()

    df_community['uuid'] = df_community['uuid'].astype(str)
    df_community['admin'] = df_community['admin'].astype(str)
    df_community.drop(columns=['logo_bitstream_id'], inplace=True)
    df_community['load_datetime'] = date.today()

    df_community2collection['collection_id'] = df_community2collection['collection_id'].astype(str)
    df_community2collection['community_id'] = df_community2collection['community_id'].astype(str)
    df_community2collection['load_datetime'] = date.today()

    df_community2community['parent_comm_id'] = df_community2community['parent_comm_id'].astype(str)
    df_community2community['child_comm_id'] = df_community2community['child_comm_id'].astype(str)
    df_community2community['load_datetime'] = date.today()

    df_doi['dspace_object'] = df_doi['dspace_object'].astype(str)
    df_doi['load_datetime'] = date.today()

    df_eperson['uuid'] = df_eperson['uuid'].astype(str)
    df_eperson['load_datetime'] = date.today()

    df_epersongroup['load_datetime'] = date.today()
    df_epersongroup['uuid'] = df_epersongroup['uuid'].astype(str)

    df_epersongroup2eperson['eperson_group_id'] = df_epersongroup2eperson['eperson_group_id'].astype(str)
    df_epersongroup2eperson['eperson_id'] = df_epersongroup2eperson['eperson_id'].astype(str)
    df_epersongroup2eperson['load_datetime'] = date.today()

    df_group2group['parent_id'] = df_group2group['parent_id'].astype(str)
    df_group2group['child_id'] = df_group2group['child_id'].astype(str)
    df_group2group['load_datetime'] = date.today()

    df_handle['resource_id'] = df_handle['resource_id'].astype(str)
    df_handle['load_datetime'] = date.today()

    # convert df_item dtypes
    df_item = df_item.convert_dtypes()
    df_item['uuid'] = df_item['uuid'].astype(str)
    df_item['item_id'] = df_item['item_id'].astype(str)
    df_item['submitter_id'] = df_item['submitter_id'].astype(str)
    df_item['owning_collection'] = df_item['owning_collection'].astype(str)
    df_item['load_datetime'] = date.today()

    df_item2bundle['bundle_id'] = df_item2bundle['bundle_id'].astype(str)
    df_item2bundle['item_id'] = df_item2bundle['item_id'].astype(str)
    df_item2bundle['load_datetime'] = date.today()

    # convert df_metadatavalue dtypes
    df_metadatavalue = df_metadatavalue.convert_dtypes()
    df_metadatavalue['dspace_object_id'] = df_metadatavalue['dspace_object_id'].astype(str)
    df_metadatavalue['load_datetime'] = date.today()

    # convert df_metadatafieldregistry dtypes
    df_metadatafieldregistry = df_metadatafieldregistry.convert_dtypes()
    df_metadatafieldregistry['load_datetime'] = date.today()

    # convert df_metadataschemaregistry dtypes
    df_metadataschemaregistry = df_metadataschemaregistry.convert_dtypes()
    df_metadataschemaregistry['load_datetime'] = date.today()

    return df_bitstream, df_bundle, df_bundle2bitstream, df_collection, df_collection2item, df_community, df_community2collection, df_community2community, df_doi, df_eperson, df_epersongroup, df_epersongroup2eperson, df_group2group, df_handle, df_item, df_item2bundle, df_metadatafieldregistry, df_metadataschemaregistry, df_metadatavalue

def land_dspacedb(df_bitstream, df_bundle, df_bundle2bitstream, df_collection, df_collection2item, df_community, df_community2collection, df_community2community, df_doi, df_eperson, df_epersongroup, df_epersongroup2eperson, df_group2group, df_handle, df_item, df_item2bundle, df_metadatafieldregistry, df_metadataschemaregistry, df_metadatavalue):
    return df_bitstream, df_bundle, df_bundle2bitstream, df_collection, df_collection2item, df_community, df_community2collection, df_community2community, df_doi, df_eperson, df_epersongroup, df_epersongroup2eperson, df_group2group, df_handle, df_item, df_item2bundle, df_metadatafieldregistry, df_metadataschemaregistry, df_metadatavalue

