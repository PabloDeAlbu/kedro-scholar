"""
This is a boilerplate pipeline 'dspacedb'
generated using Kedro 0.19.8
"""
from datetime import date

def fetch_dspacedb(df_bitstream, df_bundle, df_bundle2bitstream, df_collection, df_collection2item, df_community, df_community2collection, df_community2community, df_doi, df_eperson, df_epersongroup, df_epersongroup2eperson, df_group2group, df_handle, df_item, df_item2bundle, df_metadatafieldregistry, df_metadataschemaregistry, df_metadatavalue):
    df_bitstream['load_datetime'] = date.today()
    df_bundle['load_datetime'] = date.today()
    df_bundle2bitstream['load_datetime'] = date.today()
    df_collection['load_datetime'] = date.today()
    df_collection2item['load_datetime'] = date.today()
    df_community['load_datetime'] = date.today()
    df_community2collection['load_datetime'] = date.today()
    df_community2community['load_datetime'] = date.today()
    df_doi['load_datetime'] = date.today()
    df_eperson['load_datetime'] = date.today()
    df_epersongroup['load_datetime'] = date.today()
    df_epersongroup2eperson['load_datetime'] = date.today()
    df_group2group['load_datetime'] = date.today()
    df_handle['load_datetime'] = date.today()
    df_item['load_datetime'] = date.today()
    df_item2bundle['load_datetime'] = date.today()
    df_metadatafieldregistry['load_datetime'] = date.today()
    df_metadataschemaregistry['load_datetime'] = date.today()
    df_metadatavalue['load_datetime'] = date.today()
    return df_bitstream, df_bundle, df_bundle2bitstream, df_collection, df_collection2item, df_community, df_community2collection, df_community2community, df_doi, df_eperson, df_epersongroup, df_epersongroup2eperson, df_group2group, df_handle, df_item, df_item2bundle, df_metadatafieldregistry, df_metadataschemaregistry, df_metadatavalue

def land_dspacedb(item, metadatavalue, metadatafieldregistry, metadataschemaregistry):
    df_item = item
    df_metadatavalue = metadatavalue
    df_metadatavalue['text_value'] = df_metadatavalue['text_value'].str.strip()
    df_metadatafieldregistry = metadatafieldregistry
    df_metadataschemaregistry = metadataschemaregistry
    return df_item, df_metadatavalue, df_metadatafieldregistry, df_metadataschemaregistry
