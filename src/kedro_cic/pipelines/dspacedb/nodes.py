"""
This is a boilerplate pipeline 'dspacedb'
generated using Kedro 0.19.8
"""
from datetime import date

def fetch_dspacedb(item, metadatavalue, metadatafieldregistry, metadataschemaregistry):
    df_item = item
    df_metadatavalue = metadatavalue
    df_metadatafieldregistry = metadatafieldregistry
    df_metadataschemaregistry = metadataschemaregistry
    df_item = df_item.convert_dtypes()
    df_metadatavalue = df_metadatavalue.convert_dtypes()
    df_metadatafieldregistry = df_metadatafieldregistry.convert_dtypes()
    df_metadataschemaregistry = df_metadataschemaregistry.convert_dtypes()
    df_item['load_datetime'] = date.today()
    df_metadatavalue['load_datetime'] = date.today()
    df_metadatafieldregistry['load_datetime'] = date.today()
    df_metadataschemaregistry['load_datetime'] = date.today()
    return df_item, df_metadatavalue, df_metadatafieldregistry, df_metadataschemaregistry

def land_dspacedb(item, metadatavalue, metadatafieldregistry, metadataschemaregistry):
    df_item = item
    df_metadatavalue = metadatavalue
    df_metadatafieldregistry = metadatafieldregistry
    df_metadataschemaregistry = metadataschemaregistry
    return df_item, df_metadatavalue, df_metadatafieldregistry, df_metadataschemaregistry
