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
    
    # convert df_item dtypes
    df_item = df_item.convert_dtypes()
    df_item['uuid'] = df_item['uuid'].astype(str)
    
    df_item['submitter_id'] = df_item['submitter_id'].astype(str)
    df_item['owning_collection'] = df_item['owning_collection'].astype(str)

    # convert df_metadatavalue dtypes
    df_metadatavalue = df_metadatavalue.convert_dtypes()
    df_metadatavalue['dspace_object_id'] = df_metadatavalue['dspace_object_id'].astype(str)

    # convert df_metadatafieldregistry dtypes
    df_metadatafieldregistry = df_metadatafieldregistry.convert_dtypes()

    # convert df_metadataschemaregistry dtypes
    df_metadataschemaregistry = df_metadataschemaregistry.convert_dtypes()
    df_metadatafieldregistry = df_metadatafieldregistry.convert_dtypes()

    df_item['load_datetime'] = date.today()
    df_metadatavalue['load_datetime'] = date.today()
    df_metadatafieldregistry['load_datetime'] = date.today()
    df_metadataschemaregistry['load_datetime'] = date.today()
    return df_item, df_metadatavalue, df_metadatafieldregistry, df_metadataschemaregistry

def land_dspacedb(item, metadatavalue, metadatafieldregistry, metadataschemaregistry):
    df_item = item
    df_metadatavalue = metadatavalue
    df_metadatavalue['text_value'] = df_metadatavalue['text_value'].str.strip()
    df_metadatafieldregistry = metadatafieldregistry
    df_metadataschemaregistry = metadataschemaregistry
    return df_item, df_metadatavalue, df_metadatafieldregistry, df_metadataschemaregistry
