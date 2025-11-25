from datetime import date
import pandas as pd

def oai_load_item(df_item_raw: pd.DataFrame)-> pd.DataFrame:

    df_item = df_item_raw[['item_id','col_id','title','date_issued', 'extract_datetime']]
    df_item_creators = df_item_raw[['item_id','creators', 'extract_datetime']]
    df_item_types = df_item_raw[['item_id','types', 'extract_datetime']]
    df_item_identifiers = df_item_raw[['item_id','identifiers', 'extract_datetime']]
    df_item_languages = df_item_raw[['item_id','languages', 'extract_datetime']]
    df_item_subjects = df_item_raw[['item_id','subjects', 'extract_datetime']]
    df_item_publishers = df_item_raw[['item_id','publishers', 'extract_datetime']]
    df_item_relations = df_item_raw[['item_id','relations', 'extract_datetime']]
    df_item_rights = df_item_raw[['item_id','rights', 'extract_datetime']]

    df_item_creators = df_item_creators.explode('creators')
    df_item_types = df_item_types.explode('types')
    df_item_identifiers = df_item_identifiers.explode('identifiers')
    df_item_languages = df_item_languages.explode('languages')
    df_item_subjects = df_item_subjects.explode('subjects')
    df_item_publishers = df_item_publishers.explode('publishers')
    df_item_relations = df_item_relations.explode('relations')
    df_item_rights = df_item_rights.explode('rights')

    df_item['load_datetime'] =  pd.Timestamp.now(tz="UTC").normalize()
    df_item_creators['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_types['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_identifiers['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_languages['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_subjects['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_publishers['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_relations['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_item_rights['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()

    return df_item, df_item_creators, df_item_types, df_item_identifiers, df_item_languages, df_item_subjects, df_item_publishers, df_item_relations, df_item_rights
