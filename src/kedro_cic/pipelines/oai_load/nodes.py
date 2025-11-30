from datetime import date
import pandas as pd

def oai_load_identifiers(df_identifiers_raw: pd.DataFrame)-> pd.DataFrame:

    df_identifiers = df_identifiers_raw[['record_id','datestamp', 'extract_datetime']]
    df_identifiers_sets = df_identifiers_raw[['record_id','set_id', 'extract_datetime']].explode('set_id')

    df_identifiers['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_identifiers_sets['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()

    return df_identifiers, df_identifiers_sets

def oai_load_records(df_record_raw: pd.DataFrame)-> pd.DataFrame:

    df_record = df_record_raw[['item_id','col_id','title','date_issued', 'extract_datetime']]

    df_record_creators = df_record_raw[['item_id','creators', 'extract_datetime']]
    df_record_types = df_record_raw[['item_id','types', 'extract_datetime']]
    df_record_identifiers = df_record_raw[['item_id','identifiers', 'extract_datetime']]
    df_record_languages = df_record_raw[['item_id','languages', 'extract_datetime']]
    df_record_subjects = df_record_raw[['item_id','subjects', 'extract_datetime']]
    df_record_publishers = df_record_raw[['item_id','publishers', 'extract_datetime']]
    df_record_relations = df_record_raw[['item_id','relations', 'extract_datetime']]
    df_record_rights = df_record_raw[['item_id','rights', 'extract_datetime']]

    df_record_creators = df_record_creators.explode('creators')
    df_record_types = df_record_types.explode('types')
    df_record_identifiers = df_record_identifiers.explode('identifiers')
    df_record_languages = df_record_languages.explode('languages')
    df_record_subjects = df_record_subjects.explode('subjects')
    df_record_publishers = df_record_publishers.explode('publishers')
    df_record_relations = df_record_relations.explode('relations')
    df_record_rights = df_record_rights.explode('rights')

    df_record['load_datetime'] =  pd.Timestamp.now(tz="UTC").normalize()
    df_record_creators['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_types['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_identifiers['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_languages['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_subjects['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_publishers['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_relations['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_record_rights['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()

    return df_record, df_record_creators, df_record_types, df_record_identifiers, df_record_languages, df_record_subjects, df_record_publishers, df_record_relations, df_record_rights
