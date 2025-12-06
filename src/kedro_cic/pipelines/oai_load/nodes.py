import pandas as pd

def oai_load_identifiers(df_identifiers_raw: pd.DataFrame)-> pd.DataFrame:

    df_identifiers = df_identifiers_raw[['record_id','datestamp', 'extract_datetime']]
    df_identifiers_sets = df_identifiers_raw[['record_id','set_id', 'extract_datetime']].explode('set_id')

    df_identifiers['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()
    df_identifiers_sets['load_datetime'] = pd.Timestamp.now(tz="UTC").normalize()

    return df_identifiers, df_identifiers_sets

def oai_load_records(df_records_raw: pd.DataFrame, env = 'dev')-> pd.DataFrame:

    df_records_raw = df_records_raw.copy()
    if env == 'dev':
        df_records_raw = df_records_raw.head(1000)

    load_ts = pd.Timestamp.now(tz="UTC").normalize()

    def _select(columns):
        return df_records_raw.loc[:, columns].copy()

    def _explode(column):
        return (
            _select(['record_id', column, 'extract_datetime'])
            .explode(column, ignore_index=True)
            .assign(load_datetime=load_ts)
        )

    df_records = _select(['record_id','col_id','title','date_issued', 'extract_datetime']).assign(load_datetime=load_ts)
    df_record_creators = _explode('creators')
    df_record_types = _explode('types')
    df_record_identifiers = _explode('identifiers')
    df_record_languages = _explode('languages')
    df_record_subjects = _explode('subjects')
    df_record_publishers = _explode('publishers')
    df_record_relations = _explode('relations')
    df_record_rights = _explode('rights')

    df_record_sets = _select(['record_id','set_id', 'extract_datetime'])
    sets_df = df_record_sets.pop('set_id').apply(pd.Series)
    sets_df = sets_df.rename(columns=lambda i: f'set_{i}')
    df_record_sets = pd.concat([df_record_sets, sets_df], axis=1)
    df_record_sets['load_datetime'] = load_ts

    return df_records, df_record_creators, df_record_types, df_record_identifiers, df_record_languages, df_record_subjects, df_record_publishers, df_record_relations, df_record_rights, df_record_sets

def oai_load_sets(df_sets_raw: pd.DataFrame) -> pd.DataFrame:
    df_sets = df_sets_raw.copy()
    df_sets["load_datetime"] = pd.Timestamp.now(tz="UTC").normalize()
    return df_sets
