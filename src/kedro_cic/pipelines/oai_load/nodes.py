from datetime import date
import pandas as pd

def _pick_load_dt(df: pd.DataFrame):
    # Si hay una sola fecha en el batch, usala; si hay varias, quedate con la más reciente;
    # si no hay, hoy.
    if 'load_datetime' not in df.columns or df['load_datetime'].isna().all():
        return date.today()
    vals = df['load_datetime'].dropna()
    if vals.nunique() == 1:
        return vals.iloc[0]
    return pd.to_datetime(vals).max().date()

def oai_load_identifiers(df_identifiers_raw: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df_identifiers_raw)

    df_identifiers = df_identifiers_raw[['record_id','datestamp', 'extract_datetime']].copy()
    df_identifiers_sets = df_identifiers_raw[['record_id','set_id', 'extract_datetime']].explode('set_id')

    df_identifiers['load_datetime'] = load_dt
    df_identifiers_sets['load_datetime'] = load_dt

    return df_identifiers, df_identifiers_sets

def oai_load_records(df_records_raw: pd.DataFrame, env = 'dev')-> pd.DataFrame:

    df_records_raw = df_records_raw.copy()
    load_dt = _pick_load_dt(df_records_raw)

    if env == 'dev':
        df_records_raw = df_records_raw.head(1000)

    def _select(columns):
        return df_records_raw.loc[:, columns].copy()

    def _explode(column):
        base_cols = ['record_id', column, 'extract_datetime']
        missing_cols = [col for col in base_cols if col not in df_records_raw.columns]
        if missing_cols:
            # Si la columna no existe, devolvé un df vacío con las columnas esperadas
            return pd.DataFrame(columns=base_cols + ['load_datetime'])
        return (
            _select(base_cols)
            .explode(column, ignore_index=True)
            .assign(load_datetime=load_dt)
        )

    df_records = _select(['record_id','col_id','title','date_issued', 'extract_datetime']).assign(load_datetime=load_dt)
    df_record_creators = _explode('creators')
    df_record_descriptions = _explode('descriptions')
    df_record_types = _explode('types')
    df_record_identifiers = _explode('identifiers')
    df_record_languages = _explode('languages')
    df_record_subjects = _explode('subjects')
    df_record_publishers = _explode('publishers')
    df_record_relations = _explode('relations')
    df_record_rights = _explode('rights')
    df_record_formats = _explode('formats')
    df_record_sets = _explode('set_id')

#    df_record_sets = _select(['record_id','set_id', 'extract_datetime'])
#    sets_df = df_record_sets.pop('set_id').apply(pd.Series)
#    sets_df = sets_df.rename(columns=lambda i: f'set_{i}')
#    df_record_sets = pd.concat([df_record_sets, sets_df], axis=1)
#    df_record_sets['load_datetime'] = load_dt

    return df_records, df_record_creators, df_record_descriptions, \
        df_record_types, df_record_identifiers, df_record_languages, \
            df_record_subjects, df_record_publishers, df_record_relations, \
                df_record_rights, df_record_formats, df_record_sets

def oai_load_sets(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df.dropna(inplace=True)

    df['load_datetime'] = load_dt

    return df
