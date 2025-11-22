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

def oai_load_set(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df.dropna(inplace=True)

    df['load_datetime'] = load_dt

    return df
