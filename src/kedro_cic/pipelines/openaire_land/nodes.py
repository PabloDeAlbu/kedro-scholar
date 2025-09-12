from datetime import date
import pandas as pd

def _pick_load_dt(df: pd.DataFrame):
    if 'load_datetime' not in df.columns or df['load_datetime'].isna().all():
        return date.today()
    vals = df['load_datetime'].dropna()
    if vals.nunique() == 1:
        return vals.iloc[0]
    return pd.to_datetime(vals).max().date()

def openaire_land_researchproduct(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    expected_columns = [
        'id',
        'openAccessColor',
        'publiclyFunded',
        'type',
        'language',
        'country',
        'mainTitle',
        'description',
        'publicationDate',
        'format',
        'bestAccessRight',
        'indicators',
        'isGreen',
        'isInDiamondJournal',
        'publisher',
        'source',
        'container',
        'contributor',
        'contactPerson',
        'coverage',
        'contactPerson',
        'embargoEndDate',
        'dateOfCollection',        
    ]

    # Agregar columnas faltantes con NaN
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df = df.convert_dtypes()

    df_researchproduct = df[expected_columns].copy()
    df_researchproduct.reset_index(drop=True, inplace=True)

    # language
    df_researchproduct['language'] = df_researchproduct['language'].apply(
        lambda x: x if isinstance(x, dict) else {}
    )
    df_researchproduct['language_code'] = df_researchproduct['language'].apply(lambda x: x['code'])
    df_researchproduct['language_label'] = df_researchproduct['language'].apply(lambda x: x['label'])

    
    ## bestAccessRight
    df_researchproduct['bestAccessRight_label'] = df['bestAccessRight'].apply(lambda x: x['label'] if x else None)
    df_researchproduct['bestAccessRight_scheme'] = df['bestAccessRight'].apply(lambda x: x['scheme'] if x else None)

    ## indicators
    df_indicators = pd.json_normalize(df['indicators']).reset_index(drop=True)
    
    indicators_expected_columns = [
        "citationImpact.citationClass",
        "citationImpact.citationCount",
        "citationImpact.impulse",
        "citationImpact.impulseClass",
        "citationImpact.influence",
        "citationImpact.influenceClass",
        "citationImpact.popularity",
        "citationImpact.popularityClass",
        "usageCounts.downloads",
        "usageCounts.views",
    ]

    # Agregar columnas para indicators y faltantes con NaN
    for col in indicators_expected_columns:
        if col not in df_indicators.columns:
            df_indicators[col] = pd.NA

    df_researchproduct = pd.concat([df_researchproduct.drop(columns=['indicators']).reset_index(drop=True), df_indicators], axis=1)

    # TODO country
    # TODO description
    # TODO format
    # TODO instance
    # TODO container
    # TODO contributor
    # TODO contactPerson
    # TODO coverage

    ## drop de columnas procesadas en otros df
    df_researchproduct.drop(columns=[
        'country', 'bestAccessRight', 
        'language', 'format',  
        'container', 'source', 'description',
        'contributor', 'contactPerson', 'coverage'
        ], inplace=True)

    df_researchproduct['load_datetime'] = load_dt

    return df_researchproduct

def openaire_land_researchproduct_authors(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_author = df[['id','authors']].explode('authors').reset_index(drop=True)

    df_authors = pd.json_normalize(df_research_author['authors'])

    df_research_author = pd.concat([df_research_author['id'], df_authors], axis=1)

    df_research_author['load_datetime'] = load_dt

    return df_research_author

def openaire_land_researchproduct_collectedfrom(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_collectedfrom = df[['id','collectedFrom']].explode('collectedFrom').reset_index(drop=True)
    df_research_collectedfrom.rename(columns={'id':'researchproduct_id'}, inplace=True)

    df_collectedfrom = pd.json_normalize(df_research_collectedfrom['collectedFrom'])
    df_collectedfrom.rename(columns={'key':'datasource_id'}, inplace=True)

    df_research_collectedfrom = pd.concat(
        [df_research_collectedfrom['researchproduct_id'], df_collectedfrom.loc[:,['datasource_id','value']]], 
        axis=1
    )

    df_research_collectedfrom['load_datetime'] = load_dt

    return df_research_collectedfrom

def openaire_land_researchproduct_contributors(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_contributor = df[['id','contributors']].explode('contributors').reset_index(drop=True)
    df_research_contributor.dropna(inplace=True)

    df_research_contributor['load_datetime'] = load_dt

    return df_research_contributor

def openaire_land_researchproduct_descriptions(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_description = df[['id','descriptions']].explode('descriptions').reset_index(drop=True)

    df_research_description['load_datetime'] = load_dt

    return df_research_description

def openaire_land_researchproduct_instances(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_instances = df[['id','instances']].explode('instances').reset_index(drop=True)

    df_instances = pd.json_normalize(df_research_instances['instances'])
    df_research_instances = pd.concat([df_research_instances['id'], df_instances], axis=1)

    df_research_instances = df_research_instances.explode('pids').reset_index(drop=True)

    df_research_instances = df_research_instances.explode('urls').reset_index(drop=True)

    df_pids = pd.json_normalize(df_research_instances['pids'])
    df_research_instances = df_research_instances.drop(columns=['pids'])

    df_research_instances = pd.concat([df_research_instances, df_pids], axis=1)

    df_research_alternateidentifiers = df_research_instances[['id','alternateIdentifiers']].dropna().explode('alternateIdentifiers').reset_index(drop=True)
    df_alternateidentifiers = pd.json_normalize(df_research_alternateidentifiers['alternateIdentifiers'])
    df_research_alternateidentifiers = pd.concat([df_research_alternateidentifiers['id'], df_alternateidentifiers], axis=1)

    df_research_instances.drop(columns=['alternateIdentifiers'], inplace=True)

    df_research_instances['load_datetime'] = load_dt
    df_research_alternateidentifiers['load_datetime'] = load_dt

    return df_research_instances, df_research_alternateidentifiers

def openaire_land_researchproduct_organizations(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_organization = df[['id','organizations']].explode('organizations').reset_index(drop=True)
    df_research_organization.rename(columns={'id':'researchproduct_id'}, inplace=True)

    df_organizations = pd.json_normalize(df_research_organization['organizations'])
    df_organizations.rename(columns={'id':'organization_id'}, inplace=True)

    df_research_organization = pd.concat(
        [df_research_organization['researchproduct_id'], df_organizations['organization_id']], 
        axis=1
    )

    df_organization_pid = df_organizations.loc[:, ['organization_id', 'pids']].copy()
    df_organizations.drop(columns=['pids'], inplace=True)
    df_organization_pid.dropna(inplace=True)

    df_organization_pid = df_organization_pid.explode('pids', ignore_index=True)
    df_organization_pid.loc[:, ['organization_id', 'pids']]

    df_pid = pd.json_normalize(df_organization_pid['pids'])
    df_pid.rename(columns={'scheme':'pid_scheme','value':'pid_value'}, inplace=True)

    df_organization_pid.drop(columns=['pids'], inplace=True)
    df_organization_pid = pd.concat([df_organization_pid, df_pid], axis=1)

    df_organizations['load_datetime'] = load_dt
    df_research_organization['load_datetime'] = load_dt
    df_organization_pid['load_datetime'] = load_dt

    return df_organizations, df_research_organization, df_organization_pid

def openaire_land_researchproduct_originalid(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_originalids = df[['id','originalIds']]

    df_research_originalids = df_research_originalids.explode('originalIds').reset_index(drop=True)

    df_research_originalids['load_datetime'] = load_dt

    return df_research_originalids

def openaire_land_researchproduct_pids(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)
    
    df_research_pid = df.loc[:,['id','pids']]
    df_research_pid.dropna(inplace=True)
    
    df_research_pid = df_research_pid.explode('pids').reset_index(drop=True)

    df_pid = pd.json_normalize(df_research_pid['pids'])
    
    df_research_pid = pd.concat([df_research_pid['id'], df_pid], axis=1)
    df_research_pid['load_datetime'] = load_dt

    return df_research_pid

def openaire_land_researchproduct_sources(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_sources = df.loc[:,['id','sources']]
    df_research_sources.dropna(inplace=True)
    
    df_research_sources = df_research_sources.explode('sources').reset_index(drop=True)

    df_research_sources['load_datetime'] = load_dt

    return df_research_sources

def openaire_land_researchproduct_subjects(df: pd.DataFrame)-> pd.DataFrame:

    load_dt = _pick_load_dt(df)

    df_research_subjects = df.loc[:,['id','subjects']]
    df_research_subjects.dropna(inplace=True)

    df_research_subjects = df_research_subjects.explode('subjects').reset_index(drop=True)

    df_subjects = pd.json_normalize(df_research_subjects['subjects'])
    df_research_subjects = pd.concat([df_research_subjects['id'], df_subjects],axis=1)

    df_research_subjects['load_datetime'] = load_dt

    return df_research_subjects
