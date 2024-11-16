import ast
from datetime import date
import math
import pandas as pd
import requests
import xmltodict

def transform_researchproduct(df):
    def get_value_by_attr(value, attr, attr_filter, attr_value):
        if isinstance(value, dict):
            if value.get(attr_filter) == attr_value:
                return value.get(attr)
        elif isinstance(value, list):
            for item in value:
                if item.get(attr_filter) == attr_value:
                    return item.get(attr)
        return None

    def convert_to_dict_or_list(value):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value

    df_researchproduct = pd.DataFrame()

    df_researchproduct['id'] = df['dri:objIdentifier']
    df_researchproduct['dateOfCollection'] = df['dri:dateOfCollection']

    df['collectedfrom'] = df['collectedfrom'].apply(convert_to_dict_or_list)
    df_researchproduct_collectedfrom = df.loc[:,['dri:objIdentifier','collectedfrom']].explode('collectedfrom').reset_index(drop=True)
    df_researchproduct_collectedfrom['collectedfrom'] = df_researchproduct_collectedfrom['collectedfrom']

    df['originalId'] = df['originalId'].apply(convert_to_dict_or_list)
    df_researchproduct_originalId = df.loc[:,['dri:objIdentifier','originalId']].explode('originalId').reset_index(drop=True)

    df['pid'] = df['pid'].apply(convert_to_dict_or_list)

    df_researchproduct['doi'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='doi')
    df_researchproduct['pmid'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='pmid')
    df_researchproduct['pmc'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='pmc')
    df_researchproduct['arXiv'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='arXiv')
    df_researchproduct['uniprot'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='uniprot')
    df_researchproduct['ena'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='ena')
    df_researchproduct['pdb'] = df.loc[:,'pid'].apply(get_value_by_attr, attr='#text', attr_filter='@classid' ,attr_value='pdb')

    df['measure'] = df['measure'].apply(convert_to_dict_or_list)
    df_researchproduct_measure = df.loc[:,['dri:objIdentifier','measure']].explode('measure').reset_index(drop=True)

    df['title'] = df['title'].apply(convert_to_dict_or_list)
    df_researchproduct_title = df.loc[:,['dri:objIdentifier','title']].explode('title').reset_index(drop=True)

    df['bestaccessright'] = df['bestaccessright'].apply(convert_to_dict_or_list)
    df_researchproduct['bestaccessright'] = df.loc[:,'bestaccessright'].apply(lambda x: x.get('@classname'))

    df_researchproduct['dateofacceptance'] = df['dateofacceptance']
    
    df_researchproduct['publisher'] = df.loc[:,'publisher']
    
    df['source'] = df['source'].apply(convert_to_dict_or_list)
    df_researchproduct_source = df.loc[:,['dri:objIdentifier','source']].explode('source').reset_index(drop=True)

    df['resulttype'] = df['resulttype'].apply(convert_to_dict_or_list)
    df_researchproduct['resulttype'] = df['resulttype'].apply(lambda x: x.get('@classname'))

    df['resourcetype'] = df['resourcetype'].apply(convert_to_dict_or_list)
    df_researchproduct['resourcetype'] = df['resourcetype'].apply(lambda x: x.get('@classname'))
    
    df_researchproduct['isgreen'] = df['isgreen']

    df_researchproduct['openaccesscolor'] = df['openaccesscolor']

    df_researchproduct['isindiamondjournal'] = df['isindiamondjournal']
    
    df_researchproduct['publiclyfunded'] = df['publiclyfunded']
    # df['journal'] = df['journal'].apply(convert_to_dict_or_list)
    # df_researchproduct['journal'] = df['journal']

    df['subject'] = df['subject'].apply(convert_to_dict_or_list)
    df_researchproduct_subject = df.loc[:,['dri:objIdentifier','subject']].explode('subject').reset_index(drop=True)

    df_researchproduct['load_datetime'] = date.today()

    return df_researchproduct, df_researchproduct_collectedfrom, df_researchproduct_originalId, df_researchproduct_measure, df_researchproduct_title, df_researchproduct_source, df_researchproduct_subject
    


def fetch_researchproduct_openaire(dim_doi: pd.DataFrame, r_token, env)-> pd.DataFrame:
    base_url = "https://api.openaire.eu/search/researchProducts"
    df_list = []

    doi_limit = 9999
    if (env == 'dev'): doi_limit = 9
    
    skipped_list = []

    not_in_openaire = dim_doi['in_openaire'] == False
    dim_doi = dim_doi[not_in_openaire]

    doi_list = dim_doi.iloc[0:doi_limit]['doi'].to_list()
    doi_comma_separated = ','.join(doi_list)

    # se define cantidad de batches a partir de la cantidad de resultados por batch y cantidad de doi
    batch_size = 10
    num_batches = math.ceil(len(doi_list) / batch_size)

    for batch_index in range(num_batches):

        batch = doi_list[batch_index * batch_size : (batch_index + 1) * batch_size]
        doi_comma_separated = ','.join(batch)

        graph_url = f"{base_url}?doi={doi_comma_separated}"
        headers = { 'Authorization': f'Bearer {r_token}' }

        api_response = requests.get(graph_url, headers=headers)
        print(f'GET "{graph_url}" {api_response.status_code}')

        if api_response.status_code == 200:
            data_dict = xmltodict.parse(api_response.content)
            results = data_dict.get('response', {}).get('results', {}).get('result', [])

            for result in results:

                publication_header = result.get('header', {})
                publication_metadata = result.get('metadata', {}).get('oaf:entity', {}).get('oaf:result', {})

                publication = publication_header | publication_metadata 
                if publication:
                    df_normalized = pd.json_normalize(publication, max_level=0)
                    df_list.append(df_normalized)
                else:
                    print("No publication data found in result")
        else:
            print(f'Error: Received status code {api_response.status_code}')
            skipped_list.extend(batch)
            break

    print(f'{len(df_list)} batches processed')
    print(f'{len(skipped_list)} DOIs skipped')

    if df_list:
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.DataFrame()

    return df

def land_researchproduct_openaire(df: pd.DataFrame)-> pd.DataFrame:
    df_researchproduct, df_researchproduct_collectedfrom, df_researchproduct_originalId, df_researchproduct_measure, df_researchproduct_title, df_researchproduct_source, df_researchproduct_subject = transform_researchproduct(df)
    return df_researchproduct, df_researchproduct_collectedfrom, df_researchproduct_originalId, df_researchproduct_measure, df_researchproduct_title, df_researchproduct_source, df_researchproduct_subject
