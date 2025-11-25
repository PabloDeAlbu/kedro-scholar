import os
import time
import certifi
import requests
import pandas as pd
import xml.etree.ElementTree as ET


def get_oai_records(base_url, verify=None):

    # Usa el bundle de certifi para evitar errores de certificado en requests
    os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    VERIFY_SSL = os.getenv("OAI_VERIFY_SSL", "false").lower() == "true"
    CA_BUNDLE = os.getenv("OAI_CA_BUNDLE") or certifi.where()

    start_time = time.time()

    verify_param = CA_BUNDLE if VERIFY_SSL else False
    if verify is not None:
        verify_param = verify

    response = requests.get(base_url, verify=verify_param)
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Sleeping for {elapsed_time:.2f} seconds")
    time.sleep(elapsed_time)

    if response.status_code == 200:
        return response
    else:
        print(f"Error: {response.status_code}")
        return None

def oai_extract_item_by_col(base_url: str, context: str, df_set: pd.DataFrame, env: str, col_iteration_limit: int = 1, verify=None) -> pd.DataFrame:
    records = []
    iteration_limit = 2

    pending_cols = df_set[df_set["processed"] == False]
    col_ids = pending_cols.head(col_iteration_limit).iloc[:, 0].tolist()

    if not col_ids:
        print("No se encontraron colecciones pendientes con processed=False.")

    for set_id in col_ids:
        resumption_token = 0
        iteration_count = 0

        while True:
            if env == 'dev' and iteration_count >= iteration_limit:
                break

            params = f'/{context}?verb=ListRecords&resumptionToken=oai_dc///{set_id}/{resumption_token}'
            url = base_url + params

            print(f"Consultando: {url}")

            response = get_oai_records(url, verify=verify)

            resumption_token += 100
            iteration_count += 1

            if not response or not response.ok:
                print(f"Error al consultar: {url}")
                break

            xml_content = response.text
            root = ET.fromstring(xml_content)
            ns = {
                'oai': 'http://www.openarchives.org/OAI/2.0/',
                'dc': 'http://purl.org/dc/elements/1.1/'
            }

            record_nodes = root.findall('.//oai:record', ns)

            if not record_nodes:
                print("No se encontraron más registros.")
                break

            for record in record_nodes:
                identifier = record.find('.//oai:identifier', ns)
                item_id = identifier.text if identifier is not None else None
                metadata = record.find('.//oai:metadata', ns)

                if metadata is None:
                    continue

                # Valores simples
                title = metadata.find('.//dc:title', ns)
                date_issued = metadata.find('.//dc:date', ns)

                # Multivaluados
                creators = [e.text for e in metadata.findall('.//dc:creator', ns)]
                types = [e.text for e in metadata.findall('.//dc:type', ns)]
                identifiers = [e.text for e in metadata.findall('.//dc:identifier', ns)]
                languages = [e.text for e in metadata.findall('.//dc:language', ns)]
                publishers = [e.text for e in metadata.findall('.//dc:publisher', ns)]
                subjects = [e.text for e in metadata.findall('.//dc:subject', ns)]
                relations = [e.text for e in metadata.findall('.//dc:relation', ns)]
                rights = [e.text for e in metadata.findall('.//dc:rights', ns)]

                records.append({
                    'item_id': item_id,
                    'col_id': set_id,
                    'title': title.text if title is not None else None,
                    'date_issued': date_issued.text if date_issued is not None else None,
                    'creators': creators,
                    'types': types,
                    'identifiers': identifiers,
                    'languages': languages,
                    'subjects': subjects,
                    'publishers': publishers,
                    'relations': relations,
                    'rights': rights
                })

    df = pd.DataFrame(records)

    timestamp = pd.Timestamp.now(tz="UTC").normalize()
    df['extract_datetime'] = timestamp
    df['load_datetime'] = timestamp

    return df, df.head(100)
