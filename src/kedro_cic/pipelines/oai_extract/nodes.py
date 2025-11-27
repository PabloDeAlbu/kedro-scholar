import os
import time
import certifi
import requests
import pandas as pd
import xml.etree.ElementTree as ET


def get_oai_records(base_url, verify=None, max_retries=3, backoff_factor=1.0):

    # Usa el bundle de certifi para evitar errores de certificado en requests
    os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    VERIFY_SSL = os.getenv("OAI_VERIFY_SSL", "false").lower() == "true"
    CA_BUNDLE = os.getenv("OAI_CA_BUNDLE") or certifi.where()

    verify_param = CA_BUNDLE if VERIFY_SSL else False
    if verify is not None:
        verify_param = verify

    for attempt in range(1, max_retries + 1):
        start_time = time.time()
        response = None
        try:
            response = requests.get(base_url, verify=verify_param)
            elapsed_time = time.time() - start_time
        except requests.RequestException as exc:
            elapsed_time = time.time() - start_time
            print(f"Error en request (intento {attempt}/{max_retries}): {exc}")
        sleep_time = max(elapsed_time, 0.1)
        print(f"Sleeping for {sleep_time:.2f} seconds")
        time.sleep(sleep_time)

        if response and response.status_code == 200:
            return response

        status = response.status_code if response else "sin respuesta"
        print(f"Error: {status} (intento {attempt}/{max_retries})")

        if attempt < max_retries:
            backoff = backoff_factor * attempt
            print(f"Reintentando en {backoff:.2f} segundos...")
            time.sleep(backoff)
    return None

def oai_extract_identifiers(base_url: str, context: str, df_set: pd.DataFrame, env: str, verify=None) -> pd.DataFrame:
    
    return None, None

def oai_extract_records(base_url: str, context: str, df_set: pd.DataFrame, env: str, verify=None) -> pd.DataFrame:
    records = []
    
    iteration_limit = 2 if env == "dev" else None

    pending_cols = df_set[df_set["processed"] == False]
    col_ids = pending_cols.head(iteration_limit).iloc[:, 0].tolist()

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


def oai_extract_sets(base_url, context, env, verify=None):

    resumption_token = 0
    all_sets = []

    iteration_limit = 2
    iteration_count = 0

    while True:

        if env == 'dev' and iteration_count >= iteration_limit:
            break

        params = f'/{context}?verb=ListSets&resumptionToken=////{resumption_token}'
        url = base_url + params

        print(f"Consultando: {url}")

        response = get_oai_records(url, verify=verify)
        if not response:
            break

        xml_content = response.text
        root = ET.fromstring(xml_content)
        ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}

        sets_data = []
        for set_elem in root.findall('.//oai:set', ns):
            set_spec = set_elem.find('oai:setSpec', ns).text if set_elem.find('oai:setSpec', ns) is not None else None
            set_name = set_elem.find('oai:setName', ns).text if set_elem.find('oai:setName', ns) is not None else None
            sets_data.append({'setSpec': set_spec, 'setName': set_name})

        if not sets_data:
            print("No se encontraron más sets.")
            break

        all_sets.extend(sets_data)
        resumption_token += 100  # avanzar manualmente
        iteration_count += 1

    df_sets = pd.DataFrame(all_sets)

    current_time = pd.Timestamp.now(tz="UTC").normalize()

    df_sets['extract_datetime'] = current_time
    df_sets['load_datetime'] = current_time

    return df_sets
