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

    # Pon VERIFY_SSL=True si quieres validar el certificado con CA_BUNDLE; se desactiva por defecto

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
