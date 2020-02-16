# standard modules
import json, logging, os, re
from datetime import datetime

# third-party modules
import requests
import bs4 as BeautifulSoup
import pandas as pd
import numpy as np

# local modules
from create_db_cache import ENGINE, DB_CACHE_PATH_ELEMS, set_up_database


# Initialize settings and global variables

logger = logging.getLogger(__name__)
logging.basicConfig()

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logger.setLevel(ENV.get('LOG_LEVEL', 'DEBUG'))

# Database
if not os.path.isfile(os.path.join(*DB_CACHE_PATH_ELEMS)):
    set_up_database()

TITLES_CSV_PATH_ELEMS = ENV['TITLES_CSV_PATH']

worldcat_config = ENV['WORLDCAT']
WC_API_KEY = worldcat_config['WC_SEARCH_API_KEY']
WC_BIB_BASE_URL = worldcat_config['BIB_RESOURCE_BASE_URL']

with open(os.path.join('config', 'marcxml_lookup.json')) as lookup_file:
    MARCXML_LOOKUP = json.loads(lookup_file.read())

PUNC_PATTERN = re.compile(r'[,\.#:]')
AMP_PATTERN = re.compile(r'&')


# Function(s)

# Create unique request string for WorldCat Search API caching
def create_unique_request_str(base_url: str, params_dict: dict, private_keys: list =["wskey"]) -> str:
    sorted_params = sorted(params_dict.keys())
    fields = []
    for param in sorted_params:
        if param not in private_keys:
            fields.append('{}-{}'.format(param, params_dict[param]))
    return base_url + '&'.join(fields)


# Make the request and cache new data, or retrieves the cached data
def make_request_using_cache(url: str, params: dict) -> str:
    unique_req_url = create_unique_request_str(url, params)
    cache_df = pd.read_sql(f'''
        SELECT * FROM request WHERE request_url = '{unique_req_url}';
    ''', ENGINE)

    if len(cache_df):
        logger.debug('Retrieving cached data...')
        return cache_df.iloc[0]['response']

    logger.debug('Making a request for new data...')
    response_obj = requests.get(url, params)
    status_code = response_obj.status_code
    if status_code == 403:
        logger.warning('Reached API limit')
        return ''
    elif status_code != 200:
        logger.debug(response_obj.text)
        logger.warning(f'Received irregular status code: {status_code}')
        return ''

    response_text = response_obj.text
    new_request_df = pd.DataFrame({
        'request_url': [unique_req_url],
        'response': [response_text]
    })
    logger.debug(new_request_df)
    new_request_df.to_sql('request', ENGINE)
    return response_text


# Use the Bibliographic Resource tool to search for records and parse the returned MARC XML
def look_up_title_in_worldcat(title_series: pd.Series) -> pd.DataFrame:

    # Generate query string
    full_title = title_series['Title']
    if title_series["Subtitle"] not in ["N/A", ""]:
        full_title += ' ' + title_series['Subtitle']
    logger.debug('full_title: ' + full_title)
    query_title = AMP_PATTERN.sub('and', PUNC_PATTERN.sub('', full_title))
    logger.debug('query_title: ' + query_title)
    query_str = f'srw.ti all "{query_title}"'

    params = {
        'wskey': WC_API_KEY,
        "query": query_str,
        "maximumRecords": 100,
        'frbrGrouping': 'off'
    }

    result = make_request_using_cache(WC_BIB_BASE_URL, params)
    if not result:
        return result
    
    result_xml = BeautifulSoup(result, 'xml')
    logger.debug(result_xml)
    number_of_records = result_xml.find("numberOfRecords").text
    logger.debug(number_of_records)

    records = result_xml.find_all("recordData")
    record_series_list = []
    for record in records:
        record_series = pd.Series()
        for key in MARCXML_LOOKUP:
            marc_field = MARCXML_LOOKUP[key]
            statement = record.find('datafield', tag=marc_field['datafield'])
            if not statement:
                value = pd.NA
            else:
                sub_statement = statement.find("subfield", code=marc_field['subfield'])
                if not sub_statement:
                    value = pd.NA
                else:
                    value = sub_statement.text
            record_series[key] = value
        record_series_list.append(record_series)
    records_df = pd.concat(record_series_list)
    return records_df

def identify_ebooks() -> None:
    press_titles_df = pd.read_csv(os.path.join(*TITLES_CSV_PATH_ELEMS))
    logger.info(press_titles_df)
    for press_title_row_tup in press_titles_df.iterrows():
        press_title_series = press_title_row_tup[1]
        oclc_records_df = look_up_title_in_worldcat(press_title_series)
        logger.info(oclc_records_df.head())
    # compare title to records
    # add isbns from matches

if __name__ == '__main__':
    identify_ebooks()
