# identify

# standard libraries
import json, logging, os
from datetime import datetime
from typing import Dict, Sequence

# third-party libraries
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# local libraries
from compare import create_compare_func, normalize, normalize_univ, NA_PATTERN
from create_db_cache import ENGINE, DB_CACHE_PATH_ELEMS, set_up_database


# Initialize settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

# Set up database if necessary
if not os.path.isfile(os.path.join(*DB_CACHE_PATH_ELEMS)):
    set_up_database()

BOOKS_CSV_PATH_ELEMS = ENV['BOOKS_CSV_PATH']

worldcat_config = ENV['WORLDCAT']
WC_API_KEY = worldcat_config['WC_SEARCH_API_KEY']
WC_BIB_BASE_URL = worldcat_config['BIB_RESOURCE_BASE_URL']
TEST_MODE_OPTS = ENV['TEST_MODE']

with open(os.path.join('config', 'marcxml_lookup.json')) as lookup_file:
    MARCXML_LOOKUP = json.loads(lookup_file.read())


# Functions - Utilities

def create_full_title(record: Dict[str, str]):
    full_title = record['Title']
    if 'Subtitle' in record.keys() and record["Subtitle"] not in ["N/A", ""]:
        full_title += ' ' + record['Subtitle']
    logger.debug('full_title: ' + full_title)
    return full_title


def mint_wc_key_name(key: str, subfield: str, index: int, num_subs: int, num_states) -> str:
    key_name = key
    if num_subs > 1:
        key_name += " " + subfield
    if num_states > 1:
        key_name += " " + str(index)
    return key_name


# Explode groups of related columns from one row into separate dictionaries
def unflatten(book_record: Dict[str, str], column_prefixes: Sequence[str]) -> Sequence[Dict[str, str]]:
    embedded_records = []
    num = 1
    more_records = True
    while more_records:
        if (f"{column_prefixes[0]} {num}") not in book_record.keys():
            more_records = False
        else:
            embedded_record = {}
            for column_prefix in column_prefixes:
                embedded_record[column_prefix] = book_record[f"{column_prefix} {num}"]
            embedded_records.append(embedded_record)
            num += 1
    logger.debug(embedded_records)
    return embedded_records


# Functions - Caching

# Create unique request string for WorldCat Search API caching
def create_unique_request_str(base_url: str, params_dict: dict, private_keys: list =["wskey"]) -> str:
    sorted_params = sorted(params_dict.keys())
    fields = []
    for param in sorted_params:
        if param not in private_keys:
            fields.append('{}-{}'.format(param, params_dict[param]))
    return base_url + '&'.join(fields)


# Make the request and cache new data, or retrieves the cached data
def make_request_using_cache(url: str, params: Dict[str, str]) -> str:
    unique_req_url = create_unique_request_str(url, params)
    cache_df = pd.read_sql(f'''
        SELECT * FROM request WHERE request_url = '{unique_req_url}';
    ''', ENGINE)

    if not cache_df.empty:
        logger.debug('Retrieving cached data...')
        return cache_df.iloc[0]['response']

    logger.debug('Making a request for new data...')
    response_obj = requests.get(url, params)
    logger.debug(response_obj.url)
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
        'response': [response_text],
        'timestamp': datetime.now().isoformat()
    })
    logger.debug(new_request_df)
    new_request_df.to_sql('request', ENGINE, if_exists='append', index=False)
    return response_text


# Functions - Processing

def parse_marcxml(xml_record: str) -> Sequence[Dict[str, str]]:
    result_xml = BeautifulSoup(xml_record, 'xml')
    number_of_records = result_xml.find("numberOfRecords").text
    logger.debug(number_of_records)

    records = result_xml.find_all("recordData")
    record_dicts = []
    for record in records:
        record_dict = {}
        for marc_key in MARCXML_LOOKUP:
            marc_field = MARCXML_LOOKUP[marc_key]
            statements = record.find_all('datafield', tag=marc_field['datafield'])
            num = 0
            for statement in statements:
                num += 1
                subfields = marc_field['subfields']
                for subfield in subfields:
                    sub_statement = statement.find('subfield', code=subfield)
                    if sub_statement and not NA_PATTERN.search(sub_statement.text):
                        key_name = mint_wc_key_name(marc_key, subfield, num, len(subfields), len(statements))
                        record_dict[key_name] = sub_statement.text
            if num > 1:
                logger.warning(f'Multiple values found for {marc_key}!')
                logger.warning(record_dict)
            logger.debug(record_dict)
        record_dicts.append(record_dict)
    return record_dicts
    

# Use the Bibliographic Resource tool to search for records and parse the returned MARC XML
def look_up_book_in_worldcat(book_dict: Dict[str, str]) -> pd.DataFrame:
    # Generate query string
    full_title = create_full_title(book_dict)
    logger.info(f'Looking for "{full_title}" in WorldCat...')
    
    # I'm currently deciding not to normalize author string
    # Data currently has one author last name; otherwise I'd do what's commented below or process one-to-many relationship
    # query_author = normalize(f"{book_dict['Author_First']} {book_dict['Author_Last']})
    query_author = book_dict['Author_Last']
    query_title = normalize(full_title)
    query_str = f'srw.ti all "{query_title}" and srw.au all "{query_author}"'
    logger.debug(query_str)
    params = {
        'wskey': WC_API_KEY,
        "query": query_str,
        "maximumRecords": 100,
        'frbrGrouping': 'off'
    }
    result = make_request_using_cache(WC_BIB_BASE_URL, params)
    
    if not result:
        return pd.DataFrame()

    records = parse_marcxml(result)
    records_df = pd.DataFrame(records)
    logger.info(f'Number of WorldCat records found: {len(records_df)}')
    logger.debug(records_df.head(10))
    return records_df


def run_checks_and_return_matches(orig_record: Dict[str, str], results_df: pd.DataFrame) -> pd.DataFrame:
    checked_df = results_df.copy()
    logger.debug(orig_record)
    logger.debug(checked_df)

    # Create comparison functions
    full_title = create_full_title(orig_record)
    compare_to_title = create_compare_func([full_title], 85)

    known_publishers = []
    for pub_dict in unflatten(orig_record, ['Publisher']):
        if pd.notna(pub_dict['Publisher']):
            known_publishers.append(pub_dict['Publisher'])
    logger.debug(known_publishers)
    compare_to_publisher = create_compare_func(known_publishers, 85, [normalize_univ])

    # Create full title column
    checked_df['Full_Title'] = checked_df['Title'] + checked_df['Subtitle']
    logger.debug(checked_df['Full_Title'])

    # Run comparisons
    checked_df['Title_Match'] = checked_df['Full_Title'].map(compare_to_title, na_action='ignore')
    checked_df['Publisher_Match'] = checked_df['Publisher'].map(compare_to_publisher, na_action='ignore')
    logger.info(checked_df[['Title', 'Publisher', 'Title_Match', 'Publisher_Match']])

    # Gather matching manifestation records
    manifest_df = checked_df.loc[(
        (checked_df['Title_Match']) & (checked_df['Publisher_Match'])
    )]
    logger.info(f'Matched {len(manifest_df)} records!')
    logger.info(manifest_df.head(20))

    # Add Full_Title and HEB ID from HEB
    manifest_df = manifest_df.assign(**{
        'HEB_ID': orig_record['ID'],
        'HEB_Title': orig_record['Title']
    })
    return manifest_df


def identify_ebooks() -> None:
    # Load input data
    input_path = os.path.join(*BOOKS_CSV_PATH_ELEMS)
    if '.xlsx' in BOOKS_CSV_PATH_ELEMS[-1]:
        press_books_df = pd.read_excel(input_path, dtype=str)
        press_books_df = press_books_df.iloc[1:]  # Remove dummy record
    else:
        press_books_df = pd.read_csv(input_path, dtype=str)

    # Necessary for the moment because column names are inconsistent
    press_books_df = press_books_df.rename(columns={
        'Author last': 'Author_Last',
        'Publisher': 'Publisher 1',
        'ISBN1_13': 'ISBN_13 1',
        'Pub Format': 'Pub_Format 1',
        'ISBN2_13': 'ISBN_13 2',
        'Pub Format 2': 'Pub_Format 2',
        'Publisher 3 ISBN': 'ISBN 3',
        'ISBN3_13': 'ISBN_13 3',
        'Publisher 3 Format': 'Pub_Format 3'
    })
    logger.debug(press_books_df.columns)

    # Limit number of records for testing purposes
    if TEST_MODE_OPTS['ON']:
        logger.info('TEST_MODE is ON.')
        press_books_df = press_books_df.iloc[:TEST_MODE_OPTS['NUM_RECORDS']]

    # For each record, fetch WorldCat data, compare to record, and document results
    match_manifest_df = pd.DataFrame({})
    non_matching_books = []
    num_books_with_matches = 0

    for press_book_row_tup in press_books_df.iterrows():
        new_book_dict = press_book_row_tup[1].copy().to_dict()
        logger.info(new_book_dict)

        wc_records_df = look_up_book_in_worldcat(new_book_dict)
        new_matches_df = run_checks_and_return_matches(new_book_dict, wc_records_df)

        if new_matches_df.empty:
            logger.warning(f'No matching records with isbns were found!')
            non_matching_books.append(new_book_dict)
        else:
            num_books_with_matches += 1
            isbns = new_matches_df['ISBN'].drop_duplicates().dropna().to_list()
            logger.info(f'Book successfully matched with record(s) with {len(isbns)} unique ISBN(s): {isbns}')
            logger.info(new_matches_df)
            match_manifest_df = match_manifest_df.append(new_matches_df)

    # Generate CSV output
    if not match_manifest_df.empty:
        logger.debug(match_manifest_df)
        match_manifest_df.to_csv(os.path.join('data', 'matched_manifests.csv'), index=False)

    if non_matching_books:
        no_isbn_matches_df = pd.DataFrame(non_matching_books)
        no_isbn_matches_df.to_csv(os.path.join('data', 'no_isbn_matches.csv'), index=False)

    # Log Summary Report
    report_str = '** Sumary Report from identify.py **\n\n'
    report_str += f'-- Total number of books included in search: {len(press_books_df)}\n'
    report_str += f'-- Number of books successfully matched with records with ISBNs: {num_books_with_matches}\n'
    report_str += f'-- Number of books with no matching records: {len(non_matching_books)}\n'
    logger.info(f'\n\n{report_str}')
    return None


# Main Program

if __name__ == '__main__':
    identify_ebooks()
