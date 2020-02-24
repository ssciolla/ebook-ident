# identify

# standard libraries
import json, logging, os, re
from datetime import datetime
from typing import Sequence

# third-party libraries
import requests
import bs4 as BeautifulSoup
import numpy as np
import pandas as pd

# local libraries
from compare import create_compare_func, look_for_ebook, normalize, normalize_univ
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

# Set up database if necessary
if not os.path.isfile(os.path.join(*DB_CACHE_PATH_ELEMS)):
    set_up_database()

BOOKS_CSV_PATH_ELEMS = ENV['BOOKS_CSV_PATH']

worldcat_config = ENV['WORLDCAT']
WC_API_KEY = worldcat_config['WC_SEARCH_API_KEY']
WC_BIB_BASE_URL = worldcat_config['BIB_RESOURCE_BASE_URL']

with open(os.path.join('config', 'marcxml_lookup.json')) as lookup_file:
    MARCXML_LOOKUP = json.loads(lookup_file.read())


# Functions - Utilities

def create_full_title(record: pd.Series):
    full_title = record['Title']
    if record["Subtitle"] not in ["N/A", ""]:
        full_title += ' ' + record['Subtitle']
    logger.debug('full_title: ' + full_title)
    return full_title


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
        'response': [response_text]
    })
    logger.debug(new_request_df)
    new_request_df.to_sql('request', ENGINE)
    return response_text


# Functions - Processing

# Use the Bibliographic Resource tool to search for records and parse the returned MARC XML
def look_up_book_in_worldcat(book_series: pd.Series) -> pd.DataFrame:

    # Generate query string
    full_title = create_full_title(book_series)
    query_author = normalize(f'{book_series["Author_First"]} {book_series["Author_Last"]}')
    logger.debug('full_title: ' + full_title)
    query_title = normalize(full_title)
    logger.debug('query_title: ' + query_title)
    query_str = f'srw.ti all "{query_title}" and srw.au all "{query_author}"'

    params = {
        'wskey': WC_API_KEY,
        "query": query_str,
        "maximumRecords": 100,
        'frbrGrouping': 'off'
    }

    result = make_request_using_cache(WC_BIB_BASE_URL, params)
    if not result:
        return pd.DataFrame()
    
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


def run_checks_and_return_isbns(orig_record: pd.Series, results_df: pd.DataFrame) -> pd.Series:
    checked_results_df = results_df.copy()
    logger.info(f'Number of WorldCat results: {len(checked_results_df)}')

    # Create comparison functions
    full_title = create_full_title(orig_record)
    compare_to_title = create_compare_func(full_title, 85)
    imprint_transforms = [normalize_univ]
    compare_to_imprint = create_compare_func(orig_record['Imprint'], 85, imprint_transforms)

    # Run comparisons
    checked_results_df['title_matched'] = checked_results_df['Title'].map(compare_to_title)
    checked_results_df['imprint_matched'] = checked_results_df['Imrpint'].map(compare_to_imprint)
    checked_results_df['ebook_present'] = checked_results_df['Physical_Description'].map(look_for_ebook)

    # Gather ISBNs
    # This is probably not right; need to check
    matches_df = checked_results_df[(
        checked_results_df['title_matched'] and 
        checked_results_df['imprint_matched'] and
        checked_results_df['ebook_present']
    )]
    logger.info(matches_df.head())
    unique_isbns = matches_df['ISBN'].drop_duplicates().dropna()
    return unique_isbns


def identify_ebooks() -> None:
    # Load input data
    press_books_df = pd.read_csv(os.path.join(*BOOKS_CSV_PATH_ELEMS))
    logger.info(press_books_df)

    # For each record, fetch WorldCat data, compare to record, and document results
    new_book_series_list = []
    multiple_isbn_matches = []
    no_isbn_matches = []
    num_successful_matches = 0

    for press_book_row_tup in press_books_df.iterrows():
        new_book_series = press_book_row_tup[1].copy()
        full_book_title = create_full_title(new_book_series)
        logger.info(f'Looking for {full_book_title} in WorldCat...')
        if not new_book_series['ISBN']:
            logger.info(f'{create_full_title(new_book_series)} already has an ISBN: {new_book_series["ISBN"]}')
        else:
            wc_records_df = look_up_book_in_worldcat(new_book_series)
            logger.info(wc_records_df.head())
            isbns = run_checks_and_return_isbns(new_book_series, wc_records_df)
            if not isbns:
                logger.warning(f'No ISBNS were found!')
                no_matches_dict = pd.Series({
                    'index': press_book_row_tup[0],
                    'full_title': full_book_title
                })
                no_isbn_matches.append(no_matches_dict)
            else:
                if len(isbns) > 1:
                    logger.warning('Multiple ISBNs found!')
                    multiple_match_series = pd.Series({
                        'index': press_book_row_tup[0],
                        'full_title': full_book_title,
                        'matching_isbns': isbns
                    })
                    multiple_isbn_matches.append(multiple_match_series)
                else:
                    new_book_series['ISBN'] = isbns[0]
                    logger.info(f'Book successfully matched with ISBN: {isbns[0]}')
                    num_successful_matches += 1
        new_book_series_list.append(new_book_series)

    # Generate CSV output
    identified_books_df = pd.concat(new_book_series_list)
    identified_books_df.to_csv(os.path.join('data', 'identified_books.csv'))

    multiple_isbn_matches_df = pd.concat(multiple_isbn_matches)
    multiple_isbn_matches_df.to_csv(os.path.join('data', 'multiple_isbn_matches.csv'))

    no_isbn_matches_df = pd.concat(no_isbn_matches)
    no_isbn_matches_df.to_csv(os.path.join('data', 'no_isbn_matches.csv'))

    # Log Summary Report
    report_str = '** Sumary Report from identify.py **\n\n'
    report_str += f'-- Total number of records: {len(press_books_df)} --\n'
    report_str += f'-- Books successfully updated with ISBNs: {num_successful_matches}\n'
    report_str += f'-- Books with multiple ISBN matches: {len(multiple_isbn_matches_df)}\n'
    report_str += f'-- Books with no matching ISBNs: {len(no_isbn_matches_df)}\n'
    if len(multiple_isbn_matches_df) or len(no_isbn_matches_df): 
        report_str += f'?? Review the match problem CSVs in the data directory.\n'
        report_str += f'?? Resolve match problems by manually adding an accurate ISBN to {"/".join(BOOKS_CSV_PATH_ELEMS)}.'
    logger.info(f'\n\n{report_str}')
    return None


# Main Program

if __name__ == '__main__':
    identify_ebooks()
