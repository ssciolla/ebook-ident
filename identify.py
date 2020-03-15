# identify

# standard libraries
import json, logging, os
from datetime import datetime
from typing import Dict, Sequence

# third-party libraries
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

# local libraries
from compare import classify_by_format, \
                    create_compare_func, \
                    extract_extra_atoms, \
                    normalize, \
                    polish_isbn, \
                    normalize_univ, \
                    NA_PATTERN
from db_cache import make_request_using_cache, set_up_database


# Initialize settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

# Set up database if necessary
if not os.path.isfile(os.path.join(*ENV['DB_CACHE_PATH'])):
    set_up_database()

BOOKS_CSV_PATH_ELEMS = ENV['BOOKS_CSV_PATH']

worldcat_config = ENV['WORLDCAT']
WC_API_KEY = worldcat_config['WC_SEARCH_API_KEY']
WC_BIB_BASE_URL = worldcat_config['BIB_RESOURCE_BASE_URL']
TEST_MODE_OPTS = ENV['TEST_MODE']

with open(os.path.join('config', 'marcxml_lookup.json')) as lookup_file:
    MARCXML_LOOKUP = json.loads(lookup_file.read())
with open(os.path.join('config', 'input_to_identify.json')) as input_to_identify_cw:
    INPUT_TO_IDENTIFY_CW = json.loads(input_to_identify_cw.read())
with open(os.path.join('config', 'identify_to_output.json')) as identify_to_output_cw:
    IDENTIFY_TO_OUTPUT_CW = json.loads(identify_to_output_cw.read())


# Functions - Utilities

def create_full_title(record: Dict[str, str]) -> str:
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
            # Drop null dictionaries
            non_null_values = pd.Series(list(embedded_record.values())).dropna().to_list()
            if len(non_null_values) > 0:
                embedded_records.append(embedded_record)
            num += 1
    logger.debug(embedded_records)
    return embedded_records


# Functions - Processing

def parse_marcxml(xml_record: str) -> Sequence[Dict[str, str]]:
    result_xml = BeautifulSoup(xml_record, 'xml')
    number_of_records = result_xml.find("numberOfRecords").text
    if int(number_of_records) > 100:
        logger.error(f'Number of records > 100: {number_of_records}')

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
                    key_name = mint_wc_key_name(marc_key, subfield, num, len(subfields), len(statements))
                    if sub_statement and not NA_PATTERN.search(sub_statement.text):
                        record_dict[key_name] = sub_statement.text
                    else:
                        record_dict[key_name] = pd.NA
            if num > 1 and marc_key != 'ISBN':
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

    # Data currently has one author last name; otherwise I'd do what's commented below or process one-to-many relationship
    # query_author = normalize(f"{book_dict['Author_First']} {book_dict['Author_Last']})
    # Replacing apostrophe because they are breaking query strings when they occur
    query_author = book_dict['Author_Last'].replace("'", " ")
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
        return pd.DataFrame({})

    records = parse_marcxml(result)
    records_df = pd.DataFrame(records)
    logger.info(f'Number of WorldCat records found: {len(records_df)}')
    logger.debug(records_df.head(10))
    return records_df


# Determines format for a row on multiple analyzed columns
def determine_format(row: pd.Series):
    results = row[['Q Format', 'Overflow Format']].drop_duplicates().dropna()
    results = [result for result in results if result != "#NA#"]
    if len(results) > 1:
        logger.warning('Different formats were found ')
        logger.warning(results)
    elif len(results) < 1:
        return pd.NA
    else:
        return results[0]


def classify_and_find_unique_manifests(matches_df: pd.DataFrame):
    if matches_df.empty:
        return pd.DataFrame({})

    logger.info(matches_df.columns)
    all_isbn_dicts = []
    for match_row_tup in matches_df.iterrows():
        match_dict = match_row_tup[1].to_dict()
        isbn_dicts = unflatten(match_dict, ['ISBN a', 'ISBN q'])
        if 'Publisher' in match_dict.keys():
            publisher = match_dict['Publisher']
        else:
            publishers = [pub_dict['Publisher'] for pub_dict in unflatten(match_dict, ['Publisher'])]
            logger.warning(f'Multiple publishers: {publishers}')
            publisher = publishers[0]
        [isbn_dict.update({'Publisher': publisher}) for isbn_dict in isbn_dicts]
        all_isbn_dicts += isbn_dicts

    all_isbns_df = pd.DataFrame(all_isbn_dicts)

    if all_isbns_df.empty:
        return pd.DataFrame({})

    logger.debug(all_isbns_df.columns)
    # Transform and analyze
    all_isbns_df['ISBN'] = all_isbns_df['ISBN a'].map(polish_isbn, na_action='ignore')
    all_isbns_df['ISBN Overflow'] = all_isbns_df['ISBN a'].map(extract_extra_atoms, na_action='ignore')

    unique_isbn_format_df = all_isbns_df.copy()
    # Save unique ISBNS for later analysis
    unique_isbns = unique_isbn_format_df['ISBN'].drop_duplicates()

    unique_isbn_format_df = all_isbns_df.fillna('#NA#').drop_duplicates()
    unique_isbn_format_df['Q Format'] = all_isbns_df['ISBN q'].map(classify_by_format, na_action='ignore').fillna('#NA#')
    unique_isbn_format_df['Overflow Format'] = unique_isbn_format_df['ISBN Overflow'].map(classify_by_format, na_action='ignore').fillna('#NA#')
    unique_isbn_format_df['Format'] = unique_isbn_format_df.apply(determine_format, axis='columns').fillna('#NA#')

    unique_isbn_format_df = unique_isbn_format_df.drop_duplicates(subset=['ISBN', 'Format'])
    unique_isbn_format_df = unique_isbn_format_df.where(unique_isbn_format_df != '#NA#', pd.NA)
    complete_isbn_format_df = unique_isbn_format_df.copy().dropna(axis='index', subset=['ISBN', 'Format'])

    for isbn_format_row_tup in unique_isbn_format_df.iterrows():
        isbn_format_series = isbn_format_row_tup[1]
        if isbn_format_series['ISBN'] in unique_isbns and isbn_format_series['Format'] == pd.NA:
            complete_isbn_format_df = complete_isbn_format_df.append(isbn_format_series)
            logger.info("ISBN without format was added!")
            logger.info(isbn_format_series)

    logger.debug(complete_isbn_format_df.head(15))
    complete_isbn_format_df = complete_isbn_format_df.drop(columns=['ISBN a', 'ISBN q', 'ISBN Overflow', 'Overflow Format', 'Q Format'])
    complete_isbn_format_df = complete_isbn_format_df.assign(**{'Source': 'WorldCat'})
    logger.info(complete_isbn_format_df)
    return complete_isbn_format_df


def run_checks_and_return_matches(orig_record: Dict[str, str], results_df: pd.DataFrame) -> pd.DataFrame:
    checked_df = results_df.copy()
    logger.debug(orig_record)
    logger.debug(checked_df)

    if checked_df.empty:
        return pd.DataFrame({})

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

    # Add Full_Title and HEB ID from HEB
    manifest_df = manifest_df.assign(**{
        'HEB_ID': orig_record['ID'],
        'HEB_Title': orig_record['Title']
    })

    logger.info(manifest_df.head(20))
    return manifest_df


def identify_books() -> None:
    # Load input data
    input_path = os.path.join(*BOOKS_CSV_PATH_ELEMS)
    if '.xlsx' in BOOKS_CSV_PATH_ELEMS[-1]:
        press_books_df = pd.read_excel(input_path, dtype=str)
        press_books_df = press_books_df.iloc[1:]  # Remove dummy record
    else:
        press_books_df = pd.read_csv(input_path, dtype=str)

    # Crosswalk to consistent column names
    press_books_df = press_books_df.rename(columns=INPUT_TO_IDENTIFY_CW)
    logger.debug(press_books_df.columns)

    # Limit number of records for testing purposes
    if TEST_MODE_OPTS['ON']:
        logger.info('TEST_MODE is ON.')
        press_books_df = press_books_df.iloc[:TEST_MODE_OPTS['NUM_RECORDS']]

    # For each record, fetch WorldCat data, compare to record, analyze and accumulate matches
    match_manifest_df = pd.DataFrame({})
    non_matching_books = []
    num_books_with_matches = 0

    for press_book_row_tup in press_books_df.iterrows():
        new_book_dict = press_book_row_tup[1].to_dict()
        logger.info(new_book_dict)

        wc_records_df = look_up_book_in_worldcat(new_book_dict)
        new_matches_df = run_checks_and_return_matches(new_book_dict, wc_records_df)
        unique_manifests_df = classify_and_find_unique_manifests(new_matches_df)

        if unique_manifests_df.empty:
            logger.warning(f'No matching records with ISBNs were found!')
            non_matching_books.append(new_book_dict)
        else:
            num_books_with_matches += 1
            match_manifest_df = match_manifest_df.append(unique_manifests_df)
            isbns = unique_manifests_df['ISBN'].drop_duplicates().to_list()
            logger.info(f'Book successfully matched with record(s) with {len(isbns)} unique ISBN(s): {isbns}')

    logger.debug('Matching Manifests')
    logger.debug(match_manifest_df.describe())

    # Generate CSV output
    if not match_manifest_df.empty:
        match_manifest_df.to_csv(os.path.join('data', 'matched_manifests.csv'), index=False)

    if non_matching_books:
        no_isbn_matches_df = pd.DataFrame(non_matching_books)
        no_isbn_matches_df.to_csv(os.path.join('data', 'no_isbn_matches.csv'), index=False)

    # Log Summary Report
    report_str = '** Summary Report from identify.py **\n\n'
    report_str += f'-- Total number of books included in search: {len(press_books_df)}\n'
    report_str += f'-- Number of books successfully matched with records with ISBNs: {num_books_with_matches}\n'
    report_str += f'-- Number of books with no matching records: {len(non_matching_books)}\n'
    logger.info(f'\n\n{report_str}')
    return None


# Main Program

if __name__ == '__main__':
    identify_books()
