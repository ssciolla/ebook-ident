# compare.py

# standard libraries
import logging, re
from typing import Callable, Optional, Sequence

# third-party libraries
import pandas as pd
from fuzzywuzzy import fuzz


# Initializing settings and global variables

logger = logging.getLogger(__name__)

# General patterns
WS_PATTERN = re.compile(r'\s+')
NA_PATTERN = re.compile(r'\b<?n\.?a\.?>?\b', flags=re.IGNORECASE)

PUNC_PATTERN = re.compile(r'[,\.#:]')
AMP_PATTERN = re.compile(r'&')
PAREN_PATTERN = re.compile(r'[\(\)]')
PAREN_CONTENT_PATTERN = re.compile(r'\(([^\(]+)\)')

ISBN_PATTERN = re.compile(r'[0-9]')

# Publisher patterns
UP_PATTERN = re.compile(r'\bup\b')
UOF_PATTERN = re.compile(r'\bu of\b')
UNIV_PATTERN = re.compile(r'\buniv\b')

# Format patterns
FORMAT_PATTERNS = {
    'Hardcover': [re.compile(r'\bhard[ -]?cover\b'), re.compile(r'\bhbk?\b'), re.compile(r'\bhcr??\b')],
    'Paperback': [re.compile(r'\bpaper[ -]?back\b'), re.compile(r'\bpbk?\b')],
    'Ebook': [re.compile(r'\be[- ]?book\b'), re.compile(r'electronic'), re.compile(r'\bebk?\b')]
}


# Functions

def tokenize(input: str) -> Sequence[str]:
    tokens = WS_PATTERN.split(input)
    logger.debug(tokens)
    return tokens


def normalize(input: str) -> str:
    normalized_str = AMP_PATTERN.sub('and', PUNC_PATTERN.sub('', input)).lower()
    logger.debug(f'normalize: {input} -> {normalized_str}')
    return normalized_str


def normalize_univ(input: str) -> str:
    norm_input = UNIV_PATTERN.sub('university', UOF_PATTERN.sub('university of', UP_PATTERN.sub('university press', input)))
    logger.debug(f'normalize_univ: {input} -> {norm_input}')
    return norm_input


def polish_isbn(input: str) -> str:
    return input.split()[0]


# Extract extra or parenthetical content from a cell (used on ISBN a)
def extract_extra_atoms(input: str) -> Optional[str]:
    if PAREN_CONTENT_PATTERN.match(input):
        groups = PAREN_CONTENT_PATTERN.match(input).groups()
        logger.info(f'extract_extra_atoms: {groups}')
        if len(groups) > 1:
            logger.warning('Multiple parenthetical expressions found.')
            return ' '.join(groups)
        else:
            return groups[0]
    if len(input.split()) > 1:
        extra_atoms = ' '.join(input.split()[1:])
        return extra_atoms
    return pd.NA


def classify_by_format(field_to_analyze: str) -> str:
    normal_field = normalize(field_to_analyze)

    matches = []
    for format in FORMAT_PATTERNS.keys():
        patterns = FORMAT_PATTERNS[format]
        for pattern in patterns:
            result = pattern.search(normal_field)
            if result is not None:
                matches.append(format)

    unique_matches = []
    for match in matches:
        if match not in unique_matches:
            unique_matches.append(match)

    if len(unique_matches) == 0:
        format = pd.NA
    elif len(unique_matches) == 1:
        format = matches[0]
    else:
        match_format_str = ",".join([match_format for match_format in unique_matches])
        logger.error(f'Matched with multiple formats: {match_format_str}')
    return format


# Create a comparison function for mapping along columns that finds the Levenshtein Difference between a 
# a column value (right) and one or more given values from the HEB record (lefts)
def create_compare_func(lefts: Sequence[str], thresh: float, transforms: Sequence[Callable] = []) -> Callable:
    left_dicts = []
    for left in lefts:
        left_tokens = tokenize(left)
        norm_left = normalize(left)
        for transform in transforms:
            norm_left = transform(norm_left)
        left_dicts.append({'orig_left': left, 'left_tokens': left_tokens, 'norm_left': norm_left})

    def compare_func(right: str) -> bool:
        norm_right = normalize(right)
        for transform in transforms:
            norm_right = transform(norm_right)

        for left_dict in left_dicts:
            one_norm_left = left_dict['norm_left']
            full_lev_ratio = fuzz.ratio(one_norm_left, norm_right)
            logger.debug(full_lev_ratio)
            if full_lev_ratio >= thresh:
                logger.debug(f'The full Levenstein distance ratio of {full_lev_ratio} met the {thresh} threshold.')
                logger.debug(f'{one_norm_left} ~ {norm_right}')
                return True

            # This won't catch one word publishers (e.g. Holt) if the alternative representation has multiple words
            right_tokens = tokenize(right)
            token_diff = abs(len(left_dict['left_tokens']) - len(right_tokens))
            logger.debug(token_diff)
            if token_diff < 3 and len(left) > 4:
                partial_lev_ratio = fuzz.partial_ratio(one_norm_left, norm_right)
                logger.debug(partial_lev_ratio)
                if partial_lev_ratio >= thresh:
                    logger.warning(f'The partial Levenstein distance ratio of {partial_lev_ratio} met the {thresh} threshold.')
                    logger.warning(f'{one_norm_left} ~ {norm_right}')
                    return True
            
        logger.debug(f'No Levenstein distance ratios met the {thresh} threshold.')
        return False

    return compare_func
