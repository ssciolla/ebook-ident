# compare.py

# standard libraries
import logging, re
from typing import Callable, Sequence

# third-party libraries
from fuzzywuzzy import fuzz


# Initializing settings and global variables

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)

UNIV_PATTERN = re.compile(r'Univ\.')
WS_PATTERN = re.compile(r'\s+')
PUNC_PATTERN = re.compile(r'[,\.#:]')
EBOOK_PATTERN = re.compile(r'e-?book')
AMP_PATTERN = re.compile(r'&')


# Functions

def tokenize(input: str) -> Sequence[str]:
    tokens = WS_PATTERN.split(input)
    logger.debug(tokens)
    return tokens


def normalize(input: str) -> str:
    normalized_str = AMP_PATTERN.sub('and', PUNC_PATTERN.sub('', input)).lower()
    logger.debug(normalized_str)
    return normalized_str


def normalize_univ(input: str) -> str:
    return UNIV_PATTERN.sub('University', input)


def look_for_ebook(input: str) -> bool:
    norm_input = normalize(input)
    result = EBOOK_PATTERN.search(norm_input)
    if result:
        return True
    return False


def create_compare_func(left: str, thresh: float, transforms: Sequence[Callable] = []) -> Callable:
    left_tokens = tokenize(left)

    norm_left = left
    for transform in transforms:
        norm_left = transform(norm_left)
    norm_left = normalize(norm_left)

    def compare_func(right: str) -> bool:
        norm_right = right
        for transform in transforms:
            norm_right = transform(norm_right)
        norm_right = normalize(norm_right)
        full_lev_ratio = fuzz.ratio(norm_left, norm_right)
        logger.debug(full_lev_ratio)
        if full_lev_ratio >= thresh:
            logger.debug(f'The full Levenstein distance ratio of {full_lev_ratio} met the {thresh} threshold.')
            return True

        right_tokens = tokenize(right)
        token_diff = abs(len(left_tokens) - len(right_tokens))
        logger.debug(token_diff)
        if token_diff < 3 and len(left) > 4:
            partial_lev_ratio = fuzz.partial_ratio(norm_left, norm_right)
            logger.debug(partial_lev_ratio)
            if partial_lev_ratio >= thresh:
                logger.debug(f'The partial Levenstein distance ratio of {full_lev_ratio} met the {thresh} threshold.')
                return True
        
        logger.debug(f'No Levenstein distance ratios met the {thresh} threshold.')
        return False

    return compare_func
