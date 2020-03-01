# compare.py

# standard libraries
import logging, re
from typing import Callable, Sequence

# third-party libraries
from fuzzywuzzy import fuzz


# Initializing settings and global variables

logger = logging.getLogger(__name__)

# General patterns
WS_PATTERN = re.compile(r'\s+')
PUNC_PATTERN = re.compile(r'[,\.#:]')
AMP_PATTERN = re.compile(r'&')

# Imprint patterns
UP_PATTERN = re.compile(r'\bup\b')
UOF_PATTERN = re.compile(r'\bu of\b')
UNIV_PATTERN = re.compile(r'\buniv\b')

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
