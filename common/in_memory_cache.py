from common.constants import TOKENS_STARTED_FROM_U

CACHED_SYMBOLS = {}
CACHED_LOGOS = {}
CACHED_DENOMS_FOR_SEARCH = {}


def set_cached_denoms(mapped_denoms):
    for denom in mapped_denoms:
        CACHED_SYMBOLS[denom['denom']] = denom['symbol']


def set_cached_logos(logos):
    for logo in logos:
        CACHED_LOGOS[logo['symbol']] = logo['logo']


def get_denom_to_search_in_api(denom):
    if denom not in CACHED_DENOMS_FOR_SEARCH:
        denom_to_search = denom
        if denom not in TOKENS_STARTED_FROM_U and (
                denom.startswith('u') or denom.startswith('stu')):
            denom_to_search = denom.replace('u', '', 1)
        if denom == 'basecro':
            denom_to_search = 'cro'
        elif denom == 'staevmos':
            denom_to_search = 'stevmos'
        elif denom == 'stuatom':
            denom_to_search = 'atom'
        elif denom == 'stuosmo':
            denom_to_search = 'osmo'
        CACHED_DENOMS_FOR_SEARCH[denom] = denom_to_search
        return denom_to_search
    else:
        return CACHED_DENOMS_FOR_SEARCH[denom]
