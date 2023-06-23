from authors.author_institution_etl import AuthorInstitutionExtract
from utils.time_run import time_run

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 800)

@time_run
def main():

    # etl authors and institutions from authorship
    journal = 'HERD'
    print(f'{journal = }')
    pandas_cache = f'./data/.cache_pandas/{journal}'
    core_cache = r'C:\Users\Lawrence\Projects\openalex-snapshot\.cache_pandas_core'
    ae = AuthorInstitutionExtract(pandas_cache=pandas_cache, core_cache=core_cache, refresh=True)
    ae.extract_authors_institutions()
    # print(ae.authors.head())
    # print(ae.institutions.head())
    #
    # # find author homonyms
    # ae.extract_authors_homonyms()
    # print(ae.authors_homonyms.head())

    # match institutions to canonical list
    ae.institutions_match_canonical()


if __name__ == '__main__':
    main()
