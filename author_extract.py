from authors.author_institution_etl import AuthorExtract
from utils.time_run import time_run

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 800)

@time_run
def main():

    journal = 'HERD'
    ae = AuthorExtract(pandas_cache=f'./data/.cache_pandas/{journal}')
    ae.extract_authors_institutions()
    ae.transform_authors()


if __name__ == '__main__':
    main()
