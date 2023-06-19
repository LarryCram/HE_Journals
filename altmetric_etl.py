import os
import diskcache as dc

from utils.dbUtils import dbUtil
from utils.altmetricetl import AltmetricRetriever
from utils.time_run import time_run
from utils.profile_run import profile_run


class AltmetricEtl(object):

    def __init__(self, journal=None):
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir}')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')
        self.cache_pandas = dc.Cache(rf'./data/.cache_pandas/{journal}', size_limit=int(4e9))
        self.ar = AltmetricRetriever()

    def altmetric_etl(self, journal=None):

        work_df = self.cache_pandas["articles"]
        work_df.info()
        doi_list = [doi.rstrip().replace(r"https://doi.org/", "") for doi in work_df.doi if isinstance(doi, (str,))]
        print(f'requesting {len(doi_list)} doi items like {doi_list[0:]}')
        altmetric_df = self.ar.getAltmetrics(doi_list=doi_list, refresh=False)
        print(f'altmetric df {self.journal = } {altmetric_df.shape}\n{altmetric_df.head()}')
        self.cache_pandas["altmetrics"] = altmetric_df

        self.db.conn.close()

        return


@time_run
@profile_run
def main():
    """
    main program for extract, transform, load for HE journals
    """
    ea = AltmetricEtl(journal='HERD')
    ea.altmetric_etl()


if __name__ == '__main__':
    main()
