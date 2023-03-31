import os

from utils.dbUtils import dbUtil
from utils.altmetricetl import AltmetricRetriever
from utils.time_run import time_run
from utils.profile_run import profile_run


class etl_Altmetric(object):

    def __init__(self, journal=None):
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir}')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')
        self.ar = AltmetricRetriever()

    def etl_altmetric(self, journal=None):

        work_df = self.db.read_db("articles")
        doi_list = [doi.rstrip().replace(r"https://doi.org/", "") for doi in work_df.doi if isinstance(doi, (str,))]
        print(f'requesting {len(doi_list)} doi items like {doi_list[0:]}')
        altmetric_df = self.ar.getAltmetrics(doi_list=doi_list, refresh=False)
        print(f'altmetric df {self.journal = } {altmetric_df.shape}\n{altmetric_df.head()}')
        altmetric_df.to_sql("altmetrics", self.db.conn, if_exists='replace')

        self.db.conn.close()

        return


@time_run
@profile_run
def main():
    """
    main program for extract, transform, load for HE journals
    """
    ea = etl_Altmetric(journal='HERD')
    ea.etl_altmetric()


if __name__ == '__main__':
    main()
