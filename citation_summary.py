import os

from utils.dbUtils import dbUtil
from utils.time_run import time_run

from utils.profile_run import profile_run

class CitationSummary:

    def __init__(self, journal=None):
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir = } {os.getcwd() = }')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def citation_summary_runner(self):
        self.load_citers()
        self.load_cited()

    def load_citers(self):
        articles = self.db.read_db(table_name='articles')
        print(f'{articles.shape = }\n{articles.head()}')
        citers = self.db.read_db(table_name='citers_referenced_works')
        citers = citers[citers.referenced_works.isin(articles.works_id)]
        print(f'{citers.shape = }\n{citers.head()}')

    def load_cited(self):
        cited = self.db.read_db(table_name='referenced_works')
        print(f'{cited.shape = }\n{cited.head()}')

@time_run
# @profile_run
def main():

    cs = CitationSummary(journal='HERD')
    cs.citation_summary_runner()


if __name__ == '__main__':
    main()