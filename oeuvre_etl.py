import os

import pandas as pd

from openalexextract.FrameMaker import FrameMaker
from utils.dbUtils import dbUtil

from utils.time_run import time_run

pd.set_option('display.width', 2000)

class ProcessOeuvre:

    def __init__(self, journal=None):
        self.journal = journal
        self.author_list = None
        self.article_authorships = None
        self.not_articles = None
        self.articles = None
        self.authorships = None
        self.works = None
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir}')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def load_works_authorships(self):
        self.works = self.db.read_db(table_name='works')
        self.authorships = (self.db.read_db(table_name='authorships')
                            .drop(columns='index'))

    def keep_articles(self):
        temp = self.works.loc[:, ['works_id', 'doi', 'abstract', 'biblio_first_page', 'biblio_last_page']]
        mask = []
        for row in temp.itertuples():
            first, last = (row.biblio_first_page, row.biblio_last_page)
            page_length = 0
            if isinstance(first, str) and isinstance(last, str) and first.isdigit() and last.isdigit():
                page_length = int(last) - int(first)
            if isinstance(row.abstract, str) or page_length > 5:
                mask.append(True)
            else:
                mask.append(False)
        self.articles = self.works[mask]
        self.db.to_db(df=self.articles, table_name='articles')
        self.not_articles = self.works[~self.works.index.isin(self.articles.index)]
        self.db.to_db(df=self.not_articles, table_name='not_articles')

    def article_authors(self):
        self.article_authorships = self.authorships[self.authorships.works_id.isin(self.articles.works_id)]
        author_list = [a for a in self.article_authorships.author_id if not isinstance(a, type(pd.NA))]
        self.author_list = sorted(list(set(author_list)))
        # self.author_list = [i for i in self.author_list if i != 'https://openalex.org/A2035216382']

    def extract_authors(self):
        from collections import defaultdict
        fm = FrameMaker()
        entity = 'authors'
        collect_all = defaultdict(list)
        for j, author in enumerate(self.author_list):
            fm.frame_maker(entity={entity: author},
                           filtre=None,
                           select=['id', 'orcid', 'display_name',
                                   'works_count', 'cited_by_count',
                                   'last_known_institution',
                                   'x_concepts',
                                   'works_api_url'],
                           refresh=False)

            for k, v in fm.frame_dict.items():
                collect_all[k].append(v)
                if j % 100 == 0:
                    print(f'{j = } {k = } {author = } {v.shape = }')

        for k, v in collect_all.items():
            df = pd.concat(v)
            table_name = k if 'authors' in k else f'authors_{k}'
            print(f'writing authors to {self.journal}.db {k = } {df.shape = } {df.columns = }\n{df.head()}')
            # df.info(verbose=True)
            self.db.to_db(df=df, table_name=table_name)

    def extract_oeuvres(self):
        from collections import defaultdict
        fm = FrameMaker()
        entity = 'works'
        collect_all = defaultdict(list)
        for j, author in enumerate(self.author_list):
            fm.frame_maker(entity={entity: None},
                           filtre={'author.id': author},
                           select=['id', 'doi', 'display_name',
                                   'publication_year', 'type',
                                   'authorships', 'biblio', 'abstract_inverted_index',
                                   'cited_by_count', 'concepts', 'locations',
                                   'referenced_works', 'related_works'],
                           refresh=False)

            for k, v in fm.frame_dict.items():
                collect_all[k].append(v)
                if j % 100 == 0:
                    print(f'{j = } {k = } {author = } {v.shape = }')

        for k, v in collect_all.items():
            df = pd.concat(v)
            table_name = f'oeuvre_{k}'
            if table_name == 'oeuvre_locations':
                df = df.drop(columns=['source_host_organization_lineage'])
                print(f'writing oeuvres to {self.journal}.db  {table_name = } {df.shape = } {df.columns = }'
                      f'\n{df.head()}\ndf.info()')
            else:
                print(f'writing oeuvres to {self.journal}.db  {table_name = } {df.shape = } {df.columns = }'
                      f'\n{df.head()}\n{df.info()}')
            self.db.to_db(df=df, table_name=table_name)

    def oeuvre_runner(self):
        self.load_works_authorships()
        self.keep_articles()
        self.article_authors()
        self.extract_authors()
        self.extract_oeuvres()


@time_run
# @profile_run
def main():

    journal = 'HERD'

    po = ProcessOeuvre(journal=journal)
    po.oeuvre_runner()


if __name__ == '__main__':
    main()
