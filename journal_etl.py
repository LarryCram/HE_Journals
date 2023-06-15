from abc import ABCMeta, abstractmethod
import re
import string
import pandas as pd
import diskcache as dc

from openalexextract.openalex_etl import OpenalexEtl
from utils.dbUtils import dbUtil
from utils.profile_run import profile_run
from utils.time_run import time_run
pd.set_option('display.max_columns', 99)  # or 1000
pd.set_option('display.max_rows', 99)  # or 1000
pd.set_option('display.max_colwidth', 48)  # or 199

class IProcessJournal(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def build_chain(cls):
        ...

    @classmethod
    @abstractmethod
    def extract_works(cls):
        ...

    @classmethod
    @abstractmethod
    def keep_articles(cls):
        ...

    @classmethod
    @abstractmethod
    def extract_cited(cls):
        ...

    @classmethod
    @abstractmethod
    def extract_citers(cls):
        ...


class ProcessJournal(IProcessJournal):

    def __init__(self, journal=None, journal_id=None, journal_issn=None, refresh=None):

        self.journal = journal
        self.journal_id = journal_id
        self.journal_issn = journal_issn
        self.refresh = refresh
        self.works = None
        self.articles = None
        self.references = None
        self.citers = None
        self.oeuvres = None

        self.oa = OpenalexEtl()
        self.oa.build_session().build_cache(cache=rf'./data/.cache_from_oa_api/{journal_id}')
        self.cache_pandas = dc.Cache(rf'./data/.cache_pandas/{journal_id}', size_limit=int(4e9))
        self.db = dbUtil(f'./data/.db/{journal}')

    def build_chain(self, query=None, refresh=None):
        self.oa.build_query(query=query).build_extractor(refresh=refresh).build_transformer()

    def extract_works(self):
        query = f'works?filter=primary_location.source.issn:{self.journal_issn}'
        self.build_chain(query=query, refresh=self.refresh)
        self.works = self.oa.extract.rename(columns={'id': 'works_id'})
        self.remove_duplicates()
        self.cache_pandas['works'] = self.works
        return self

    def remove_duplicates(self):
        self.works = self.works.sort_values('publication_year').drop_duplicates(subset='works_id', keep='last')
        for row in self.works.itertuples(index=True):
            temp_title = re.sub(f'[{string.punctuation}]', '', row.display_name)
            self.works.at[row.Index, 'temp_title'] = ' '.join(x.lower() for x in temp_title.split(' '))
        print(self.works[['works_id', 'display_name', 'temp_title']])
        print(f'{self.works.shape = }')
        self.works = self.works.drop_duplicates(subset='temp_title', keep='last')
        self.works = self.works.drop(columns='temp_title')
        print(f'{self.works.shape = }')

    def keep_articles(self):
        self.works['reference_count'] = [len(refs) for refs in self.works.referenced_works]
        mask = []
        for row in self.works.itertuples():
            first, last = (row.biblio_first_page, row.biblio_last_page)
            page_length = 0
            if isinstance(first, str) and isinstance(last, str) and first.isdigit() and last.isdigit():
                page_length = int(last) - int(first)
            logic = self.is_article(page_length, row.abstract, row.display_name)
            if logic:
                mask.append(True)
            else:
                mask.append(False)
        self.articles = self.works[mask].copy()
        self.cache_pandas['articles'] = self.works
        self.check_articles()
        return self

    def check_articles(self):
        self.db.to_db(df=self.articles[['works_id', 'title', 'publication_year', 'abstract',
                                        'biblio_volume', 'biblio_first_page', 'biblio_last_page', 'reference_count']]
                      , table_name='articles', if_exists='replace')
        not_articles = self.works[~self.works.index.isin(self.articles.index.values)]
        self.db.to_db(df=not_articles[['works_id', 'title', 'publication_year', 'abstract',
                                        'biblio_volume', 'biblio_first_page', 'biblio_last_page', 'reference_count']]
                      , table_name='not_articles', if_exists='replace')
        print(f'to database: {self.articles.shape = } {not_articles.shape = }')
        return

    def is_article(self, page_length, abstract, display_name):
        if any(
            re.match(f'^{term}[a-z ]*?$', display_name.strip().lower())
            for term in [
                'book review',
                'errata',
                'corrigend',
                'correction',
                'review',
                'editor',
                'review articles',
                'review symposium',
            ]
        ):
            return False
        if re.match('^editorial: ', display_name.strip().lower()):
            return False
        if page_length > 4:
            return True
        if isinstance(abstract, str):
            return True
        return False

    def extract_citers(self):

        citers_df_list = []
        for row in self.articles.itertuples():
            cited_by_api_url = row.cited_by_api_url
            if not cited_by_api_url:
                continue
            query = cited_by_api_url.replace('https://api.openalex.org/', '')
            self.build_chain(query=query, refresh=self.refresh)
            self.citers = self.oa.extract.rename(columns={'id': 'cited_id'}).drop_duplicates(subset='cited_id')
            self.citers.insert(0, 'works_id', row.works_id)
            citers_df_list.append(self.citers)
        ref_df = pd.concat(citers_df_list)
        self.cache_pandas['citers'] = ref_df
        return self

    def extract_cited(self):

        ref_df_list = []
        for row in self.articles.itertuples():
            referenced_works = row.referenced_works
            reference_list = '|'.join(referenced_works[:50])
            if len(reference_list) < 1:
                continue
            query = f'works?filter=ids.openalex:{reference_list}'
            self.build_chain(query=query, refresh=self.refresh)
            self.references = self.oa.extract.rename(columns={'id': 'cited_id'}).drop_duplicates(subset='cited_id')
            self.references.insert(0, 'works_id', row.works_id)
            ref_df_list.append(self.references)
        ref_df = pd.concat(ref_df_list)
        self.cache_pandas['references'] = ref_df
        return self


# @time_run
# @profile_run
def main():

    journal = 'HERD'
    journal_id = 'S4210176587'
    journal_issn = '0729-4360'
    pj = ProcessJournal(journal=journal, journal_id=journal_id, journal_issn=journal_issn)
    pj.extract_works().keep_articles().extract_cited().extract_citers()


if __name__ == '__main__':
    main()
