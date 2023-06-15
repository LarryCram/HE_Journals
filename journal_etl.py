from abc import ABCMeta, abstractmethod
import re
import pandas as pd
import diskcache as dc

from openalexextract.openalex_etl import OpenalexEtl
from utils.dbUtils import dbUtil
from utils.profile_run import profile_run
from utils.time_run import time_run

class IProcessJournal(metaclass=ABCMeta):

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

    def __init__(self, journal=None, journal_id=None, journal_issn=None):

        self.journal = journal
        self.journal_id = journal_id
        self.journal_issn = journal_issn
        self.works = None
        self.articles = None
        self.references = None
        self.citers = None
        self.oeuvres = None

        self.oa = OpenalexEtl()
        self.oa.build_session().build_cache(cache=rf'./data/.cache_from_oa_api/{journal_id}')
        self.cache_pandas = dc.Cache(rf'./data/.cache_pandas/{journal_id}', size_limit=int(4e9))
        self.db = dbUtil(f'./data/.db/{journal}')

    def extract_works(self):

        query = f'works?filter=primary_location.source.issn:{self.journal_issn}'
        self.oa.build_query(query=query).build_extractor(refresh=False).build_transformer()
        self.works = self.oa.extract.rename(columns={'id': 'works_id'}).drop_duplicates(subset='works_id')
        self.cache_pandas['works'] = self.works
        return self

    def keep_articles(self):

        self.works['reference_count'] = [len(refs) for refs in self.works.referenced_works]
        print(f'works:\n{self.works.head()}')
        self.works.info()

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
        self.db.to_db(df=self.articles[['works_id', 'title', 'publication_year', 'abstract',
                                        'biblio_first_page', 'biblio_last_page', 'reference_count']]
                      , table_name='articles', if_exists='replace')
        not_articles = self.works[~self.works.index.isin(self.articles.index.values)]
        self.db.to_db(df=not_articles[['works_id', 'title', 'publication_year', 'abstract',
                                        'biblio_first_page', 'biblio_last_page', 'reference_count']]
                      , table_name='not_articles', if_exists='replace')
        print(f'to database: {self.articles.shape = } {not_articles.shape = }')
        return self

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

        citer_df_list = []
        for row in self.articles.itertuples():
            referenced_works = row.referenced_works
            reference_list = '|'.join(referenced_works[:50])
            if len(reference_list) < 1:
                continue
            query = f'works?filter=ids.openalex:{reference_list}'
            self.oa.build_query(query=query).build_extractor(refresh=False).build_transformer()
            self.references = self.oa.extract.rename(columns={'id': 'cited_id'}).drop_duplicates(subset='cited_id')
            self.references.insert(0, 'works_id', row.works_id)
            ref_df_list.append(self.references)
        ref_df = pd.concat(ref_df_list)
        self.cache_pandas['references'] = ref_df
        return self

        from collections import defaultdict
        works = self.db.read_db(table_name='articles').loc[:, ['works_id']]
        print(works.head())
        entity = 'works'
        master_dict = defaultdict(list)
        for j, work in enumerate(list(works.works_id)):
            if j % 250 == 0:
                print(f'{j = } -> {j}/{len(list(works.works_id))}')
            self.fm.frame_maker(entity={entity: None},
                                filtre={'cites': work},
                                select=['id', 'doi', 'display_name',
                                        'publication_year', 'type',
                                        'authorships', 'biblio', 'abstract_inverted_index',
                                        'cited_by_count', 'concepts', 'primary_location',
                                        'referenced_works', 'related_works'],
                                refresh=False)
            for name, df in self.fm.frame_dict.items():
                df['article_id'] = work
                master_dict[name].append(df)

    def extract_cited(self):

        ref_df_list = []
        for row in self.articles.itertuples():
            referenced_works = row.referenced_works
            reference_list = '|'.join(referenced_works[:50])
            if len(reference_list) < 1:
                continue
            query = f'works?filter=ids.openalex:{reference_list}'
            self.oa.build_query(query=query).build_extractor(refresh=False).build_transformer()
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
    pj.extract_works().keep_articles().extract_cited()


if __name__ == '__main__':
    main()
