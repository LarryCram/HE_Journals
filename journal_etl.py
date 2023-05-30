import os
import pandas as pd

from openalexextract.FrameMaker import FrameMaker
from utils.dbUtils import dbUtil
from utils.profile_run import profile_run
from utils.time_run import time_run


class ProcessJournal:

    def __init__(self, journal=None, journal_id=None, journal_issn=None):

        self.journal = journal
        self.journal_id = journal_id
        self.journal_issn = journal_issn
        self.fm = FrameMaker()
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir}')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def extract_works(self):
        entity = 'works'
        self.fm.frame_maker(cache_name=f'{entity}_{self.journal_id}',
                            entity={entity: None},
                            filtre={'primary_location.source.id': self.journal_id},
                            refresh=False)
        print(f'tables: {self.fm.frame_dict.keys() = }')
        for table_name, table in self.fm.frame_dict.items():
            print(f'loading works by source: {table_name = } {table.shape = }\n{table.columns}')
            # table.info()
            table.drop(columns='source_host_organization_lineage', errors='ignore', inplace=True)
            table.drop(columns='raw_affiliation_strings', errors='ignore', inplace=True)
            table.drop(columns='primary_location_source', errors='ignore', inplace=True)
            self.db.to_db(df=table, table_name=f'{table_name}_by_id')
            # self.db.to_db(df=table, table_name=f'{table_name}')

    def keep_articles(self):
        works = self.fm.frame_dict['works'].reset_index()
        temp = works.loc[:, ['works_id', 'doi', 'display_name',
                             'abstract', 'biblio_first_page', 'biblio_last_page']]
        number_of_references = self.fm.frame_dict['referenced_works'].value_counts('works_id').to_dict()
        temp['reference_count'] = temp.works_id.map(number_of_references)
        print(f'number_of_references {next(iter(number_of_references)) = }')
        print(f'works:\n{temp.head()}')
        mask = []
        for row in temp.itertuples():
            first, last = (row.biblio_first_page, row.biblio_last_page)
            page_length = 0
            if isinstance(first, str) and isinstance(last, str) and first.isdigit() and last.isdigit():
                page_length = int(last) - int(first)
            logic = self.isarticle(page_length, row.abstract,
                                   number_of_references[row.works_id], row.display_name)
            if logic:
                mask.append(True)
            else:
                mask.append(False)
        articles = works[mask].copy()
        self.db.to_db(df=articles, table_name='articles')
        not_articles = works[~works.index.isin(articles.index.values)]
        self.db.to_db(df=not_articles, table_name='not_articles')
        print(f'to database: {articles.shape = } {not_articles.shape = }')

    def isarticle(self, page_length, abstract, n_references, display_name):
        if any([term in display_name.lower() for term in
                             ['book review', 'errata', 'corrigend', 'correction']]):
            return False
        if page_length > 4:
            return True
        if isinstance(abstract, str):
            return True
        return False

    # def extract_citers(self):
    #     from collections import defaultdict
    #     works = self.db.read_db(table_name='articles').loc[:, ['works_id']]
    #     print(works.head())
    #     entity = 'works'
    #     master_dict = defaultdict(list)
    #     for j, work in enumerate(list(works.works_id)):
    #         if j % 250 == 0:
    #             print(f'{j = } -> {j}/{len(list(works.works_id))}')
    #         self.fm.frame_maker(entity={entity: None},
    #                             filtre={'cites': work},
    #                             select=['id', 'doi', 'display_name',
    #                                     'publication_year', 'type',
    #                                     'authorships', 'biblio', 'abstract_inverted_index',
    #                                     'cited_by_count', 'concepts', 'primary_location',
    #                                     'referenced_works', 'related_works'],
    #                             refresh=False)
    #         for name, df in self.fm.frame_dict.items():
    #             df['article_id'] = work
    #             master_dict[name].append(df)
    #
    #     for table_name, table_list in master_dict.items():
    #         table = pd.concat(table_list)
    #         table_name = f'citers_{table_name}'
    #         table.drop(columns='source_host_organization_lineage', errors='ignore', inplace=True)
    #         table.drop(columns='raw_affiliation_strings', errors='ignore', inplace=True)
    #         table.drop(columns='primary_location_source', errors='ignore', inplace=True)
    #         print(f'loading works by source: {table_name = } {table.shape = }\n{table.columns}')
    #         self.db.to_db(df=table, table_name=table_name)

    def extract_cited(self):
        from collections import defaultdict
        cited = self.db.read_db(table_name='referenced_works')
        works = self.db.read_db(table_name='articles').loc[:, ['works_id']]
        cited = cited[cited.works_id.isin(works.works_id)]
        entity = 'works'
        master_dict = defaultdict(list)
        grouped = cited.groupby('works_id')
        for j, (works_id, group) in enumerate(grouped, start=1):
            reference_lst = group.referenced_works.values
            if len(reference_lst) < 50:
                ref_lst = '|'.join(reference_lst)
            print(f'{j = } {works_id = } {reference_lst = } {ref_lst = }')

            # if j % 250 == 0:
            #     print(f'{j = } -> {j}/{len(list(cited.works_id))}')
            #     continue
            self.fm.frame_maker(cache_name=f'{entity}_{works_id}_references',
                                entity={entity: None},
                                filtre={'openalex': ref_lst},
                                refresh=False)
            for name, df in self.fm.frame_dict.items():
                master_dict[name].append(df)
            print(f'{j = } {works_id = }')
            exit(66)

        for table_name, table_list in master_dict.items():
            table = pd.concat(table_list)
            table_name = f'citers_{table_name}'
            table.drop(columns='source_host_organization_lineage', errors='ignore', inplace=True)
            table.drop(columns='raw_affiliation_strings', errors='ignore', inplace=True)
            table.drop(columns='primary_location_source', errors='ignore', inplace=True)
            print(f'loading works by source: {table_name = } {table.shape = }\n{table.columns}')
            self.db.to_db(df=table, table_name=table_name)

    def process_journal_runner(self):
        self.extract_works()
        # self.keep_articles()
        # self.extract_cited()
        # self.extract_citers()


@time_run
@profile_run
def main():
    journal = 'HERD'
    journal_id = 'S4210176587'
    journal_issn = '0729-4360'
    pj = ProcessJournal(journal=journal, journal_id=journal_id, journal_issn=journal_issn)

    pj.process_journal_runner()


if __name__ == '__main__':
    main()
