import os
import pandas as pd

from openalexextract.FrameMaker import FrameMaker
from utils.dbUtils import dbUtil
from utils.profile_run import profile_run
from utils.time_run import time_run


class ProcessJournal:

    def __init__(self, journal=None, journal_id=None):

        self.journal = journal
        self.journal_id = journal_id
        self.fm = FrameMaker()
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir}')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def extract_works(self):
        entity = 'works'
        self.fm.frame_maker(entity={entity: None},
                            filtre={'host_venue.id': self.journal_id},
                            select=['id', 'doi', 'display_name',
                                    'publication_year', 'type',
                                    'authorships', 'biblio', 'abstract_inverted_index',
                                    'cited_by_count', 'concepts', 'locations',
                                    'referenced_works', 'related_works', 'cited_by_api_url'],
                            refresh=False)
        print(f'tables: {self.fm.frame_dict.keys() = }')
        for table_name, table in self.fm.frame_dict.items():
            print(f'{table_name = } {table.shape = }')
            # table.info()
            self.db.to_db(df=table, table_name=table_name)

    def keep_articles(self):
        works = self.fm.frame_dict['works'].reset_index()
        temp = works.loc[:, ['works_id', 'doi', 'abstract', 'biblio_first_page', 'biblio_last_page']]
        number_of_references = self.fm.frame_dict['referenced_works'].value_counts('works_id').to_dict()
        print(number_of_references)
        print(temp.head())
        mask = []
        for row in temp.itertuples():
            first, last = (row.biblio_first_page, row.biblio_last_page)
            page_length = 0
            if isinstance(first, str) and isinstance(last, str) and first.isdigit() and last.isdigit():
                page_length = int(last) - int(first)
            # if isinstance(row.abstract, str) or page_length > 5 or number_of_references[row[0]] > 1:
            # print(row, number_of_references[row.works_id])
            logic = page_length > 5 or (isinstance(row.abstract, str) or (number_of_references[row.works_id] > 1))
            if logic:
                mask.append(True)
            else:
                mask.append(False)
        articles = works[mask]
        self.db.to_db(df=articles, table_name='articles')
        not_articles = works[~works.index.isin(articles.index)]
        self.db.to_db(df=not_articles, table_name='not_articles')

    def extract_citers(self):
        from collections import defaultdict
        works = self.db.read_db(table_name='works').loc[:, ['works_id']]
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
                                        'cited_by_count', 'concepts', 'locations',
                                        'referenced_works', 'related_works'],
                                refresh=False)

            for name, df in self.fm.frame_dict.items():
                try:
                    df.insert(0, 'cited_id', work)
                    master_dict[name].append(df)
                except Exception as e:
                    print(f'count not insert cited)id column {e = }')

        for name, df_list in master_dict.items():
            if 'locations' in name:
                continue
            table = pd.concat(df_list)
            table_name = f'citers_{name}'
            print(f'{table_name = } {table.shape = }\n{table.head()}')
            self.db.to_db(df=table, table_name=table_name)

    def process_journal_runner(self):
        self.extract_works()
        self.keep_articles()
        # self.extract_citers()


@time_run
@profile_run
def main():
    journal = 'HERD'
    journal_id = 'S4210176587'
    pj = ProcessJournal(journal=journal, journal_id=journal_id)

    pj.process_journal_runner()


if __name__ == '__main__':
    main()
