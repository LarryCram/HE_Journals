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

    def process_journal_runner(self):
        # self.extract_works()
        self.extract_citers()

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
            print(f'{table_name = }')
            table.info()
            self.db.to_db(df=table, table_name=table_name)

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
                except:
                    pass

        for name, df_list in master_dict.items():
            table = pd.concat(df_list)
            table_name = f'citers_{name}'
            print(f'{table_name = } {table.shape = }\n{table.head()}')
            self.db.to_db(df=table, table_name=table_name)


@time_run
@profile_run
def main():

    journal = 'HERD'
    journal_id = 'S4210176587'
    pj = ProcessJournal(journal=journal, journal_id=journal_id)

    pj.process_journal_runner()


if __name__ == '__main__':
    main()
