import os

from openalexextract.FrameMaker import FrameMaker
from utils.dbUtils import dbUtil
from utils.profile_run import profile_run
from utils.time_run import time_run


class ProcessJournal:

    def __init__(self, journal=None):
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir}')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')


@time_run
@profile_run
def main():

    fm = FrameMaker()
    journal_id = 'S4210176587'
    entity = 'works'
    fm.frame_maker(entity={entity: None},
                   filtre={'host_venue.id': journal_id},
                   select=['id', 'doi', 'display_name',
                           'publication_year', 'type',
                           'authorships', 'biblio', 'abstract_inverted_index',
                           'cited_by_count', 'concepts', 'locations',
                           'referenced_works', 'related_works'],
                   refresh=False)
    pj = ProcessJournal(journal='HERD')
    print(f'tables: {fm.frame_dict.keys() = }')
    for table_name, table in fm.frame_dict.items():
        print(f'{table_name = }')
        table.info()
        pj.db.to_db(df=table, table_name=table_name)


if __name__ == '__main__':
    main()
