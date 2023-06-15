import pandas as pd

from openalexextract.AARCHIVE.FrameMaker import FrameMaker
from utils.dbUtils import dbUtil
from utils.profile_run import profile_run
from utils.time_run import time_run

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ConceptSourceInstitutionExtractor:

    def __init__(self):
        self.db = dbUtil(r'data/.db/core')

    def get_concept(self):
        fm = FrameMaker()
        entity = 'concepts'
        fm.frame_maker(entity={entity: None},
                       filtre=None,
                       refresh=False)

        for k, df in fm.frame_dict.items():
            table_name = k if k.startswith(entity) else f'{entity}_{k}'
            df.info(verbose=True)
            self.db.to_db(df=df, table_name=table_name)

    def get_source(self):
        fm = FrameMaker()
        entity = 'sources'
        fm.frame_maker(entity={entity: None},
                       filtre=None,
                       refresh=False)

        for k, df in fm.frame_dict.items():
            if 'apc_prices' in df.columns:
                df['apc_prices'] = pd.NA
                df['apc_usd'] = pd.NA
            table_name = k if entity in k else f'{entity}_{k}'
            print(f'{k = } {df.shape = } {df.columns = }\n{df.head()}')
            self.db.to_db(df=df, table_name=table_name)

    def get_institution(self):
        fm = FrameMaker()
        entity = 'institutions'
        fm.frame_maker(entity={entity: None},
                       filtre=None,
                       refresh=False)

        for k, df in fm.frame_dict.items():
            table_name = k if entity in k else f'{entity}_{k}'
            print(f'{k = } {df.shape = } {df.columns = }\n{df.head()}')
            self.db.to_db(df=df, table_name=table_name)

    def csi_runner(self):
        # self.get_concept()
        # self.get_source()
        self.get_institution()

@time_run
@profile_run
def main():

    csi = ConceptSourceInstitutionExtractor()
    csi.csi_runner()


if __name__ == '__main__':
    main()
