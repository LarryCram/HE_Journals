import os
from collections import defaultdict
import numpy as np
import pandas as pd

from utils.time_run import time_run
from utils.dbUtils import dbUtil

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

class ConceptLabels:

    def __init__(self, journal=None):
        self.concepts_master = None
        self.journal = journal
        self.data_dir = rf'{os.getcwd()}\data'
        print(f'{self.data_dir = }')
        self.db_core = dbUtil(db_name=f'{self.data_dir}/.db/core')
        self.db_journal = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def loader(self):
        temp = self.db_core.read_db(table_name='concepts')[['concepts_id', 'display_name']]
        concepts_master = dict(zip(temp.concepts_id, temp.display_name))
        [print(j, k, v) for j, (k, v) in enumerate(concepts_master.items()) if j < 8]
        self.concepts_master = concepts_master

    def idf_computation(self, df=None):
        print(df.head())
        idf = defaultdict(int)
        for k in df.concepts_id:
            idf[k] += 1
        print(f'{len(idf) = }')
        idf_ = idf.copy()
        for k, v in idf_.items():
            idf[k] = np.log10((1.0 + len(idf))/(1.0 + v)) + 1
        [print(j, k, v) for j, (k, v) in enumerate(sorted(idf.items(), key=lambda x: x[1], reverse=True)) if j < 16]
        return idf

    def tfidf_computation(self, df=None, idf=None):
        grouped = df.groupby('works_id')
        temp = []
        for works_id, group in grouped:
            temp.extend([float(s)*float(idf.get(c))
                         for c, s in zip(group.concepts_id, group.concepts_score)])
        df.insert(3, 'tfidf', temp)
        df = df[df.concepts_score.astype(float) > 0.1].sort_values('tfidf', ascending=False)
        df['display_name'] = df.concepts_id.map(self.concepts_master)

        df_hold = list()
        df_hold.append(df.groupby('works_id').head(3))
        df = pd.concat(df_hold)

        print(df.head())
        print(df.value_counts('display_name'))
        print(df.groupby('partition').display_name.count().head(16))

    def make_trial(self):
        articles = self.db_journal.read_db(table_name='articles')
        communities = self.db_journal.read_db(table_name='communities')
        c_dict = defaultdict(set)
        [c_dict[p].update({v1, v2})
         for p, v1, v2 in zip(communities.partition, communities.source_id, communities.target_id)]
        rev_c_dict = {f'https://openalex.org/{k}': v for v, lst in c_dict.items() for k in lst}
        print(len(rev_c_dict))
        concepts = self.db_journal.read_db(table_name='concepts')
        concepts = concepts.loc[concepts.works_id
                                .isin(articles.works_id), ['concepts_id', 'works_id', 'concepts_score']].copy()
        concepts['partition'] = concepts.works_id.map(rev_c_dict).fillna(-1).astype(int)
        print(f'{len(articles) = } {len(concepts) = }')
        return concepts

    def runner(self):
        self.loader()
        trial = self.make_trial()
        idf = self.idf_computation(df=trial)
        self.tfidf_computation(df=trial, idf=idf)


@time_run
# @profile_run
def main():

    cl = ConceptLabels(journal='HERD')
    cl.runner()


if __name__ == '__main__':
    main()
