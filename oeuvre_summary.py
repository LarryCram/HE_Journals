import os

from collections import Counter
from utils.dbUtils import dbUtil
from utils.time_run import time_run

from utils.profile_run import profile_run

import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

class OeuvreSummary:

    def __init__(self, journal=None):
        self.oeuvre_concepts = None
        self.concepts_idf = None
        self.oeuvre_works = None
        self.oeuvre_authorships = None
        self.authorships = None
        self.articles = None
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir = } {os.getcwd() = }')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def oeuvre_summary_runner(self):
        self.load_journal()
        self.load_oeuvre()
        # self.oeuvre_corpus_statistics()
        self.oeuvre_concepts_inverse_doc_freq()
        self.oeuvre_concepts_tfidf()
        self.oeuvre_separator()

    def load_journal(self):
        self.articles = self.db.read_db(table_name='articles')[[
            'works_id', 'display_name', 'publication_year', 'cited_by_count']]
        print(f'{self.articles.shape = }\n{self.articles.head()}')
        self.authorships = self.db.read_db(table_name='authorships')[[
            'works_id', 'author_id', 'author_display_name', 'institutions_id', 'display_name']]
        self.authorships.rename(columns={'display_name': 'institutions_display_name'}, inplace=True)
        # self.authorships.dropna(subset=['author_id'], inplace=True)
        print(f'{self.authorships.shape = }\n{self.authorships.head()}')
        self.articles = self.articles.merge(self.authorships, left_on='works_id', right_on='works_id')
        print(f'{self.articles.shape = }\n{self.articles.head()}')

    def load_oeuvre(self):
        self.oeuvre_works = self.db.read_db(table_name='oeuvre_works')[[
            'works_id', 'display_name', 'publication_year', 'cited_by_count']]
        self.oeuvre_authorships = self.db.read_db(table_name='oeuvre_authorships')[[
            'works_id', 'author_id', 'author_display_name', 'institutions_id', 'display_name']]
        self.oeuvre_authorships.rename(columns={'display_name': 'institutions_display_name'}, inplace=True)
        self.oeuvre_authorships = self.oeuvre_authorships[
            self.oeuvre_authorships.author_id.isin(self.authorships.author_id)]
        self.oeuvre_works = self.oeuvre_works.merge(self.oeuvre_authorships, left_on='works_id', right_on='works_id').\
            sort_values('publication_year')
        print(f'{self.oeuvre_authorships.shape = }\n{self.oeuvre_authorships.head()}')
        print(f'{self.oeuvre_works.shape = }\n{self.oeuvre_works.head()}')

    def oeuvre_corpus_statistics(self):
        c = Counter(self.oeuvre_authorships.author_display_name)
        print(f'{c.total() = }\n{c.most_common(10) = }\n{c.most_common()[:-11:-1] = }')
        v = Counter(sorted(c.values()))
        print(f'{v.total() = }\n{v.most_common(10) = }\n{v.most_common()[:-11:-1] = }')

    def oeuvre_separator(self):
        self.oeuvre_works['size'] = self.oeuvre_works.groupby('author_id').transform('size')
        print(self.oeuvre_works.head())
        groups = self.oeuvre_works.groupby('author_id')
        for j, (author_id, group) in enumerate(sorted(groups, key=lambda x: len(x[1]), reverse=True)):
            print(f"\nAUTHOR: {author_id = } {len(group) = }")
            group = group.sort_values('publication_year')
            for k, row in enumerate(group.itertuples()):
                if k > 2:
                    break
                print(f"{row.works_id} {row.publication_year} {row.display_name[:32]} "
                      f"{row.author_display_name} {row.institutions_display_name} "
                      f"{row.tfidf} {row.concepts_display_name}")
            if j > 16:
                break

    def oeuvre_concepts_inverse_doc_freq(self):
        self.oeuvre_concepts = self.db.read_db(table_name='oeuvre_concepts')
        c = Counter(list(self.oeuvre_concepts.concepts_display_name))
        n_docs = float(self.oeuvre_concepts.works_id.nunique())
        print(f'{n_docs = }')

        def idf(count):
            x = 1.0 + np.log10((1.0 + n_docs)/(1.0 + float(count)))
            return x

        concepts_idf = {concept: idf(count) for concept, count in c.items()}
        concepts_idf = dict(sorted(concepts_idf.items(), key=lambda x: x[1]))
        print(f'{list(concepts_idf.items())[:16]}')
        print(f'{list(concepts_idf.items())[-16:]}')
        self.concepts_idf = concepts_idf

    def oeuvre_concepts_tfidf(self):
        works = self.oeuvre_works.merge(self.oeuvre_concepts, left_on='works_id', right_on='works_id')
        works['tfidf'] = [self.concepts_idf.get(c, pd.NA) for c in works.concepts_display_name]
        works['tfidf_wtd'] = [0.0 if isinstance(s, type(pd.NA)) else float(c) * float(s)
                              for c, s in zip(works.tfidf, works.concepts_score)]
        works = works.sort_values('tfidf_wtd', ascending=False).drop_duplicates('works_id', keep='first')
        print(works.head())
        self.oeuvre_works = works


@time_run
# @profile_run
def main():

    oeuvres = OeuvreSummary(journal='HERD')
    oeuvres.oeuvre_summary_runner()


if __name__ == '__main__':
    main()