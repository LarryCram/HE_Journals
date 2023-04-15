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
        self.article = None
        self.Oeuvre = None
        self.oeuvre_concepts = None
        self.concepts_idf = None
        self.oeuvre_works = None
        self.oeuvre_authorships = None
        self.authorships = None
        self.article_authorships = None
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir = } {os.getcwd() = }')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def oeuvre_summary_runner(self):
        self.load_journal()
        self.load_oeuvre()
        self.homonym_detection()
        self.oeuvre_corpus_statistics()
        self.oeuvre_concepts_inverse_doc_freq()
        self.oeuvre_concepts_summary()
        # self.oeuvre_author_processor()

    def load_journal(self):
        self.article = self.db.read_db(table_name='articles')[[
            'works_id', 'display_name', 'publication_year', 'cited_by_count']]
        print(f'{self.article.shape = }\n{self.article.head()}')
        self.authorships = self.db.read_db(table_name='authorships')[[
            'works_id', 'author_id', 'author_display_name', 'institutions_id', 'institutions_display_name']]
        # self.authorships.dropna(subset=['author_id'], inplace=True)
        print(f'{self.authorships.shape = }\n{self.authorships.head()}')
        self.article_authorships = self.article.merge(self.authorships, left_on='works_id', right_on='works_id')
        print(f'{self.article_authorships.shape = }\n{self.article_authorships.head()}')

    def load_oeuvre(self):
        self.oeuvre_works = self.db.read_db(table_name='oeuvre_works')[[
            'works_id', 'display_name', 'publication_year', 'cited_by_count']]
        self.oeuvre_authorships = self.db.read_db(table_name='oeuvre_authorships')[[
            'works_id', 'author_id', 'author_display_name',
            'institutions_id', 'institutions_display_name', 'country_code']]
        self.oeuvre_authorships = self.oeuvre_authorships[
            self.oeuvre_authorships.author_id.isin(self.authorships.author_id)]
        self.oeuvre_works = self.oeuvre_works.merge(self.oeuvre_authorships, left_on='works_id', right_on='works_id').\
            sort_values('publication_year')
        self.oeuvre_works['size'] = self.oeuvre_works.groupby('author_id').transform('size')
        print(f'{self.oeuvre_authorships.shape = }\n{self.oeuvre_authorships.head()}')
        print(f'{self.oeuvre_works.shape = }\n{self.oeuvre_works.head()}')

    def oeuvre_corpus_statistics(self):
        c = Counter(self.oeuvre_authorships.author_display_name)
        print(f'{c.total() = }\n{c.most_common(10) = }\n{c.most_common()[:-11:-1] = }')
        v = Counter(sorted(c.values()))
        print(f'{v.total() = }\n{v.most_common(10) = }\n{v.most_common()[:-11:-1] = }')

    def homonym_detection(self):
        reject_list = []
        df = self.oeuvre_works.sort_values(by=['size', 'publication_year'], ascending=[False, True])
        grouped = df.groupby('author_id')
        for k, (author_id, group) in enumerate(grouped):
            author = group.author_display_name.to_list()[0]
        # implausible duration
            p_min, p_max = group['publication_year'].agg(['min', 'max']).to_list()
            duration = p_max - p_min
        # implausible mobility
            mob_max = group.groupby(['publication_year']).institutions_id.nunique().max()
        # implausible productivity
            prod_max = group.groupby(['publication_year']).works_id.nunique().max()
        # assemble rules
            if duration > 70 or mob_max > 10 or prod_max > 100:
                reject = True
            else:
                reject = False
            # print(f'{author_id = } {duration = } {mob_max = } {prod_max = } {reject = }')
            reject_list.append([author_id, author, duration, mob_max, prod_max, reject])

        rejecter = pd.DataFrame(reject_list, columns=['author_id', 'display_name',
                                                      'duration', 'mob_max', 'prod_max', 'reject'])
        print(f'{rejecter.shape = } '
              f'{rejecter.duration.max() = } '
              f'{rejecter.mob_max.max() = } '
              f'{rejecter.prod_max.max() = } {rejecter.author_id.count() = } '
              f'{rejecter[rejecter.reject].count().to_list() = }'
              f' \n{rejecter.sort_values("duration", ascending=False).head()}')
        self.db.to_db(df=rejecter, table_name='homonym_rejecter', if_exists='replace')

    def oeuvre_author_processor(self):
        oeuvre_list = []
        print(self.oeuvre_works.head())
        df = self.oeuvre_works
        grouped = df.groupby('author_id')
        oeuvre_dict = {author: group.works_id.values.tolist() for author, group in grouped}
        for j, (author, oeuvre_works) in enumerate(oeuvre_dict.items()):
            oeuvre = df[df.works_id.isin(oeuvre_works)]
            print(f'AUTHOR: {j = } {author = } {oeuvre.shape = }')  #\n{oeuvre.head()}')
            if lst := self.oeuvre_processor(author_id=author, df=oeuvre):
                oeuvre_list.extend(lst)
            # if j > 32:
            #     break
        oeuvre = pd.DataFrame(oeuvre_list, columns=['author_id', 'works_id',
                                                    'n_authors', 'n_institutions', 'n_countries',
                                                    'academic_age', 'career_stage'])
        print(f'oeuvre_summary {oeuvre.shape = }\n{oeuvre.head()}')
        self.db.to_db(df=oeuvre, table_name='oeuvre_summary')

    def oeuvre_processor(self, author_id=None, df=None):
        oeuvre_list = []
        first_publication_year = df.publication_year.sort_values().values[0]
        grouped = df.groupby('works_id')
        # print(f'{len(df) = } {len(grouped) = } {first_publication_year = }\n{df.publication_year.to_list() = }')
        for k, (works_id, group) in enumerate(grouped):
            # print(group.head())
            academic_age = group.publication_year.values[0] - first_publication_year
            # print(
            #     f'{k = } {works_id = } {group.display_name.values[0] = } {group.author_display_name.values[0] = } '
            #     f'{len(group) = } {group.publication_year.to_list() = }')
            n_authors = group.author_id.nunique()
            n_institutions = group.institutions_id.nunique()
            n_countries = group.country_code.nunique()
            # print(f'{n_authors = } {n_institutions = } {n_countries = } {academic_age = }')
            oeuvre_list.append([author_id, works_id, n_authors, n_institutions, n_countries,
                                academic_age, self.career_stage(academic_age)])
            # exit(99)
        return oeuvre_list

    def career_stage(self, academic_age=None):
        if 0 <= academic_age <= 1:
            return 'fyr'
        elif academic_age <= 5:
            return 'ecr'
        elif academic_age <= 15:
            return 'mcr'
        elif academic_age > 15:
            return 'sr'
        else:
            return False

    def oeuvre_concepts_inverse_doc_freq(self):
        self.oeuvre_concepts = self.db.read_db(table_name='oeuvre_concepts').drop(columns='index', errors='ignore')
        c = Counter(list(self.oeuvre_concepts.concepts_display_name))
        n_docs = float(self.oeuvre_concepts.works_id.nunique())
        print(f'{n_docs = }')

        def idf(count):
            x = 1.0 + np.log10((1.0 + n_docs)/(1.0 + float(count)))
            return float(x)

        concepts_idf = {concept: idf(count) for concept, count in c.items()}
        concepts_idf = dict(sorted(concepts_idf.items(), key=lambda x: x[1]))
        print(f'{list(concepts_idf.items())[:16]}')
        print(f'{list(concepts_idf.items())[-16:]}')
        self.concepts_idf = concepts_idf

    def oeuvre_concepts_summary(self):
        from collections import defaultdict
        concepts = defaultdict(float)
        concepts_tfidf = defaultdict(float)
        grouped = self.oeuvre_works.groupby('author_id')
        expertise_ = []
        for j, (author, group) in enumerate(grouped):
            author_concepts = self.oeuvre_concepts[self.oeuvre_concepts.works_id.isin(group.works_id)]
            author_concepts.insert(0, 'author_id', author)
            print(f'AUTHOR {author = } {len(group) = } {group.publication_year.to_list() = }')
            print(author_concepts.head())
            for concept, score in zip(author_concepts.concepts_display_name, author_concepts.concepts_score):
                s = 0.0 if isinstance(score, type(pd.NA)) else float(score)
                concepts[concept] += s
                concepts_tfidf[concept] += s * self.concepts_idf.get(concept, 0.0)
            concept, score = sorted(concepts.items(), key=lambda x: x[1], reverse=True)[1]
            print(concept, score)
            concept_tfidf, score_tfidf = sorted(concepts_tfidf.items(), key=lambda x: x[1], reverse=True)[1]
            print(concept_tfidf, score_tfidf)
            expertise_.append([author, concept, score, concept_tfidf, score_tfidf])
            concepts_time_series = self.oeuvre_works.loc[:, ['author_id', 'works_id', 'publication_year']].\
                merge(author_concepts, left_on=['works_id', 'author_id'], right_on=['works_id', 'author_id'])
            print(concepts_time_series.head())
            self.education_expertise(df=concepts_time_series)
            if j > 32:
                break

        expertise = pd.DataFrame(expertise_, columns=['author_id', 'concept', 'score', 'concept_tfidf', 'score_tfidf'])
        print(f'{expertise.shape = }/n{expertise.head(32)}')
        exit(23)

    def education_expertise(self, df=None):
        print(df[df.concepts_display_name.str.contains(r'education|pedagogy|teach|student', case=False)].head(32))
        return df[df.concepts_display_name.str.contains(r'education|pedagogy|teach|student', case=False)]


@time_run
# @profile_run
def main():

    oeuvres = OeuvreSummary(journal='HERD')
    oeuvres.oeuvre_summary_runner()


if __name__ == '__main__':
    main()