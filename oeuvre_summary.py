import os

from collections import Counter, defaultdict
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
        self.expertise = None
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

    def load_articles(self):
        self.article = self.db.read_db(table_name='articles')[[
            'works_id', 'display_name', 'publication_year', 'cited_by_count']]
        # print(f'{self.article.shape = }\n{self.article.head()}')
        self.authorships = self.db.read_db(table_name='authorships')[[
            'works_id', 'author_id', 'author_display_name', 'institutions_id', 'institutions_display_name']]
        self.article_authorships = self.article.merge(self.authorships, left_on='works_id', right_on='works_id')
        self.db.to_db(df=self.article_authorships, table_name='article_authorships')
        cols = ['works_id', 'author_id', 'author_display_name', 'institutions_display_name']
        print(f"{self.article_authorships.shape = }\n{self.article_authorships[cols].head()}")

    def load_oeuvre(self):
        # extract oeuvre_works
        self.oeuvre_works = self.db.read_db(table_name='oeuvre_works')[[
            'works_id', 'display_name', 'publication_year', 'cited_by_count']].drop_duplicates(subset='works_id')
        article_works_set = set(self.article.works_id.to_list())
        self.oeuvre_works['is_journal'] = \
            [work in article_works_set for work in self.oeuvre_works.works_id]
        # extract oeuvre_authorships
        self.oeuvre_authorships = self.db.read_db(table_name='oeuvre_authorships')[[
            'works_id', 'author_id', 'author_display_name',
            'institutions_id', 'institutions_display_name', 'country_code']]
        print(f'raw Oeuvres {self.oeuvre_works.shape = } {self.oeuvre_authorships.shape = }\n'
              f'{self.oeuvre_works.head()}')
        # drop authorships where the author is not in the journal article
        journal_authors_set = set(self.authorships.author_id.to_list())
        self.oeuvre_authorships = self.oeuvre_authorships[self.oeuvre_authorships.author_id.isin(journal_authors_set)]
        print(f'Drop oeuvre work authors who are not HERD authors {self.oeuvre_authorships.shape = }\n'
              f'{self.oeuvre_authorships.head()}')
        # merge the oeuvre works/authorships
        self.oeuvre_works = self.oeuvre_authorships.set_index(['author_id', 'works_id'])\
            .join(self.oeuvre_works.set_index('works_id'), how='left').reset_index()
        self.oeuvre_works = self.oeuvre_works[['works_id', 'display_name', 'publication_year', 'cited_by_count',
                                               'is_journal', 'author_id', 'author_display_name',
                                               'institutions_id', 'institutions_display_name', 'country_code']].copy()
        self.oeuvre_works['size'] = self.oeuvre_works.groupby('author_id').transform('size')
        self.oeuvre_works['size_journal'] = self.oeuvre_works[['works_id', 'is_journal', 'author_id']]\
            .drop_duplicates().groupby('author_id').is_journal.transform(lambda x: [1 for c in x if c].count(1))
        cols = ['works_id', 'author_id', 'author_display_name', 'institutions_display_name']
        print(f'Oeuvre with at least one HERD author in each work {self.oeuvre_works.shape = }'
              f'\n{self.oeuvre_works[cols].head()}')
        self.db.to_db(df=self.oeuvre_works,
                      table_name='oeuvre_full', if_exists='replace')

    def oeuvre_corpus_statistics(self):
        oeuvre = self.oeuvre_works[['works_id', 'author_id', 'author_display_name',
                                    'publication_year', 'cited_by_count',
                                    'institutions_id', 'institutions_display_name']]\
            .sort_values('publication_year').copy()
        oeuvre['works_per_author'] = oeuvre.groupby('author_id').works_id.transform('count')
        oeuvre['earliest'] = oeuvre.groupby('author_id').publication_year.transform('min')
        oeuvre['latest'] = oeuvre.groupby('author_id').publication_year.transform('max')
        oeuvre['citations'] = oeuvre.groupby('author_id').cited_by_count.transform('sum')
        oeuvre = oeuvre[['author_id', 'author_display_name', 'works_per_author', 'citations', 'earliest', 'latest']]\
            .sort_values('works_per_author', ascending=False).drop_duplicates(subset=['author_id'])
        oeuvre['publication_rate'] = [int(p/(l - e)) if l-e != 0 else pd.NA
                                      for p, e, l in zip(oeuvre.works_per_author, oeuvre.earliest, oeuvre.latest)]
        print(f'{oeuvre.shape = }\n{oeuvre.head(16)}')
        print(f'{len(oeuvre[oeuvre.works_per_author < 50]) = } '
              f'{len(oeuvre) = } {len(oeuvre[oeuvre.publication_rate < 5]) = }')

    def synonym_detection_full_name(self):
        seeker = defaultdict(set)
        for name, author_id in zip(self.oeuvre_works.author_display_name,
                                   self.oeuvre_works.author_id):
                                   # self.oeuvre_works.institutions_display_name):
            if not isinstance(name, str):
                continue
            seeker[name].add(author_id)
        synonym_list = []
        for j, (k, v) in enumerate(seeker.items()):
            synonym_list.append([k] + list(v))
            if j % 100 == 0:
                print(j, k, v, list(v), [k] + list(v))
        print(synonym_list[:4])
        synonym_df = pd.DataFrame(synonym_list)
        print(synonym_df.head())
        print(synonym_df.columns)
        print(synonym_df.shape)
        synonym_df.columns = ['display_name'] + [f'author_id_{c}' for c in range(synonym_df.shape[1]-1)]
        print(synonym_df.head())
        self.db.to_db(df=synonym_df, table_name='synonyms')
        self.synonym_detection_analysis_std_name(synonym_df=synonym_df)

    def synonym_detection_analysis_std_name(self, synonym_df=None):
        print(f'{synonym_df.shape = }\n{synonym_df.head()}')
        synonym_ = defaultdict(set)
        for name, author_id in zip(synonym_df.display_name, synonym_df.author_id_0):
            if " " in name:
                first, last = name.rsplit(" ", maxsplit=1)
                synonym_[f'{first.lower()[:1]}_{last.lower()}'].add((author_id, name))
        synonym_list = []
        for k, v in synonym_.items():
            if len(v) > 1:
                print(k, v, ['| '.join([n, i]) for n, i in v])
                synonym_list.append([k] + ['| '.join([n, i]) for n, i in v])
        synonym_df_ = pd.DataFrame(synonym_list)
        synonym_df_.columns = synonym_df.columns
        print(synonym_df_.head())
        self.db.to_db(df=synonym_df_, table_name='potential_synonyms')

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
            mob_max = group.institutions_id.nunique()
            mob_max = 0
        # implausible production
            prod_max = group.works_id.nunique()
        # implausible annual production
            rate = group.works_id.nunique()/duration if duration > 1 else 0
        # assemble rules
            if duration > 70 or mob_max > 100 or prod_max > 1000 or rate > 50:
                reject = True
            else:
                reject = False
            # print(f'{author_id = } {duration = } {mob_max = } {prod_max = } {reject = }')
            journal_count = self.article_authorships[self.article_authorships.author_id == author_id]\
                .drop_duplicates(subset='works_id').works_id.count()
            reject_list.append([author_id, author, duration, mob_max, prod_max, rate, journal_count, reject])
        rejecter = pd.DataFrame(reject_list, columns=['author_id', 'display_name',
                                                      'duration', 'mob_max', 'prod_max', 'rate',
                                                      'journal_count', 'reject'])
        print(f'{rejecter.shape = } '
              f'{rejecter.duration.max() = } '
              f'{rejecter.mob_max.max() = } '
              f'{rejecter.prod_max.max() = },'
              f'{rejecter.rate.max() = } {rejecter.author_id.count() = } '
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
            if j > 16:
                break
        cols = ['author_id', 'works_id',
                             'n_authors', 'n_institutions', 'n_countries',
                             'authors', 'institutions', 'countries',
                             'year', 'academic_age', 'career_stage']
        print(f'{cols = }')
        oeuvre = pd.DataFrame(oeuvre_list, columns=cols)
        print(f'{oeuvre.columns = }')
        print(f'oeuvre_summary {oeuvre.shape = }\n{oeuvre.head()}')
        self.db.to_db(df=oeuvre, table_name='oeuvre_summary')

    def oeuvre_processor(self, author_id=None, df=None):
        oeuvre_list = []
        first_publication_year = df.publication_year.sort_values().values[0]
        grouped = df.groupby('works_id')
        for k, (works_id, group) in enumerate(grouped):
            year = group.publication_year.values[0]
            academic_age = year - first_publication_year
            n_authors = group.author_id.nunique()
            n_institutions = group.institutions_id.nunique()
            n_countries = group.country_code.nunique()
            authors = self.joiner(group.author_display_name.to_list())
            institutions = self.joiner(group.institutions_display_name.to_list())
            countries = self.joiner(group.country_code.to_list())
            oeuvre_list.append([author_id, works_id, n_authors, n_institutions, n_countries,
                                authors, institutions, countries,
                                year, academic_age, self.career_stage(academic_age)])
        return oeuvre_list

    def joiner(self, lst=None):
        if isinstance(lst, list):
            try:
                return '| '.join([ll for ll in lst if isinstance(ll, str)])
            except:
                return '| '.join([' '.join(ll) for ll in lst if isinstance(ll, tuple)])
        return pd.NA

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
        grouped = self.oeuvre_works.groupby('author_id')
        expertise_ = []
        for j, (author, group) in enumerate(grouped):
            concepts = defaultdict(float)
            author_concepts = self.oeuvre_concepts[self.oeuvre_concepts.works_id.isin(group.works_id)]
            author_concepts.insert(0, 'author_id', author)
            print(f'AUTHOR {author = } {len(group) = } {group.publication_year.to_list() = }')
            print(author_concepts.head())
            for concept, score in zip(author_concepts.concepts_display_name, author_concepts.concepts_score):
                s = 0.0 if isinstance(score, type(pd.NA)) else float(score)
                # concepts[concept] += s
                concepts[concept] += s * self.concepts_idf.get(concept, 0.0)
            concepts_ = list({k: v for k, v in sorted(concepts.items(), key=lambda x: x[1], reverse=True)})[:6]
            ed_ex = self.education_expertise(concepts_)
            concepts_list = self.joiner(concepts_)
            expertise_.append([author, concepts_list, ed_ex])
            if j > 16:
                break
        self.expertise = pd.DataFrame(expertise_, columns=['author_id', 'concepts', 'education'])
        print(f'{self.expertise.shape = }/n{self.expertise.head(16)}')
        self.db.to_db(df=self.expertise, table_name='expertise')

    def education_expertise(self, concepts=None):

        trial = set(['education', 'pedagogy', 'teach', 'student'])
        print(f'{concepts = } {trial = }')
        for concept in concepts:
            for t in trial:
                print(f'{concept.lower() = } {t = } {t in concept.lower()}')
                if t in concept.lower():
                    return True
        return False

    def oeuvre_summary_runner(self):
        self.load_articles()
        self.load_oeuvre()
        self.oeuvre_corpus_statistics()
        # self.homonym_detection()
        # self.synonym_detection_full_name()

        self.oeuvre_concepts_inverse_doc_freq()
        self.oeuvre_concepts_summary()
        self.oeuvre_author_processor()


@time_run
# @profile_run
def main():
    oeuvres = OeuvreSummary(journal='HERD')
    oeuvres.oeuvre_summary_runner()


if __name__ == '__main__':
    main()