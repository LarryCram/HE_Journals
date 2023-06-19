from abc import ABCMeta, abstractmethod
import diskcache as dc
import pandas as pd
from collections import defaultdict, Counter


class IAuthorExtract(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def extract_authors_institutions(cls):
        ...

    @classmethod
    @abstractmethod
    def transform_authors(cls):
        ...

class AuthorExtract(IAuthorExtract):

    def __init__(self, pandas_cache=None):
        self.pandas_cache = dc.Cache(pandas_cache)
        self.works = None
        self.authorships = None
        self.authors = None
        self.author_homonyms = None
        self.institutions = None

    def extract_authors_institutions(self):
        self.extract_authorships()
        self.extract_institutions()

    def extract_authorships(self):
        self.works = self.pandas_cache['works']
        self.works = self.works.set_index('works_id')
        self.authorships = self.works.loc[:, 'authorships'].to_frame()
        self.authorships = self.authorships.explode('authorships')
        self.authors = pd.json_normalize(self.authorships['authorships'])
        self.authors.index = self.authorships.index
        self.authors.columns = [c.replace('author', 'author').replace('.', '_') for c in self.authors.columns]
        self.authors = self.authors.set_index('author_id', drop=True, append=True)
        return self

    def transform_authors(self):
        self.authors.info()
        print(self.authors.head())
        self.potential_homonyms()
        print(self.author_homonyms.head())

    def potential_homonyms(self):
        self.shorten_name()
        homonyms = self.authors.join(self.institutions).reset_index()\
            .groupby(['author_display_name', 'short_name', 'institution_display_name'])\
            .author_id.apply(lambda x: Counter(x).most_common()).to_frame()
        homonyms.columns = ['author_id_homonyms']
        homonyms['homonym_counts'] = [len(x) for x in homonyms.author_id_homonyms]
        homonyms = homonyms.sort_values('homonym_counts', ascending=False)
        homonyms['author_id_canonical'] = [x[0][0] for x in homonyms.author_id_homonyms]
        homonyms['author_id_alternates'] = \
            ['|'.join([x[k][0] for k in range(len(x))]) for x in homonyms.author_id_homonyms]
        self.author_homonyms = homonyms

    def shorten_name(self):
        self.authors['short_name'] = ['' if isinstance(n, float) else f'{n[0]} {n.rsplit(" ", maxsplit=1)[-1]}'
                                      for n in self.authors.author_display_name]

    def extract_institutions(self):
        temp = self.authors.loc[:, 'institutions'].to_frame()
        temp = temp.explode('institutions')
        self.institutions = pd.json_normalize(temp['institutions'])
        self.institutions.index = temp.index
        self.institutions = self.institutions\
            .rename(columns={'id': 'institution_id', 'display_name':'institution_display_name'})
        self.institutions = self.institutions.set_index('institution_id', drop=True, append=True)
        self.authors = self.authors.drop(columns='institutions')
        return self

