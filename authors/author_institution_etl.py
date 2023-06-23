from abc import ABCMeta, abstractmethod
import diskcache as dc
import pandas as pd
from collections import defaultdict, Counter
from string_grouper.string_grouper import StringGrouper, match_strings


class IAuthorInstitutionExtract(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def extract_authors_institutions(cls):
        ...

    @classmethod
    @abstractmethod
    def extract_authors_homonyms(cls):
        ...

    @classmethod
    @abstractmethod
    def institutions_match_canonical(cls):
        ...

class AuthorInstitutionExtract(IAuthorInstitutionExtract):

    def __init__(self, pandas_cache=None, core_cache=None, refresh=True):
        self.pandas_cache = dc.Cache(pandas_cache)
        self.core_cache = dc.Cache(core_cache)
        self.refresh = refresh
        self.works = None
        self.authorships = None
        self.authors = None
        self.authors_homonyms = None
        self.institutions = None
        self.canonical = None

    def extract_authors_institutions(self):
        self.works = self.pandas_cache['articles']
        self.works = self.works.set_index('works_id')
        if self.refresh:
            self.extract_authors()
            self.extract_institutions()
            self.pandas_cache['authors'] = self.authors
            self.pandas_cache['institutions'] = self.institutions
        self.authors = self.pandas_cache['authors']
        self.institutions = self.pandas_cache['institutions']

    def extract_authors(self):
        self.authorships = self.works.loc[:, 'authorships'].to_frame()
        self.authorships = self.authorships.explode('authorships')
        self.authors = pd.json_normalize(self.authorships['authorships'])
        self.authors.index = self.authorships.index
        self.authors.columns = [c.replace('author', 'author').replace('.', '_') for c in self.authors.columns]
        self.authors = self.authors.set_index('author_id', drop=True, append=True)
        return self

    def extract_authors_homonyms(self):
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
        self.authors_homonyms = homonyms

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

    def institutions_match_canonical(self):
        self.extract_institutions_canonical()
        self.match_institutions()

    def match_institutions(self):
        print(f'{self.canonical.type.unique() = }')
        canonical_names = self.canonical.loc[self.canonical.type == 'education', 'institutions_display_name'].tolist()
        print(f'{len(canonical_names) = }')
        journal_names = self.institutions.institution_display_name.tolist()
        print(f'{len(journal_names) = }')
        all_names = canonical_names + journal_names
        print(f'{len(all_names) = }')
        sg = StringGrouper(master=all_names)
        matched = sg.match_strings(master=canonical_names, duplicates=journal_names)
        print(matched)
        return self

    def extract_institutions_canonical(self):
        print(f'keys of core cache {list(self.core_cache.iterkeys()) = }')
        if 'canonical' in list(self.core_cache.iterkeys()):
            self.canonical = self.core_cache['canonical']
            print(f'loaded prepared canonical institutions from core cache {self.canonical.shape = }')
            return self
        self.canonical = self.core_cache['institutions']
        keep_columns = [c for c in self.canonical.columns
                        if 'international.display_name' not in c
                        and 'summary_stats' not in c
                        and 'x_concepts' not in c
                        and 'counts_by_year' not in c]
        self.canonical = self.canonical[keep_columns]
        self.canonical = self.canonical.rename(columns={'id': 'institutions_id', 'display_name': 'institutions_display_name'})
        self.canonical = self.canonical.set_index('institutions_id')
        print(f'{self.canonical.shape = }')
        self.canonical = self.canonical.explode('roles')
        temp = pd.json_normalize(self.canonical['roles'].tolist()).drop(columns='id')
        temp.index = self.canonical.index
        self.canonical = self.canonical.drop(columns='roles')
        self.canonical = pd.merge(self.canonical.reset_index(drop=False), temp.reset_index(drop=True)
                                  , left_index=True, right_index=True)
        self.core_cache['canonical'] = self.canonical.reset_index()
        print(f'self.canonical\n{self.canonical.head()}')
        self.core_cache['canonical'] = self.canonical
        return self


