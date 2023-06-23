from abc import ABCMeta, abstractmethod

import re
import os
import time
import pandas as pd
import requests
import diskcache as dc
from collections import defaultdict


class IOpenalexEtl(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def build_session():
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def build_cache(cache):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def build_extractor(refresh):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def build_transformer():
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def build_loader():
        raise NotImplementedError


class OpenalexEtl(IOpenalexEtl):
    """
    build ExtractByTitle
    """

    def __init__(self):
        self.session = None
        self.query = None
        self.cache = None
        self.extract = None
        self.response = None
        self.result = []

    def build_session(self):
        params = {'per_page': 200, 'mailto': 'Lawrence.Cram@cdu.edu.au'}
        self.session = requests.Session()
        self.session.params = params
        return self

    def build_cache(self, cache=None):
        if not os.path.exists(cache):
            os.mkdir(cache)
        self.cache = dc.Cache(cache, size_limit=int(4e9))
        print(f'build_cache {cache = } {self.cache = }')
        return self

    def build_query(self, query=None):
        self.query = rf'https://api.openalex.org/{query}'
        return self

    def build_extractor(self, refresh=False):

        if not refresh:
            self.result = self.cache.get(self.query, default='KEY_NOT_FOUND', read=True)
            if self.result != 'KEY_NOT_FOUND':
                print(f'from cache: {self.query = }')
                return self
        self.run_cursor_to_end(query=self.query)
        print(f'from api: {self.query = }')
        self.cache[self.query] = self.result
        return self

    def run_cursor_to_end(self, query: str = None):
        """
        Cycle over all the entire cursor sequence for the query and combine responses appropriately
        :param query:
        :return:
        """
        next_cursor = "*"
        self.result = []
        while next_cursor:
            time.sleep(0.2)
            response = self.session.get(f'{query}&cursor={next_cursor}')
            if response.status_code not in [200, ]:
                err = rf'session.get error {response.status_code = } {self.query = } {response.request.url = }'
                raise RuntimeError(err)
            response = response.json()
            # combine group_by, list or single entity responses differently
            if 'meta' not in response:
                self.result = [response]
                return self
            elif 'group_by' in query:
                self.result.extend(response['group_by'])
            else:
                self.result.extend(response['results'])
            count = int(response['meta']['count'])
            next_cursor = response['meta']['next_cursor']
            not_done = count - len(self.result)
            print(f'{count = } {not_done = } {next_cursor = }')
            if count == -1 or not_done <= 1 or not next_cursor:
                break
        return self

    def build_transformer(self):
        if self.result:
            self.result = self.replace_inverted_index(self.result)
            # print(f'{len(self.result) = }')
            # study = defaultdict(list)
            # for item in self.result:
            #     study[item.get('id')].append(item.get('publication_year'))
            # print(study)
            self.extract = pd.json_normalize(self.result, max_level=3)
            # self.extract.info()
            self.extract.columns = [c.replace('.', '_') for c in self.extract.columns]
            # exit(55)
        return self

    def build_loader(self, load_dataframe=None):
        self.extract.to_parquet(load_dataframe)
        return self

    def replace_inverted_index(self, response):
        abstract_string = 'abstract_inverted_index'
        if isinstance(response, type(None)) or len(response) < 1 or abstract_string not in response[0]:
            # print(f'\n{abstract_string = } not found in first item of response list')
            return response
        new_response = []
        for item in response:
            if abstract_string in item.keys():
                abstract_inverted = item.pop(abstract_string)
                abstract = self.abstract(abstract_inverted)
                item |= {'abstract': abstract}
            new_response.append(item)
        return new_response

    @staticmethod
    def abstract(dd: dict = None) -> object:
        if isinstance(dd, (type(pd.NA), type(None), float,)):
            return
        try:
            abstract_dict = {pos: word for word, posList in dd.items() for pos in
                             posList}
            abstract = ' '.join([abstract_dict[pos] for pos in sorted(abstract_dict)])
            abstract = re.sub('^abstract *?', '', abstract, count=1, flags=re.I)
            s = abstract.split(" ")
            abstract = " ".join([i for i in s if not re.findall("[^\u0000-\u05C0\u2100-\u214F]+", i)])
            return abstract.strip()
        except:
            raise ValueError(f'exception in construction of abstract from inverted index ({dd = })')

