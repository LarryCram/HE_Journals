from abc import ABCMeta, abstractmethod
import json
import gzip
from pathlib import Path

import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

from openalexextract.openalex_etl import OpenalexEtl
import diskcache as dc

class IOpenalexCore(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def core_extractor(self):
        raise NotImplementedError

class OpenalexCore(IOpenalexCore):
    """
    build ExtractByTitle
    """

    def __init__(self, refresh=None):
        self.extract = None
        self.refresh = refresh
        self.oa_etl = OpenalexEtl()
        self.oa_etl.build_session()
        self.cache_pandas = dc.Cache('./data/.cache_pandas/CORE', size_limit=int(4e9))
        print(f'{self.cache_pandas = }')

    def core_extractor(self, entity=None):

        root = Path(f'{Path.cwd()}/data/openalex-snapshot/{entity}')
        f_list = list(root.glob('updated*/*.gz'))
        hold = []
        for f in f_list:
            print(f'{f = }')
            with gzip.open(f, 'r') as gz:
                response = gz.readlines()
                hold.extend(response)
        temp = []
        for item in hold:
            temp.append(pd.json_normalize(json.loads(item)))
        self.extract = pd.concat(temp)
        self.cache_pandas[entity] = self.extract

