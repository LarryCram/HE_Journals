import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import diskcache as dc

import toml
import tomli


class SetUp(object):
    """
    Set up the requests session and cache directory for this run
    """

    _instance = None
    session = None
    config = None
    fields = None
    cache = None
    per_page = None

    def __new__(cls):
        """
        uses __new__ as a singleton
        """
        print(f'SetUp().__new__ -> {cls._instance = }')
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls.per_page = '200'
            cls.config = cls.load_parameters()
            cls.open_session()
            cls.open_cache()
            cls.fields = cls.load_fields()
            print(f'{cls.per_page = }')
            print(f'{cls.config = }')
            print(f'{cls.fields.keys() = }')
            print(f'{cls.fields.items() = }')
            print(f'SetUp() after first __new__ -> {cls._instance = }')
        return cls._instance

    def __init__(self):
        pass

    @classmethod
    def open_session(cls):
        """
        New requests session with standard headers
        :return:
        """
        cls.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=10,
                connect=3,
                backoff_factor=0.1,
                status_forcelist=[443, 429, 500, 502, 503, 504], ))
        cls.session.mount(r'http://', adapter)
        cls.session.mount(r'https://', adapter)

    @classmethod
    def open_cache(cls):
        """
        Cache instance used ih this session
        :return:
        """
        cls.cache = dc.Index(cls.config['oax_cache'], size_limit=1_000_000_000_000)
        # print(f'CHECK CACHE ON OPENING: {cls.cache.check() = }')

    @classmethod
    def load_parameters(cls):
        """
        Read toml for config
        :return:
        """
        with open(r"config.toml", mode="rb") as fp:
            return tomli.load(fp)

    @classmethod
    def load_fields(cls):
        """
        read API for each entity and find list of fields
        :return:
        """
        fields = {}
        for entity in cls.config['entities']:
            fields[entity] = sorted(list(
                cls.session.get(rf'https://api.openalex.org/{entity}', timeout=6).json()['results'][0].keys()))
        cls.load_fields_toml()
        return fields

    @classmethod
    def load_fields_toml(cls):
        """
        If it does not exist, make a fields.toml configuration files and load it
        :return:
        """
        import tomli_w
        import os
        fields_tomli_file = r"./fields.toml"
        if os.path.exists(fields_tomli_file):
            return
        with open(fields_tomli_file, mode='wb') as w_file:
            tomli_w.dump(cls.fields, w_file)
        with open(fields_tomli_file, mode='rb') as r_file:
            toml.load(r_file)
