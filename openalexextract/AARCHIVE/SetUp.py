import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    per_page = None

    def __new__(cls, for_code=None):
        """
        uses __new__ as a singleton
        """
        print(f'SetUp().__new__ -> {cls._instance = }')
        if cls._instance is None:
            cls.for_code = for_code
            cls._instance = object.__new__(cls)
            cls.per_page = '200'
            cls.config = cls.load_parameters()
            cls.open_session()
            cls.fields = cls.load_fields()
            print(f'{cls.per_page = }')
            print(f'{cls.config = }')
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
                # cls.session.get(rf'https://api.openalex.org/{entity}', timeout=6).json()['results'][0].keys()))
                requests.get(rf'https://api.openalex.org/{entity}', timeout=6).json()['results'][0].keys()))
        return fields

