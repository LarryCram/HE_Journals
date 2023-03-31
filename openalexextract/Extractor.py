import requests.exceptions

from openalexextract.SetUp import SetUp
from utils.chained_get import chained_get
from requests_toolbelt.utils import dump
import hashlib
import time


class Extractor(SetUp):
    """
    Extract query from OpenAlex API
    """

    def __init__(self):
        """
        Extractor for API, subclass of SetUp which creates the session
        """
        super(SetUp).__init__()
        self.session = SetUp().session
        self.cache = SetUp().cache

    def extractor(self, query: str = None, refresh: bool = None):
        """
        Try to retrieve from cache if refresh is False: return value if success, or go to API

        :param query:
        :param refresh:
        :return:
        """
        if not refresh:
            response = self.retrieve_from_cache(cache_key=self.cache_key(query=query))
            # print(f'{query = } {self.cache_key(query=query) = }')
            if response:
                return response
        response = self.run_cursor_to_end(query=query)
        if not response or len(response) == 0:
            print(f'EMPTY RESPONSE {query = }')
            return None
        # print(f'{query = } {self.cache_key(query=query) = }')
        self.include_in_cache(cache_key=self.cache_key(query=query), cache_value=response)
        return response

    def cache_key(self, query: str = None):
        """
        Generate hash key from query string
        :param query: API query string
        :return: hash key
        """
        return hashlib.md5(query.encode('utf-8')).hexdigest()

    def retrieve_from_cache(self, cache_key: str = None):
        """
        Retrieve query from cache if key exists
        :param cache_key: key to test
        :return: return query response or None
        """
        if cache_key not in list(self.cache):
            # [print(f'{cache_key = } {kk = }') for kk in sorted(list(self.cache))]
            print(f'retrieve_from_cache - return None - key not found {cache_key = }')
            # exit(99)
            return None
        response = self.cache[cache_key]
        return response

    def retrieve_from_web(self, query: str = None):
        """
        Retrieve query from web
        :param query: valid API query (validity not tested)
        :return: return query response or None
        :raise: OSError if response status is not 200
        """
        time.sleep(0.1)

        qq = f'{query}?mailto:Lawrence.Cram@anu.edu.au'
        try:
            response = self.session.get(qq, timeout=6)
        except requests.exceptions.TooManyRedirects as e:
            print(f'too may redirects {e = } {qq = }')
            return None
        response.raise_for_status()
        status_code = response.status_code
        # print(f'web retrieval {query = } {len(response.json()) = }')
        if status_code not in [200, ]:
            raise OSError(f'web retrieval failed {response.status_code = } {response.request.headers = }\n'
                          f'{qq = }')
        try:
            return response.json()
        except ValueError as e:
            print(f'web retrieval fails {e = } {qq = }')
            return None

    def include_in_cache(self, cache_key=None, cache_value=None):
        """
        Cache include (include cache_value at cache_key
        :param cache_key:
        :param cache_value:
        :return:
        """
        self.cache[cache_key] = cache_value

    def remove_from_cache(self, cache_key=None):
        """"
        Drop entry from cache if key exists
        :param cache_key: key to test
        """
        del self.cache[cache_key]

    def run_cursor_to_end(self, query: str = None):
        """
        Cycle over all the entire cursor sequence for the query and combine responses appropriately
        :param query:
        :return:
        """
        next_cursor = "*"
        results = []
        while next_cursor:
            _json = self.retrieve_from_web(
                query=f'{query}&cursor={next_cursor}&per_page=200&mailto=Lawrence.Cram@anu.edu.au')
            if not _json:
                print(f'query does not return a JSON object {query = }')
                return None

            # combine group_by, list or single entity responses differently
            if 'meta' not in _json:
                return _json
            elif 'group_by' in query:
                results.extend(_json['group_by'])
            else:
                results.extend(_json['results'])

            count = int(chained_get(_json, ['meta', 'count'], -1))
            next_cursor = chained_get(_json, ['meta', 'next_cursor'], None)
            not_done = count - len(results)
            print(f'{count = } {not_done = } {next_cursor = }')
            if count == -1 or not_done <= 1 or not next_cursor:
                break

        return results



