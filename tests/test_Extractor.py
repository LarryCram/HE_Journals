from openalexextract.Extractor import Extractor

ex = Extractor()
query = r"https://api.openalex.org/works/W1775749144"

def test_init():
    assert isinstance(ex, Extractor)


def test_extractor():
    response = ex.extractor(query=query)
    assert response['id'][-11:] == query[-11:]


def test_cache_key():
    cache_key = '968ef6dd597180190e782273117770ad'
    assert cache_key == ex.cache_key(query)


def test_retrieve_from_cache():
    cache_key = None
    assert ex.retrieve_from_cache(cache_key=cache_key) is None
    cache_key = '968ef6dd597180190e782273117770ad'
    assert ex.retrieve_from_cache(cache_key=cache_key) is not None


def test_retrieve_from_web():
    assert ex.retrieve_from_web(query=query) is not None


def test_include_in_cache():
    ex.include_in_cache(cache_key='key_for_test', cache_value='value_for_test')
    assert ex.cache['key_for_test'] == 'value_for_test'


def test_remove_from_cache():
    ex.include_in_cache(cache_key='key_for_test', cache_value='value_for_test')
    ex.remove_from_cache(cache_key='key_for_test')
    assert ex.cache.get('key_for_test', None) is None


def test_run_cursor_to_end():
    assert ex.run_cursor_to_end(query=query) is not None
