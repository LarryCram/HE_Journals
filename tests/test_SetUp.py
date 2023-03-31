from openalexextract.SetUp import SetUp
from diskcache import Cache

su = SetUp()


def test_new_session():
    assert 'User-Agent' in su.session.headers


def test_new_cache():
    assert isinstance(su.cache, Cache)


def test_load_parameters():
    assert 'user_email' in su.load_parameters().keys()


def test_load_fields():
    for entity in []:
        assert entity in su.fields


def test_load_fields_toml():
    import os
    # su.load_fields_toml()
    toml_path = r'fields.toml'
    assert os.path.exists(toml_path)

