from openalexextract.AARCHIVE.QueryMaker import QueryMaker

qm = QueryMaker()


def test_validity_check():
    assert qm.validity_check(entity='concepts', filtre={'display_name.search': 'education'}, select=['ancestors', 'id'])


def test_build_query():
    query = qm.query_maker(entity='concepts', filtre={'display_name.search': 'education'}, select=['ancestors', 'id'])
    print(f'\n{query = }')
    assert '?' in query and '&' in query and ',' in query


def test_query_maker():
    del [qm.entity, qm.filtre, qm.select]
    query = qm.query_maker(entity='concepts', filtre={'display_name.search': 'education'}, select=['ancestors', 'id'])
    assert '?' in query and '&' in query and ',' in query
    del [qm.entity, qm.filtre, qm.select]
    query = qm.query_maker(entity='concepts', filtre={'display_name.search': 'education'})
    assert '?' in query
    del [qm.entity, qm.filtre]
    query = qm.query_maker(entity='concepts', select=['ancestors', 'id'])
    assert '?' in query and '&' in query and ',' in query


