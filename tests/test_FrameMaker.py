import pandas as pd

from openalexextract.QueryMaker import QueryMaker
from openalexextract.Extractor import Extractor
from openalexextract.FrameMaker import FrameMaker

qm = QueryMaker()
ex = Extractor()
fm_class = FrameMaker()

driver = {
    'works': ({'host_venue.issn': '1469-8366', 'title.search': 'sociological'}, ['display_name', 'authorships', 'ids']),
    # 'authors': ({'display_name.search': 'lawrence cram'}, ['display_name', 'ids']),
    # 'venues': ({'display_name.search': 'phytoremediation'}, ['display_name', 'ids', 'issn']),
    # 'concepts': ({'display_name.search': 'higher education'}, ['display_name', 'ids']),
    # 'institutions': ({'display_name.search': 'western sydney'}, ['display_name', 'ids', 'geo']),
}
response_original = {}
fm = {}
for entity, fields in driver.items():
    print(f'{entity = } {fields = }')
    fm[entity] = FrameMaker()
    fm[entity].frame_maker(entity=entity,
                           filtre=fields[0],
                           select=fields[1])
    response_original[entity] = fm[entity].response
    print(f'{response_original[entity] = }')

# fm_class.df = pd.DataFrame([['Hello', ['hello', 'goodbye'], [100, ],
#                              [{'key1': 'value1', 'key2': 'value2'}],
#                              {'key1': {'kk1': 'vv1'}, 'key2': {'kk2': 'vv2'}}, ]],
#                            columns=['scalar', 'scalar_list', 'list', 'list_of_dict', 'dict'])
# print(f'{fm_class.df = }')


def test_abstract():
    abs_inv = {'test': [1, 3], 'me': [2, ]}
    for e in driver:
        assert 'test me test' == fm[e].abstract(abs_inv)


def test_replace_inverted_index():
    for e in driver:
        if 'abstract_inverted_index' in fm[e].response[0]:
            fm[e].replace_inverted_index()
            assert 'abstract' in fm[e].response[0]


def test_frame_from_response():
    for e in driver:
        fm[e].frame_from_response()
        print(f'\n{fm[e].df.head()}')
        assert isinstance(fm[e].df, pd.DataFrame)
        print(f'\n{fm[e].df.columns = }')


def test_classifier():
    # fm_class.df = pd.DataFrame([['Hello',
    #                              ['hello', 'goodbye'],
    #                              (list(range(10))),
    #                              [{'key1': 'value1', 'key2': 'value2'}, {'key3': 'value3', 'key4': 'value4'}],
    #                              {'key1': {'kk1': 'vv1'}, 'key2': {'kk2': 'vv2'}}]],
    #                            columns=['scalar', 'list_scalar', 'list_long', 'list_of_dict', 'dict'])
    # print(f'{fm_class.df = }')
    for e in driver:
        print(f'test_classifier: {e = }')
        for col in fm[e].df.columns:
            if col in ['ids']:
                assert fm[e].classifier(col) == 'dict'
            elif col in ['authorships']:
                assert fm[e].classifier(col) == 'list'
            else:
                assert fm[e].classifier(col) == 'scalar'


def test_classify_columns():
    # fm_class.df = pd.DataFrame([['Hello',
    #                              ['hello', 'goodbye'],
    #                              list(range(10)),
    #                              [{'key1': 'value1', 'key2': 'value2'}, {'key3': 'value3', 'key4': 'value4'}],
    #                              {'key1': {'kk1': 'vv1'}, 'key2': {'kk2': 'vv2'}}]],
    #                            columns=['scalar', 'list_scalar', 'list_long', 'list_of_dict', 'dict'])
    # fm_class.classify_columns()
    for e in driver:
        print(f'test_classify_columns: {e = }')
        fm_class = fm[e]
        for k, v in fm_class.classified.items():
            if k == 'dict':
                assert set(fm_class.classified[k]) - {'ids', } == set()
            elif k == 'list':
                assert set(fm_class.classified[k]) - {'authorships'} == set()
            elif k == 'scalar':
                assert set(fm_class.classified[k]) - {'id', 'display_name'} == set()
        print(f'\n{fm_class.df = }')
        print(f'\n{fm_class.classified.items() = }')


def test_make_dict_of_pandas():
    # fm_class.entity = 'TESTER'
    # fm_class.df = pd.DataFrame([['Hello',
    #                              ['hello', 'goodbye'],
    #                              list(range(10)),
    #                              [{'key1': 'value1', 'key2': 'value2'}, {'key3': 'value3', 'key4': 'value4'}],
    #                              {'key1': {'kk1': 'vv1'}, 'key2': {'kk2': 'vv2'}}]],
    #                            columns=['scalar', 'list_scalar', 'list_long', 'list_of_dict', 'dict'])
    # print(f'\n{fm_class.df = }')
    for e in driver:
        print(f'test_make_dict_of_pandas: {e = }')
        fm_class = fm[e]
        fm_class.classify_columns()
        fm_class.make_dict_of_pandas()
        assert 'ids' in fm_class.df_dict
        assert 'authorships' in fm_class.df_dict
        # assert 'id' in fm_class.df_dict
        # assert 'display_name' in fm_class.df_dict
        assert fm_class.entity in fm_class.df_dict


def test_make_frames_from_pandas():
    # fm_class.entity = 'TESTER'
    # fm_class.df = pd.DataFrame([['Hello',
    #                              ['hello', 'goodbye'],
    #                              list(range(10)),
    #                              [{'key1': 'value1', 'key2': 'value2'}, {'key1': 'value3', 'key2': 'value4'}],
    #                              {'key1': {'kk1': 'vv1'}, 'key2': {'kk2': 'vv2'}}],
    #                             ['Hello1',
    #                              ['hello1', 'goodbye1'],
    #                              list(range(10)),
    #                              [{'key1': 'value1', 'key2': 'value2'}, {'key1': 'value3', 'key2': 'value4'}],
    #                              {'key1': {'kk1': 'vv1'}, 'key2': {'kk2': 'vv2'}}]
    #                             ],
    #                            columns=['scalar', 'list_scalar', 'list_long', 'list_of_dict', 'dict'])
    # print(f'\n{fm_class.df = }')
    for e in driver:
        fm_class = fm[e]
        fm_class.classify_columns()
        fm_class.make_dict_of_pandas()
        fm_class.make_frames_from_pandas()
        assert True


def test_make_frame_from_list():
    assert True


def test_make_frame_from_dict():
    assert True
