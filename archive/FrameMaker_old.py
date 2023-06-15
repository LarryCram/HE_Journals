import pandas as pd
from collections import defaultdict
from benedict import benedict

from openalexextract.AARCHIVE.QueryMaker import QueryMaker
from openalexextract.AARCHIVE.Extractor import Extractor

qm = QueryMaker()
ex = Extractor()


class FrameMaker(object):
    """
    Prepare dictionary of pandas DataFrames from OpenAlex by entity and selected fields
    """

    def __init__(self):
        self.entity_id = None
        self.refresh = None
        self.inverse_classified = None
        self.df_dict = None
        self.classified = None
        self.df = None
        self.response = None
        self.query = None
        self.select = None
        self.filtre = None
        self.entity = None

    def frame_maker(self, entity=None, filtre=None, select=None, refresh=None):

        self.entity = entity
        self.filtre = filtre
        self.select = select
        self.refresh = refresh
        self.query = qm.query_maker(entity=entity, filtre=filtre, select=select)
        self.response = ex.extractor(query=self.query, refresh=refresh)
        self.replace_inverted_index()

        self.dict_from_response()

        self.frame_from_response()
        self.classify_columns()
        self.make_dict_of_pandas()
        self.make_frames_from_pandas()
        # for k, v in self.df_dict.items():
        #     print(f'{k = } {v.shape = }\n{v.head()}')

    def replace_inverted_index(self):
        abstract_string = 'abstract_inverted_index'
        if abstract_string not in self.response[0]:
            print(f'\n{abstract_string = } not found in first item of response list')
            return None
        new_response = []
        for item in self.response:
            if abstract_string in item.keys():
                abstract_inverted = item.pop(abstract_string)
                abstract = self.abstract(abstract_inverted)
                # print(f'\n{abstract = }')
                item |= {'abstract': abstract}
            new_response.append(item)
        self.response = new_response

    @staticmethod
    def abstract(dd: dict = None) -> object:
        if isinstance(dd, (type(pd.NA), type(None), float, )):
            return
        try:
            abstract_dict = {pos: word for word, posList in dd.items() for pos in
                             posList}
            return ' '.join([abstract_dict[pos] for pos in sorted(abstract_dict)])
        except:
            raise ValueError(f'exception in construction of abstract from inverted index ({dd = })')

    def dict_from_response(self):
        print(f'{self.response[0] = }')
        for row in self.response:
            self.process_json_row(row)

    @staticmethod
    def specify_id(entity, dd):
        if 'id' in dd:
            entity_id = f'{entity}_id'
            dd.update({entity_id: dd['id']})
            del dd['id']
        return dd

    def process_json_row(self, row):
        d = benedict.from_json(row, strict=False).flatten()
        d = self.specify_id(self.entity, d)
        d_copy = d.deepcopy()
        for (k, v) in d_copy.items():
            if not isinstance(v, list):
                print(f'field is scalar {k = } {v = }')
            else:
                field = d.pop(k)
                if not isinstance(field[0], dict):
                    print(f'field is list of items: {k = } {field = }')
                else:
                    field = [f.flatten() for f in field]
                    field = [self.specify_id(k, f) for f in field]
                    if has_list := [k1 for k1, v1 in field[0].items() if isinstance(v1, list)]:
                        field, new_field = self.process_embedded_list(has_list, field)
                    print(f'old fields: {k = } {field = }')

    def process_embedded_list(self, has_list, field):
        field_copy = field.copy()
        field = []
        sub_field = []
        if len(has_list) > 1:
            raise ValueError(f'the list of items that are lists not dict items is > 1 {has_list = }')
        for k1 in has_list:
            for jj, row in enumerate(field_copy):
                field_id = self.fetch_id_dict(row)
                sub_rows = row.pop(k1)
                for sub_ in sub_rows:
                    sub_ = self.specify_id(k1, sub_)
                    if field_id:
                        sub_.update(field_id)
                    print(f'{k1 = } {sub_ = } {field_id = }')
                    sub_field.append(sub_)
                print(f'{row = }')
                print(f'new fields: {k1 = } {sub_field = }')
                field.append(row)
        return field, sub_field

    @staticmethod
    def fetch_id_dict(dd):
        for k, v in dd.items():
            if '_id' in k:
                return {k: dd[k]}

    def frame_from_response(self):
        self.df = pd.json_normalize(self.response, max_level=0)
        self.df.columns = [c.replace('.', '_') for c in self.df.columns]
        self.entity_id = f'{self.entity}_id'
        self.df.rename(columns={'id': self.entity_id}, inplace=True)
        self.df.set_index(self.entity_id, drop=True, inplace=True)
        print(f'\nframe_from_response\n{list(self.df.columns) = } {self.df.shape = }\n{self.df.head()}')

    def classify_columns(self):
        self.classified = defaultdict(list)
        # print(f'classify_columns: {self.df.columns = }')
        for col in self.df.columns:
            self.classified[self.classifier(self.df, col)].append(col)
            print(f'classify_columns: {col = } {self.classifier(self.df, col) = }')
        self.inverse_classified = {v: k1 for (k1, v1) in self.classified.items() for v in v1}
        print(f'classify_columns: {self.inverse_classified = }')

    def classifier(self, df, col):
        # print(f'classifier -> {col = }')
        ser = df[col]
        cell = ser[0]
        # print(f'{ser = } {cell = }')
        if isinstance(cell, list):
            return 'list'
        return 'dict' if isinstance(cell, dict) else 'scalar'

    def make_dict_of_pandas(self):
        self.df_dict = {}
        old_columns = self.df.columns
        # print(f'make_dict_of_frames: {self.df.columns = }\n{self.classified = }')
        for col in old_columns:
            for classifier, values in self.classified.items():
                if col in values and classifier != 'scalar':
                    temp_df = self.df.pop(col)
                    self.df_dict[col] = temp_df.to_frame()
        self.df_dict[self.entity] = self.df
        for k, v in self.df_dict.items():
            print(f'{k = }\n{v}')

    def make_frames_from_pandas(self):
        frame_keys_original = list(self.df_dict.keys())
        for frame_key in frame_keys_original:
            frame_type = self.inverse_classified.get(frame_key, None)
            if frame_type == 'list':
                self.make_frame_from_list(frame_key)
            if frame_type == 'dict':
                self.make_frame_from_dict(frame_key)

    def make_frame_from_list(self, frame_key):
        df = self.df_dict[frame_key]
        print(f'{df.head()}\n{df.iloc[0, 0][0] = } {type(df.iloc[0, 0][0]) = }')
        if isinstance(df.iloc[0, 0][0], dict):
            print('list of dicts')
            df = df.explode(column=df.columns[0])
            new_df = pd.json_normalize(df.iloc[:, 0])
            new_df.index = df.index
            # for c in new_df.columns:
            #     if 'id' in c[-2:]:

            new_df.columns = [c.replace('.', '_') for c in new_df.columns]
            try:
                id_ = [c for c in new_df.columns if 'id' in c[-2:]].pop()
            except Exception:
                id_ = False
            if id_:
                new_df.set_index(id_, drop=True, append=True, inplace=True)
            print(new_df.head())
        else:
            print('list of scalars')
            new_df = df.explode(column=df.columns[0])
            print(new_df.head())
        new_df = self.ensure_flat_list(new_df)
        self.df_dict[frame_key] = new_df

    def ensure_flat_list(self, df):

        for col in df.columns:
            if 'list' in self.classifier(df, col):
                print(df)
                print(f'secondary classify_columns (found list): {col = } {self.classifier(df, col) = }')
                if isinstance(df[col][0], dict):
                    print(f'additional matters are a dict')
                    new_df = pd.json_normalize(df[col], max_level=1)
                    new_df.columns = [c.replace('.', '_') for c in new_df.columns]
                    print(new_df.head())
                else:
                    print(f'additional matters are a list')
                    temp = df[col].explode().dropna()
                    new_df = pd.json_normalize(temp, max_level=1)
                    new_df.index = temp.index
                    new_df.columns = [f'{col}_c' for c in new_df.columns]
                    try:
                        id_ = [c for c in new_df.columns if '_id' in c].pop()
                    except Exception:
                        id_ = False
                    if id_:
                        new_df.set_index(id_, drop=True, append=True, inplace=True)
                    print(new_df)
                print(df.iloc[:4, :])
                print(f'adding to df_dict {col = }\n{new_df.head()}')
                self.df_dict[col] = new_df
                df.pop(col)
        return df

    def make_frame_from_dict(self, frame_key):
        df = self.df_dict[frame_key].dropna()
        print(f'frame_from_dict {frame_key = }\n{df.head()}')
        new_df = pd.json_normalize(df.iloc[:, 0], errors='ignore', max_level=2)
        new_df.columns = [c.replace('.', '_') for c in new_df.columns]
        new_df.index = df.index
        print(new_df.head())
        self.ensure_flat_list(new_df)
        self.df_dict[frame_key] = new_df
