import re
import pandas as pd
from collections import defaultdict
from benedict import benedict

from openalexextract.QueryMaker import QueryMaker
from openalexextract.Extractor import Extractor


class FrameMaker(object):
    """
    Prepare dictionary of pandas DataFrames from OpenAlex by entity and selected fields
    """

    def __init__(self):

        self.parent_template = None
        self.set_parents()

        self.entity = None
        self.entity_item = None
        self.entity_id = None
        self.refresh = None
        self.inverse_classified = None
        self.df = None
        self.frame_dict = {}

        self.response = None
        self.query = None
        self.select = None
        self.filtre = None

        self.qm = QueryMaker()
        self.ex = Extractor()

    def set_parents(self):
        self.parent_template = {'works': {'authorships.institutions': self.frame_from_list_of_dict,
                                          # 'locations': self.frame_from_list_of_dict,
                                          'locations.source.issn': self.frame_from_list_of_dict,
                                          'concepts': self.frame_from_list_of_dict,
                                          'referenced_works': self.frame_from_list_of_scalar,
                                          'related_works': self.frame_from_list_of_scalar,
                                          },

                                'authors': {'x_concepts': self.frame_from_list_of_dict
                                            },

                                'concepts': {'ancestors': self.frame_from_list_of_dict,
                                             'related_concepts': self.frame_from_list_of_dict,
                                             'counts_by_year': self.frame_from_list_of_dict,
                                             'international.display_name': self.frame_from_dict,
                                             'international.description': self.frame_from_dict,
                                             'ids.umls_cui': self.string_from_embedded_list,
                                             'ids.umls_aui': self.string_from_embedded_list
                                             },

                                'institutions': {'associated_institutions': self.frame_from_list_of_dict,
                                                 'counts_by_year': self.frame_from_list_of_dict,
                                                 'international.display_name': self.frame_from_dict,
                                                 'x_concepts': self.frame_from_list_of_dict,
                                                 'display_name_acronyms': self.string_from_embedded_list,
                                                 'display_name_alternatives': self.string_from_embedded_list
                                                 },

                                'sources': {'alternate_titles': self.string_from_embedded_list,
                                            'counts_by_year': self.frame_from_list_of_dict,
                                            'ids.issn': self.string_from_embedded_list,
                                            'issn': self.string_from_embedded_list,
                                            'societies': self.frame_from_list_of_dict,
                                            'x_concepts': self.frame_from_list_of_dict
                                            }

                                }

    def frame_maker(self, entity=None, filtre=None, select=None, refresh=None):

        self.query = self.qm.query_maker(entity=entity, filtre=filtre, select=select)
        self.entity, self.entity_item = entity.copy().popitem()
        self.filtre = filtre
        self.select = select
        self.refresh = refresh
        self.entity_id = f'{self.entity}_id'
        # print(f'FRAME_MAKER: {self.entity = } {self.filtre = } {self.select = } {self.query = }')

        self.response = self.ex.extractor(query=self.query, refresh=refresh)
        if not self.response:
            return
        if isinstance(self.response, list):
            self.replace_inverted_index()
        self.get_parents()

    def replace_inverted_index(self):
        abstract_string = 'abstract_inverted_index'
        if len(self.response) < 1 or abstract_string not in self.response[0]:
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
        if isinstance(dd, (type(pd.NA), type(None), float,)):
            return
        try:
            abstract_dict = {pos: word for word, posList in dd.items() for pos in
                             posList}
            abstract = ' '.join([abstract_dict[pos] for pos in sorted(abstract_dict)])
            abstract = re.sub('^abstract *?', '', abstract, count=1, flags=re.I)
            return abstract.strip()
        except:
            raise ValueError(f'exception in construction of abstract from inverted index ({dd = })')

    def get_parents(self):
        """
        transform JSON response to construct the dictionary of parents
        :return:
        """
        parents = self.parent_template.get(self.entity)
        if not parents:
            print(f'{self.entity = } {self.parent_template = } {self.entity_id = } {parents = }')
            raise ValueError(f'no parents for entity {self.entity = }')

        self.df = pd.json_normalize(self.response, max_level=1)
        self.df = self.df.rename(columns={'id': self.entity_id})
        self.df = self.df.set_index(self.entity_id)
        # self.df.info()
        # print(self.df.head())

        for col, select in parents.items():
            # print(f'{col = } {select = }')
            select(col=col)
        self.df.columns = [c.replace('.', '_') for c in self.df.columns]
        self.frame_dict[self.entity] = self.df
        # for k, v in self.frame_dict.items():
        #     print(f'{k = } {v.shape = } {v.columns = }\n{v.head()}')

    def frame_from_list_of_scalar(self, col=None):
        temp = self.df.pop(col).to_frame()
        self.frame_dict[col] = temp.explode(col)
        # print(f'from list of scalar {temp.shape = } {temp.columns = }\n{temp.head()}')

    def frame_from_list_of_dict(self, col=None):
        col_ext = None
        if '.' in col:
            col, col_ext = col.split('.', maxsplit=1)

        temp = self.df.pop(col).to_frame()
        temp = temp.explode(col).reset_index()
        temp = temp.join(pd.json_normalize(temp[col])).drop(columns=col)
        if 'id' in temp.columns:
            temp.columns = [f'{col}_{c}' if self.entity not in c else c for c in temp.columns]

        if col_ext:
            if '.' not in col_ext:
                temp = temp.explode(col_ext)
                temp = temp.join(pd.json_normalize(temp[col_ext])).drop(columns=col_ext)
                temp = temp.rename(columns={'id': f'{col_ext}_id', 'display_name': f'{col_ext}_display_name'})
            else:
                if col_ext not in list(temp.columns):
                    temp[col_ext] = pd.NA
                else:
                    temp[col_ext] = ['| '.join(v) if isinstance(v, list) else pd.NA for v in temp[col_ext]]

        temp.columns = [c.replace('.', '_') for c in temp.columns]
        self.frame_dict[col] = temp
        # print(f'from list of dicts {temp.shape = } {temp.columns = }\n{temp.head()}')

    def frame_from_dict(self, col=None):
        temp = self.df.pop(col)
        # print(f'{temp.head()}')
        temp.info()
        temp = pd.json_normalize(temp)
        temp.columns = [c.replace('.', '_') for c in temp.columns]
        # print(f'frame_from_dict {temp.head()}')
        self.frame_dict[col] = temp

    def string_from_embedded_list(self, col=None):
        # print(f'string_from_embedded_list {col = } {self.df[col][0] = }')
        if '.' in col:
            col, col_ext = col.split('.', maxsplit=1)
        # print(f'{col = } {col_ext = }')
        self.df[col] = ['| '.join(d) if isinstance(d, list) else pd.NA for d in self.df[col]]

