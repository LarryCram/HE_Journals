from openalexextract.Extractor import Extractor
ex = Extractor()


class QueryMaker(object):
    """
    Prepare dictionary of pandas DataFrames from OpenAlex by entity and selected fields
    """

    def __init__(self):
        self.entity_item = None
        self.entity_id = None
        self.select = None
        self.filtre = None
        self.entity = None

    def build_query(self):
        query = ex.config['oax_url']
        if self.entity:
            if self.entity_item:
                query += f'{self.entity}/{self.entity_item}?'
            else:
                query += f'{self.entity}?'
        if self.filtre:
            query += f'filter={self.filtre}'
        if self.select:
            query += f'&select={self.select}'
        return query

    def query_maker(self, entity=None, filtre=None, select=None):
        if select and 'id' not in select:
            select.insert(0, 'id')
        # print(f'query_maker: {entity = } {filtre = } {select = }')
        self.entity, self.entity_item = entity.copy().popitem()
        self.entity_id = f'{self.entity}_id'
        self.validity_check(entity=self.entity, filtre=filtre, select=select)
        if filtre:
            self.filtre = ','.join(f'{k}:{v}' for k, v in filtre.items())
        if select:
            self.select = ",".join(select)
        query = self.build_query()
        # print(f'query_maker -> {query = }')
        return query

    def validity_check(self, entity=None, filtre=None, select=None):

        # print(f'validity_check: {entity = } {filtre = } {select = }')
        # print(f'{set(select) = }')
        # print(f'{set(ex.fields[entity]) = }')
        if entity not in ex.config['entities']:
            raise ValueError(f'entity {entity = } not in list of OpenAlex entities!')
        if filtre and not isinstance(filtre, (dict,)):
            raise ValueError(f'filtre {filtre = } is not a dictionary!')
        if select and not set(select).issubset(set(ex.fields[entity])):
            raise ValueError(f'select list {select = } is not in list of OpenAlex select fields!')
        if select and 'id' not in select:
            raise ValueError(f'select list {select = } must include "id" element')
        return True
