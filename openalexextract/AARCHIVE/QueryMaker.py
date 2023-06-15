from openalexextract.AARCHIVE.Extractor import Extractor


class QueryMaker(object):
    """
    Prepare dictionary of pandas DataFrames from OpenAlex by entity and selected fields
    """

    def __init__(self, for_code=None):
        self.for_code = for_code
        self.entity_item = None
        self.entity_id = None
        self.select = None
        self.filtre = None
        self.entity = None
        self.ex = Extractor(for_code=self.for_code)

    def build_query(self):
        query = self.ex.config['oax_url']
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
        # print(f' query_maker: {entity = } {filtre = } {select = }')
        self.entity, self.entity_item = entity.copy().popitem()
        self.entity_id = f'{self.entity}_id'
        self.validity_check(entity=self.entity, filtre=filtre, select=select)
        if filtre:
            self.filtre = ','.join(f'{k}:{v}' for k, v in filtre.items())
        if select:
            self.select = ",".join(select)
        query = self.build_query()
        # print(f'query_maker -> {query = } => {entity = } {filtre = } {select = }')
        return query

    def validity_check(self, entity=None, filtre=None, select=None):

        # print(f'validity_check: {entity = } {filtre = } {select = }')
        # print(f'{set(select) = }')
        # print(f'{set(self.ex.fields[entity]) = }')
        if entity not in self.ex.config['entities']:
            raise ValueError(f'entity {entity = } not in list of OpenAlex entities!')
        if filtre and not isinstance(filtre, (dict,)):
            raise ValueError(f'filtre {filtre = } is not a dictionary!')
        if select and not set(select).issubset(set(self.ex.fields[entity])):
            print(f'select list item is not in list of OpenAlex select fields for {entity = }\n{select = }')
            print(f'{self.ex.fields[entity] = }')
            raise ValueError('select list has item that is not allowed')
        if select and 'id' not in select:
            raise ValueError(f'select list {select = } must include "id" element')
        return True
