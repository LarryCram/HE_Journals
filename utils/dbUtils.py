

class dbUtil:
    import pandas as pd
    import sqlite3 as db

    def __init__(self, db_name=None):
        self.db_name = db_name
        self.db_file = rf'{self.db_name}.db'
        print(f'opening or creating db file {self.db_file}')
        self.conn = self.db.connect(self.db_file)

    def close(self):
        return self.conn.close()

    def read_db(self, table_name=None, columns=None):
        """
        read table/columns from sqlite db
        :param table_name:
        :param table:
        :param columns:
        :return:
        """
        if columns:
            df = self.pd.read_sql_query(f' SELECT {columns} FROM {table_name}', self.conn)
        else:
            df = self.pd.read_sql_query(f' SELECT * FROM {table_name}', self.conn)
        df = df.convert_dtypes(infer_objects=True, convert_string=True,
                               convert_integer=True, convert_boolean=True, convert_floating=True)
        return df

    def to_db(self, df=None, table_name=None, if_exists='replace'):
        df.convert_dtypes(infer_objects=True, convert_string=True,
                          convert_integer=True, convert_boolean=True, convert_floating=True). \
            to_sql(table_name, self.conn, index=True, if_exists=if_exists)

    def sql_db(self, query=None):
        return self.pd.read_sql(query, self.conn)

