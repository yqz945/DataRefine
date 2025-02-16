import pandas as pd
from typing import List, Dict
from sqlalchemy import create_engine, inspect
from .base import DataSource

class HiveDataSource(DataSource):
    def create_engine(self):
        url = f"hive://{self.config['username']}:{self.config['password']}@" \
              f"{self.config['host']}:{self.config['port']}/{self.config['database']}"
        return create_engine(url)
    
    def get_tables(self) -> List[Dict]:
        inspector = inspect(self.engine)
        return [
            {
                'name': table_name,
                'schema': schema
            }
            for schema in inspector.get_schema_names()
            for table_name in inspector.get_table_names(schema=schema)
        ]
    
    def get_table_data(self, table_name: str, schema: str = None) -> pd.DataFrame:
        self.connect()
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT * FROM {full_table_name}"
        return pd.read_sql(query, self._connection)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        self.connect()
        return pd.read_sql(query, self._connection) 