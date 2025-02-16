import pandas as pd
from typing import List, Dict, Optional
from sqlalchemy import create_engine, inspect, text
import numpy as np
from .base import DataSource
import logging

logger = logging.getLogger(__name__)

class MySQLDataSource(DataSource):
    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)
        self.engine = self.create_engine()

    def create_engine(self):
        """创建数据库引擎"""
        url = f"mysql+mysqlconnector://{self.config['username']}:{self.config['password']}@" \
              f"{self.config['host']}:{self.config['port']}/{self.config['database']}?charset=utf8mb4"
        return create_engine(url)

    def get_tables(self) -> List[Dict]:
        """获取所有表信息"""
        logger.info(f"Getting tables for database: {self.config['database']}")
        try:
            inspector = inspect(self.engine)
            # 只返回基本信息，不查询记录数
            tables = [
                {
                    'name': table_name,
                    'schema': self.config['database'],
                    'description': self.get_table_comment(table_name) or ''  # 尝试获取表注释
                }
                for table_name in inspector.get_table_names()
            ]
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}", exc_info=True)
            raise

    def get_table_comment(self, table_name: str) -> str:
        """获取表注释"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT table_comment FROM information_schema.tables "
                    f"WHERE table_schema = '{self.config['database']}' AND table_name = '{table_name}'"
                ))
                row = result.fetchone()
                return row[0] if row else ''
        except Exception:
            return ''

    def get_table_info(self, table_name: str) -> Dict:
        """获取表的基本信息"""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as total FROM {table_name}"))
            total = result.fetchone()[0]
            
        return {
            'name': table_name,
            'schema': self.config['database'],
            'total_rows': total
        }

    def get_table_data(self, table_name: str, schema: str = None, limit: int = 10000, offset: int = 0) -> pd.DataFrame:
        """
        分页获取表数据
        Args:
            table_name: 表名
            schema: 模式名
            limit: 每页记录数
            offset: 偏移量
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        query = f"SELECT * FROM {full_table_name}"
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"

        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            data = result.fetchall()
            return pd.DataFrame(data, columns=columns)

    def preview_table(self, table_name: str, rows: int = 1000) -> pd.DataFrame:
        """预览表数据"""
        return self.get_table_data(table_name, limit=rows)

    def query_table(self, table_name: str, conditions: Dict = None, 
                   columns: List[str] = None, limit: int = 10000) -> pd.DataFrame:
        """
        条件查询表数据
        Args:
            table_name: 表名
            conditions: 查询条件，如 {'age': 18, 'city': 'Shanghai'}
            columns: 需要的列，如 ['id', 'name', 'age']
            limit: 返回记录数限制
        """
        cols = "*" if not columns else ", ".join(columns)
        query = f"SELECT {cols} FROM {table_name}"
        
        if conditions:
            where_clauses = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += f" LIMIT {limit}"
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            data = result.fetchall()
            return pd.DataFrame(data, columns=columns) 