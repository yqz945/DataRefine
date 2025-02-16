from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Optional
from sqlalchemy.engine import Engine

class DataSource(ABC):
    """数据源基类"""
    
    def __init__(self, name: str, config: Dict):
        self.name = name
        self.config = config
        self._engine = None  # 添加 engine 属性
    
    @abstractmethod
    def create_engine(self) -> Engine:
        """创建数据库引擎"""
        pass
    
    @property
    def engine(self) -> Engine:
        """获取数据库引擎（懒加载）"""
        if self._engine is None:
            self._engine = self.create_engine()
        return self._engine
    
    @engine.setter
    def engine(self, value):
        self._engine = value
    
    def disconnect(self) -> None:
        """断开数据源连接"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
    
    @abstractmethod
    def get_tables(self) -> List[Dict]:
        """获取数据源中的表列表"""
        pass
    
    def get_table_data(self, table_name: str, schema: str = None) -> pd.DataFrame:
        """获取指定表的数据"""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        return pd.read_sql_table(full_table_name, self.engine)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """执行SQL查询"""
        return pd.read_sql_query(query, self.engine)

    def get_table_info(self, table_name: str) -> Dict:
        """获取表的基本信息"""
        raise NotImplementedError

    def get_table_data(self, table_name: str, limit: int = 10000, offset: int = 0) -> pd.DataFrame:
        """获取表数据"""
        raise NotImplementedError

    def preview_table(self, table_name: str, rows: int = 1000) -> pd.DataFrame:
        """预览表数据"""
        raise NotImplementedError 