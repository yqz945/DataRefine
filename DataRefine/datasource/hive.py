import jaydebeapi
import pandas as pd
from typing import List, Dict
import logging
from .base import DataSource

logger = logging.getLogger(__name__)

class HiveDataSource(DataSource):
    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)
        self._connection = None
        
    def connect(self):
        """创建 JDBC 连接"""
        if not self._connection:
            try:
                # Hive JDBC URL
                jdbc_url = f"jdbc:hive2://{self.config['host']}:{self.config['port']}/{self.config['database']}"
                
                # 连接 Hive
                self._connection = jaydebeapi.connect(
                    "org.apache.hive.jdbc.HiveDriver",
                    jdbc_url,
                    [self.config['username'], self.config['password']],
                    "D:/hive_jdbc/hive-jdbc-3.1.2-standalone.jar"  # Hive JDBC 驱动路径
                )
                logger.info(f"Connected to Hive: {jdbc_url}")
            except Exception as e:
                logger.error(f"Error connecting to Hive: {str(e)}")
                raise
    
    def get_table_data(self, table_name: str, limit: int = 10000, offset: int = 0) -> pd.DataFrame:
        """获取表数据"""
        try:
            self.connect()
            query = f"""
                SELECT * FROM {table_name}
                LIMIT {limit}
                OFFSET {offset}
            """
            logger.info(f"Executing query: {query}")
            
            with self._connection.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
                
        except Exception as e:
            logger.error(f"Error getting table data: {str(e)}")
            raise
            
    def get_table_info(self, table_name: str) -> Dict:
        """获取表的基本信息"""
        try:
            self.connect()
            with self._connection.cursor() as cursor:
                # 获取总行数
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total = cursor.fetchone()[0]
                
                return {
                    'name': table_name,
                    'schema': self.config['database'],
                    'total_rows': total
                }
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            raise
            
    def disconnect(self):
        """关闭连接"""
        if self._connection:
            self._connection.close()
            self._connection = None

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
    
    def execute_query(self, query: str) -> pd.DataFrame:
        self.connect()
        return pd.read_sql(query, self._connection) 