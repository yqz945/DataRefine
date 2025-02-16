import os
import yaml
from typing import Dict, List, Optional
from .mysql import MySQLDataSource
from .hive import HiveDataSource
from .base import DataSource

class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.datasources = {}
        self.credentials = self._load_credentials()
        self.load_config()
    
    def _load_credentials(self) -> Dict:
        """加载数据源凭证"""
        try:
            from DataRefine.local_config import DATASOURCE_CREDENTIALS
            return DATASOURCE_CREDENTIALS
        except ImportError:
            print("Warning: local_config.py not found, using empty credentials")
            return {}
    
    def load_config(self):
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        for source in config['datasources']:
            try:
                # 获取凭证
                if 'credentials_key' in source['config']:
                    cred_key = source['config'].pop('credentials_key')
                    if cred_key in self.credentials:
                        source['config'].update(self.credentials[cred_key])
                    else:
                        print(f"Warning: credentials not found for {cred_key}")
                
                # 创建数据源实例
                if source['type'] == 'mysql':
                    self.datasources[source['name']] = MySQLDataSource(source['name'], source['config'])
                elif source['type'] == 'hive':
                    try:
                        self.datasources[source['name']] = HiveDataSource(source['name'], source['config'])
                    except Exception as e:
                        print(f"Warning: Failed to initialize Hive datasource {source['name']}: {str(e)}")
            except Exception as e:
                print(f"Warning: Failed to initialize datasource {source['name']}: {str(e)}")
    
    def get_datasource(self, name: str) -> Optional[DataSource]:
        """获取指定名称的数据源"""
        return self.datasources.get(name)
    
    def get_all_datasources(self) -> List[Dict]:
        """获取所有数据源的基本信息"""
        return [
            {
                'name': name,
                'type': ds.__class__.__name__.replace('DataSource', '').lower(),
                'description': ds.config.get('description', '')
            }
            for name, ds in self.datasources.items()
        ]
    
    def close_all(self):
        """关闭所有数据源连接"""
        for ds in self.datasources.values():
            ds.disconnect() 