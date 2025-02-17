import os
import yaml
import logging
from typing import Dict, List, Optional
from .mysql import MySQLDataSource
from .hive import HiveDataSource
from .base import DataSource

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.datasources = {}
        self.datasource_configs = {}  # 存储原始配置信息
        self.credentials = self._load_credentials()
        self.load_config()
    
    def _load_credentials(self) -> Dict:
        """加载数据源凭证"""
        try:
            from DataRefine.local_config import DATASOURCE_CREDENTIALS
            return DATASOURCE_CREDENTIALS
        except ImportError:
            logger.warning("Warning: local_config.py not found, using empty credentials")
            return {}
    
    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config file: {str(e)}")
            return

        for source in config['datasources']:
            try:
                # 保存原始配置
                self.datasource_configs[source['name']] = source

                # 获取凭证
                source_config = source['config'].copy()
                if 'credentials_key' in source_config:
                    cred_key = source_config.pop('credentials_key')
                    if cred_key in self.credentials:
                        source_config.update(self.credentials[cred_key])
                    else:
                        logger.warning(f"Credentials not found for {cred_key}")
                
                # 创建数据源实例
                if source['type'] == 'mysql':
                    self.datasources[source['name']] = MySQLDataSource(source['name'], source_config)
                elif source['type'] == 'hive':
                    try:
                        self.datasources[source['name']] = HiveDataSource(source['name'], source_config)
                    except ImportError as e:
                        logger.warning(f"Skipping Hive datasource '{source['name']}': {str(e)}")
                        continue
                    except Exception as e:
                        logger.error(f"Failed to initialize Hive datasource {source['name']}: {str(e)}")
                        continue
            except Exception as e:
                logger.error(f"Failed to initialize datasource {source['name']}: {str(e)}")
                continue

        logger.info(f"Successfully loaded {len(self.datasources)} datasources")
    
    def get_datasource(self, name: str) -> Optional[DataSource]:
        """获取指定名称的数据源"""
        return self.datasources.get(name)
    
    def get_all_datasources(self) -> List[Dict]:
        """获取所有数据源的基本信息"""
        return [
            {
                'name': name,
                'type': ds.__class__.__name__.replace('DataSource', '').lower(),
                'description': self.datasource_configs[name].get('description', '')
            }
            for name, ds in self.datasources.items()
        ]
    
    def get_datasource_tables(self, datasource_name: str) -> List[Dict]:
        """获取指定数据源的表信息（从配置文件）"""
        if datasource_name not in self.datasource_configs:
            return []
            
        config = self.datasource_configs[datasource_name]
        return config.get('tables', [])
    
    def close_all(self):
        """关闭所有数据源连接"""
        for ds in self.datasources.values():
            ds.disconnect() 