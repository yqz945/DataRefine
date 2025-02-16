import os

# 存储服务配置
STORAGE_CONFIG = {
    # 开发环境使用本地存储
    'development': {
        'type': 'local',
        'upload_dir': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'upload')
    },
    
    # 生产环境使用腾讯云COS
    'production': {
        'type': 'cos',
        'secret_id': os.getenv('COS_SECRET_ID'),      # 从环境变量获取
        'secret_key': os.getenv('COS_SECRET_KEY'),    # 从环境变量获取
        'region': os.getenv('COS_REGION', 'ap-guangzhou'),
        'bucket': os.getenv('COS_BUCKET')
    }
}

# 数据源分页配置
DATASOURCE_CONFIG = {
    'page_size': 100,  # 默认每页记录数
    'max_page_size': 10000,  # 最大每页记录数限制
    'page_size_options': [100, 500, 1000, 5000]  # 页面上可选的每页记录数
}

# 当前环境，可以通过环境变量设置
ENV = os.getenv('DATAREFINE_ENV', 'development')

# 获取当前环境的存储配置
CURRENT_STORAGE_CONFIG = STORAGE_CONFIG[ENV]

try:
    from .local_config import COS_CONFIG
    # 如果在生产环境且存在本地配置，使用本地配置覆盖环境变量配置
    if ENV == 'production':
        CURRENT_STORAGE_CONFIG.update(COS_CONFIG)
except ImportError:
    pass 