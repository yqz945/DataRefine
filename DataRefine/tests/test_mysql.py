import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from datasource import DataSourceManager
import logging

def setup_test_logger():
    """设置测试用的日志器"""
    logger = logging.getLogger('test_mysql')
    logger.setLevel(logging.DEBUG)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    
    # 创建文件处理器
    log_file = os.path.join(project_root, 'logs', 'test.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # 创建格式化器
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # 添加处理器到日志器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def test_mysql_connection():
    """测试MySQL数据源连接"""
    # 设置日志
    logger = setup_test_logger()
    
    try:
        # 初始化数据源管理器
        config_path = os.path.join(project_root, 'datasources.yaml')
        manager = DataSourceManager(config_path)
        
        # 获取数据源
        datasource = manager.get_datasource("统计分析数据集市")
        if not datasource:
            print("❌ 未找到数据源")
            return
            
        # 测试获取表列表
        print("\n1. 测试获取表列表:")
        tables = datasource.get_tables()
        print(f"✓ 成功获取到 {len(tables)} 个表")
        for table in tables[:5]:  # 只显示前5个表
            print(f"  - {table['name']}")
        
        if tables:
            # 选择第一个表进行测试
            test_table = tables[0]['name']
            
            # 测试获取表信息
            print(f"\n2. 测试获取表 {test_table} 的信息:")
            table_info = datasource.get_table_info(test_table)
            print(f"✓ 表 {test_table} 共有 {table_info['total_rows']} 条记录")
            
            # 测试预览表数据
            print(f"\n3. 测试预览表 {test_table} 的数据:")
            preview_data = datasource.preview_table(test_table, rows=5)
            print("✓ 预览前5行数据:")
            print(preview_data)
            
        print("\n✨ 所有测试完成")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        logger.error(f"Test failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    test_mysql_connection() 