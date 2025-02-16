import os
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(app):
    """配置应用日志"""
    # 先清除已有的处理器
    app.logger.handlers.clear()
    logging.getLogger('werkzeug').handlers.clear()
    
    # 创建logs目录
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 设置日志文件路径
    log_file = os.path.join(log_dir, 'app.log')
    
    # 创建日志处理器
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1024 * 1024,  # 1MB
        backupCount=10,
        encoding='utf-8'
    )
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    
    # 设置日志格式
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 设置日志级别 - 文件和控制台都设置为INFO级别
    file_handler.setLevel(logging.INFO)
    console_handler.setLevel(logging.INFO)  # 改为INFO级别
    
    # 添加处理器到应用日志记录器
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)  # 改为INFO级别
    
    # 设置Werkzeug日志
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.handlers.clear()
    werkzeug_logger.addHandler(file_handler)
    werkzeug_logger.addHandler(console_handler)  # 添加控制台输出
    werkzeug_logger.setLevel(logging.INFO)
    
    return app.logger 