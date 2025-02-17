import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

import pandas as pd
from flask import redirect, render_template, abort, request, jsonify
from dtale.app import build_app, initialize_process_props
from dtale.views import startup
from dtale.utils import build_url
from tests import dtale
from DataRefine.config import CURRENT_STORAGE_CONFIG, DATASOURCE_CONFIG
from DataRefine.storage import StorageFactory
import uuid
from DataRefine.datasource import DataSourceManager
from DataRefine.logger import setup_logger

# 获取当前文件所在目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DATA_DIR = os.path.join(BASE_DIR, 'sample_data')

# 示例数据配置
SAMPLE_DATASETS = {
    'movies': {
        'file': 'movies.csv',
        'title': '电影数据分析'
    },
    'population': {
        'file': 'population.csv',
        'title': '人口统计分析'
    },
    'sales': {
        'file': 'sales.csv',
        'title': '销售数据分析'
    },
    'weather': {
        'file': 'weather.csv',
        'title': '气象数据分析'
    }
}

# dtale默认设置
DTALE_SETTINGS = {
    'precision': 4,  # 数值精度
    'enable_custom_filters': True
}

# 创建存储服务实例
storage = StorageFactory.create_storage(**CURRENT_STORAGE_CONFIG)

# 添加允许的文件类型和大小限制
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB in bytes


def allowed_file(filename):
    """检查文件类型是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


if __name__ == '__main__':
    try:
        HOST = "0.0.0.0"
        PORT = 8080

        initialize_process_props(HOST, PORT)
        app_url = build_url(HOST, str(PORT))
        app = build_app(app_url,
                        additional_templates=os.path.join(BASE_DIR, 'templates'),
                        reaper_on=True)

        # 设置日志
        print("Setting up logger...")  # 添加调试信息
        logger = setup_logger(app)
        print("Logger setup complete")  # 添加调试信息


        @app.route("/sample/<dataset_id>")
        def load_sample(dataset_id):
            """加载示例数据到dtale"""
            # 检查数据集是否存在
            if dataset_id not in SAMPLE_DATASETS:
                abort(404, f"未找到数据集: {dataset_id}")

            try:
                dataset = SAMPLE_DATASETS[dataset_id]
                file_path = os.path.join(SAMPLE_DATA_DIR, dataset['file'])

                # 检查文件是否存在
                if not os.path.exists(file_path):
                    abort(404, f"数据文件不存在: {dataset['file']}")

                # 读取数据
                df = pd.read_csv(file_path)

                # 启动dtale实例，应用默认设置
                instance = startup(
                    "",
                    data=df,
                    ignore_duplicate=True,
                    **DTALE_SETTINGS
                )

                # 渲染带有dtale iframe的页面
                return render_template(
                    'dtale_view.html',
                    dtale_url=f'/dtale/main/{instance._data_id}',
                    title=dataset['title']
                )

            except Exception as e:
                return f"加载数据失败: {str(e)}", 500


        @app.route("/")
        def hello_world():
            return render_template('index.html')


        @app.route("/upload", methods=['POST'])
        def upload_file():
            """处理文件上传"""
            if 'file' not in request.files:
                return jsonify({
                    'success': False,
                    'message': '没有文件被上传'
                }), 400

            file = request.files['file']
            if file.filename == '':
                return jsonify({
                    'success': False,
                    'message': '未选择文件'
                }), 400

            if not allowed_file(file.filename):
                return jsonify({
                    'success': False,
                    'message': '只支持上传 CSV 和 Excel (xlsx) 文件'
                }), 400

            # 检查文件大小
            file.seek(0, 2)  # 移动到文件末尾
            size = file.tell()  # 获取文件大小
            file.seek(0)  # 重置文件指针到开始

            if size > MAX_FILE_SIZE:
                return jsonify({
                    'success': False,
                    'message': '文件大小不能超过20MB'
                }), 400

            try:
                # 生成唯一的文件名
                original_filename = file.filename
                filename = f"{uuid.uuid4().hex}_{original_filename}"

                # 使用storage服务保存文件
                file_path = storage.upload(file, filename)

                # 根据文件类型读取数据
                if filename.endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:  # xlsx
                    df = pd.read_excel(file_path)

                # 启动dtale实例
                instance = startup(
                    "",
                    data=df,
                    ignore_duplicate=True,
                    **DTALE_SETTINGS
                )

                # 返回dtale_view.html的URL，而不是直接返回dtale的URL
                return jsonify({
                    'success': True,
                    'message': '文件上传成功',
                    'data': {
                        'filename': original_filename,
                        'stored_filename': filename,
                        'view_url': f'/view/upload/{instance._data_id}'  # 新的视图URL
                    }
                })

            except Exception as e:
                return jsonify({
                    'success': False,
                    'message': f'文件上传失败: {str(e)}'
                }), 500


        # 添加新的视图路由
        @app.route("/view/upload/<data_id>")
        def view_upload(data_id):
            """查看上传的数据"""
            return render_template(
                'dtale_view.html',
                title='数据分析',
                dtale_url=f'/dtale/main/{data_id}'
            )


        # 初始化数据源管理器
        config_path = os.path.join(BASE_DIR, 'datasources.yaml')
        datasource_manager = DataSourceManager(config_path)


        @app.route("/datasource/<datasource_name>/<table_name>")
        def view_datasource(datasource_name: str, table_name: str):
            """查看数据源表数据"""
            logger.info(f"Accessing datasource: {datasource_name}, table: {table_name}")
            
            page = request.args.get('page', 1, type=int)
            page_size = request.args.get('page_size', DATASOURCE_CONFIG['page_size'], type=int)
            
            # 限制page_size不超过最大值
            page_size = min(page_size, DATASOURCE_CONFIG['max_page_size'])
            
            try:
                datasource = datasource_manager.get_datasource(datasource_name)
                if not datasource:
                    logger.error(f"Datasource not found: {datasource_name}")
                    return jsonify({'error': 'Datasource not found'}), 404

                try:
                    # 获取表信息
                    logger.info(f"Getting table info for {table_name}")
                    table_info = datasource.get_table_info(table_name)
                    logger.info(f"Table info retrieved: {table_info}")

                    # 计算偏移量并获取数据
                    offset = (page - 1) * page_size
                    logger.info(f"Fetching data with limit={page_size}, offset={offset}")
                    data = datasource.get_table_data(table_name, limit=page_size, offset=offset)
                    logger.info(f"Data fetched successfully, shape: {data.shape if data is not None else 'None'}")

                    # 启动dtale实例
                    instance = startup(
                        "",
                        data=data,
                        ignore_duplicate=True,
                        **DTALE_SETTINGS
                    )
                    logger.info(f"Dtale instance created with id: {instance._data_id}")

                    return render_template(
                        'datasource_view.html',
                        title=f'{table_name}',
                        dtale_url=f'/dtale/main/{instance._data_id}',
                        total_rows=table_info['total_rows'],
                        datasource_name=datasource_name,
                        table_name=table_name,
                        current_page=page,
                        page_size=page_size,
                        page_size_options=DATASOURCE_CONFIG['page_size_options']
                    )

                except Exception as e:
                    logger.error(f"Error accessing table {table_name}: {str(e)}", exc_info=True)
                    return jsonify({'error': str(e)}), 500

            except Exception as e:
                logger.error(f"Error in view_datasource: {str(e)}", exc_info=True)
                return jsonify({'error': str(e)}), 500


        @app.route("/datasource/<datasource_name>/<table_name>/data")
        def get_datasource_data(datasource_name: str, table_name: str):
            """获取数据源表的分页数据"""
            page = request.args.get('page', 1, type=int)
            page_size = request.args.get('page_size', DATASOURCE_CONFIG['page_size'], type=int)
            page_size = min(page_size, DATASOURCE_CONFIG['max_page_size'])

            datasource = datasource_manager.get_datasource(datasource_name)
            if not datasource:
                return jsonify({'error': 'Datasource not found'}), 404

            try:
                offset = (page - 1) * page_size
                data = datasource.get_table_data(table_name, limit=page_size, offset=offset)

                # 启动新的dtale实例
                instance = startup(
                    "",
                    data=data,
                    ignore_duplicate=True,
                    **DTALE_SETTINGS
                )

                return jsonify({
                    'success': True,
                    'dtale_url': f'/dtale/main/{instance._data_id}'
                })
            except Exception as e:
                return jsonify({'error': str(e)}), 500


        @app.route("/api/datasources")
        def get_datasources():
            """获取所有数据源的基本信息"""
            try:
                result = datasource_manager.get_all_datasources()
                return jsonify({
                    'success': True,
                    'datasources': result
                })
            except Exception as e:
                logger.error(f"Error in get_datasources: {str(e)}", exc_info=True)
                return jsonify({'error': str(e)}), 500


        @app.route("/api/datasources/<datasource_name>/tables")
        def get_datasource_tables(datasource_name: str):
            """获取指定数据源的表信息"""
            try:
                logger.info(f"Received request for tables of datasource: {datasource_name}")
                
                # 检查数据源是否存在
                datasource = datasource_manager.get_datasource(datasource_name)
                if not datasource:
                    logger.warning(f"Datasource not found: {datasource_name}")
                    return jsonify({
                        'success': False,
                        'error': '数据源不存在'
                    }), 404
                    
                try:
                    # 从配置文件获取表信息，而不是查询数据库
                    tables = datasource_manager.get_datasource_tables(datasource_name)
                    logger.info(f"Found {len(tables)} tables in configuration")
                    
                    return jsonify({
                        'success': True,
                        'tables': tables
                    })
                    
                except Exception as e:
                    logger.error(f"Error getting tables for datasource {datasource_name}: {str(e)}")
                    return jsonify({
                        'success': False,
                        'error': str(e)
                    }), 503
                    
            except Exception as e:
                logger.error(f"Error in get_datasource_tables: {str(e)}", exc_info=True)
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 400


        # 添加新的接口，用于获取单个表的详细信息
        @app.route("/api/datasources/<datasource_name>/tables/<table_name>/info")
        def get_table_info(datasource_name: str, table_name: str):
            """获取单个表的详细信息"""
            try:
                datasource = datasource_manager.get_datasource(datasource_name)
                if not datasource:
                    return jsonify({
                        'success': False,
                        'error': '数据源不存在'
                    }), 404

                table_info = datasource.get_table_info(table_name)
                return jsonify({
                    'success': True,
                    'info': table_info
                })

            except Exception as e:
                logger.error(f"Error getting table info: {str(e)}", exc_info=True)
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500


        app.run(host=HOST, port=PORT)
    except Exception as e:
        print(f"Error starting app: {str(e)}")  # 添加错误捕获
        raise
