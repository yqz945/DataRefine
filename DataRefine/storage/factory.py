from typing import Optional
from .base import StorageBase
from .local import LocalStorage
from .cos import COSStorage

class StorageFactory:
    @staticmethod
    def create_storage(**kwargs) -> Optional[StorageBase]:
        """
        创建存储服务实例
        Args:
            **kwargs: 存储服务所需的配置参数，必须包含'type'参数
        Returns:
            Optional[StorageBase]: 存储服务实例
        """
        storage_type = kwargs.pop('type', None)
        if not storage_type:
            raise ValueError("Storage configuration must include 'type' parameter")
            
        if storage_type == 'local':
            upload_dir = kwargs.get('upload_dir', 'upload')
            return LocalStorage(upload_dir)
        
        elif storage_type == 'cos':
            required_params = ['secret_id', 'secret_key', 'region', 'bucket']
            if not all(param in kwargs for param in required_params):
                raise ValueError(f"COS storage requires parameters: {required_params}")
            
            return COSStorage(
                secret_id=kwargs['secret_id'],
                secret_key=kwargs['secret_key'],
                region=kwargs['region'],
                bucket=kwargs['bucket']
            )
        
        raise ValueError(f"Unsupported storage type: {storage_type}") 