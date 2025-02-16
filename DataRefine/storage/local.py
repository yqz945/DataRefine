import os
from typing import BinaryIO, Optional
from .base import StorageBase

class LocalStorage(StorageBase):
    def __init__(self, upload_dir: str = "upload"):
        """
        初始化本地存储
        Args:
            upload_dir: 上传文件保存的目录，默认为'upload'
        """
        self.upload_dir = os.path.abspath(upload_dir)
        # 确保上传目录存在
        os.makedirs(self.upload_dir, exist_ok=True)
    
    def upload(self, file_obj: BinaryIO, filename: str) -> str:
        """上传文件到本地目录"""
        file_path = os.path.join(self.upload_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(file_obj.read())
        return file_path
    
    def download(self, filename: str) -> Optional[BinaryIO]:
        """从本地目录下载文件"""
        file_path = os.path.join(self.upload_dir, filename)
        if os.path.exists(file_path):
            return open(file_path, 'rb')
        return None
    
    def delete(self, filename: str) -> bool:
        """删除本地文件"""
        file_path = os.path.join(self.upload_dir, filename)
        try:
            os.remove(file_path)
            return True
        except OSError:
            return False 