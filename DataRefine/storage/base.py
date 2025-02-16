from abc import ABC, abstractmethod
from typing import BinaryIO, Optional

class StorageBase(ABC):
    """存储服务的抽象基类"""
    
    @abstractmethod
    def upload(self, file_obj: BinaryIO, filename: str) -> str:
        """
        上传文件
        Args:
            file_obj: 文件对象
            filename: 文件名
        Returns:
            str: 文件访问路径或URL
        """
        pass
    
    @abstractmethod
    def download(self, filename: str) -> Optional[BinaryIO]:
        """
        下载文件
        Args:
            filename: 文件名
        Returns:
            Optional[BinaryIO]: 文件对象，如果文件不存在返回None
        """
        pass
    
    @abstractmethod
    def delete(self, filename: str) -> bool:
        """
        删除文件
        Args:
            filename: 文件名
        Returns:
            bool: 是否删除成功
        """
        pass 