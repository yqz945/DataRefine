from typing import BinaryIO, Optional
from qcloud_cos import CosConfig, CosS3Client
from .base import StorageBase

class COSStorage(StorageBase):
    def __init__(self, secret_id: str, secret_key: str, region: str, bucket: str):
        """
        初始化腾讯云COS存储
        Args:
            secret_id: 腾讯云 SecretId
            secret_key: 腾讯云 SecretKey
            region: 地域信息
            bucket: 存储桶名称
        """
        config = CosConfig(
            Region=region,
            SecretId=secret_id,
            SecretKey=secret_key
        )
        self.client = CosS3Client(config)
        self.bucket = bucket
        self.region = region
    
    def upload(self, file_obj: BinaryIO, filename: str) -> str:
        """上传文件到COS"""
        self.client.put_object(
            Bucket=self.bucket,
            Body=file_obj,
            Key=filename
        )
        return f'https://{self.bucket}.cos.{self.region}.myqcloud.com/{filename}'
    
    def download(self, filename: str) -> Optional[BinaryIO]:
        """从COS下载文件"""
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=filename
            )
            return response['Body']
        except Exception:
            return None
    
    def delete(self, filename: str) -> bool:
        """删除COS中的文件"""
        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=filename
            )
            return True
        except Exception:
            return False 