from .base import StorageBase
from .local import LocalStorage
from .cos import COSStorage
from .factory import StorageFactory

__all__ = ['StorageBase', 'LocalStorage', 'COSStorage', 'StorageFactory'] 