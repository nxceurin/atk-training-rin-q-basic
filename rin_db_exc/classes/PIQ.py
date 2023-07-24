import logging
from abc import ABC, abstractmethod

logger: logging.Logger = logging.getLogger(__name__)


class PersistentQInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass
