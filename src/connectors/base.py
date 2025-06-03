from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseAPIConnector(ABC):
    @abstractmethod
    async def fetch(self, **kwargs) -> Dict[str, Any]:
        ...

    @abstractmethod
    def parse(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        ...
