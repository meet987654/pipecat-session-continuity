from abc import ABC, abstractmethod
from typing import Optional

class BaseStorage(ABC):
    """Abstract base class for session continuity storage backends."""
    
    @abstractmethod
    async def save(self, key: str, value: str, ttl_seconds: int) -> None:
        """
        Saves a JSON serialized string with a TTL.
        """
        pass
    
    @abstractmethod
    async def load(self, key: str) -> Optional[str]:
        """
        Loads the JSON serialized string for the given key if it exists and hasn't expired.
        Returns None if not found or expired.
        """
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """
        Deletes the key from storage.
        """
        pass
