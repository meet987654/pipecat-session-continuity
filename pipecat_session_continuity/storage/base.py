from abc import ABC, abstractmethod
from typing import Optional


class StorageUnavailableError(RuntimeError):
    """The session store could not be read (outage, I/O error, corrupt record).

    Raised instead of returning "no session": treating an outage as a fresh call would
    drop the conversation and the idempotency records that block duplicate tool calls.
    """


class BaseStorage(ABC):
    """Abstract base class for session continuity storage backends.

    Fail closed: ``load`` returns None only when the key does not exist or has expired.
    On any backend error it must raise (``StorageUnavailableError`` or any other
    exception, which the manager wraps); returning None on an error makes an outage look
    like a fresh session.
    """
    
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
        Returns None if not found or expired. Raises if the backend cannot be read.
        """
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """
        Deletes the key from storage.
        """
        pass
