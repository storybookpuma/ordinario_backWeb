import time
import logging

logger = logging.getLogger(__name__)


class TimedCache:
    """Simple in-memory cache with TTL."""

    def __init__(self, default_ttl=300):
        self._store = {}
        self.default_ttl = default_ttl

    def get(self, key):
        entry = self._store.get(key)
        if entry is None:
            return None
        if time.time() > entry["expires_at"]:
            self._store.pop(key, None)
            return None
        return entry["value"]

    def set(self, key, value, ttl=None):
        self._store[key] = {
            "value": value,
            "expires_at": time.time() + (ttl or self.default_ttl),
        }

    def delete(self, key):
        self._store.pop(key, None)

    def delete_prefix(self, prefix):
        keys_to_remove = [k for k in self._store if k.startswith(prefix)]
        for k in keys_to_remove:
            self._store.pop(k, None)

    def clear(self):
        self._store.clear()
