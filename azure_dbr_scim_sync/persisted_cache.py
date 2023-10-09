import json
from threading import RLock
from typing import Callable

import fsspec


class Cache:

    def __init__(self,
                 storage_account: str,
                 container: str,
                 path: str,
                 tenat_id: str = None,
                 client_id: str = None,
                 client_secret: str = None):
        self._storage_account = storage_account
        self._container = container
        self._path = path
        self._storage_options = {'account_name': storage_account}

        if tenat_id:
            self._storage_options['tenant_id'] = tenat_id

        if client_id:
            self._storage_options['client_id'] = client_id

        if client_secret:
            self._storage_options['client_secret'] = client_secret

        self._data = {}
        self._lock = RLock()
        self._change_counter = 0
        self._load()

    def _get_handle(self, mode):
        if self._container is None and self._storage_account is None:
            return fsspec.open(self._path, mode=mode, encoding="utf-8")
        else:
            return fsspec.open(self._path, **self._storage_options)

    def get_and_validate(self, key, validator: Callable):
        with self._lock:
            value = self._data.get(key)
            if not value:
                return None

        if validator is not None:
            # FIXME: outside of lock to make sure we dont block other processes
            # ... our calling implementation never quries the same keys concurrently
            # ... to make it feature full it we should make lock per key
            if validator(key, value):
                return value
        else:
            return value

        with self._lock:
            self._data.pop(key, None)
            self._change_counter = self._change_counter + 1
            self._auto_flush_if_needed()

            return None

    def __setitem__(self, key, value):
        with self._lock:
            old_value = self._data.get(key)
            if old_value != value:
                self._data[key] = value
                self._change_counter = self._change_counter + 1
                self._auto_flush_if_needed()

    def _auto_flush_if_needed(self):
        with self._lock:
            if self._change_counter >= 10:
                self.flush()

    def _load(self):
        with self._lock:
            try:
                with self._get_handle("r") as f:
                    json_str = f.read()
                    self._data = json.loads(json_str)
            except FileNotFoundError:
                self._data = {}

    def flush(self):
        with self._lock:
            json_str = json.dumps(self._data, indent=4)
            with self._get_handle("w") as f:
                f.write(json_str)

            self._change_counter = 0

    def clear(self):
        with self._lock:
            if self._data:
                self._data = {}
                self.flush()
