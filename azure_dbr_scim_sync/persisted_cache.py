import json
import logging
import os
from threading import RLock

import fsspec
from adlfs import AzureBlobFileSystem

logger = logging.getLogger('sync.cache')


class Cache:

    def __init__(self,
                 path: str,
                 *,
                 storage_account: str = None,
                 container: str = None,
                 tenat_id: str = None,
                 client_id: str = None,
                 client_secret: str = None):
        self._storage_account = storage_account
        self._container = container or os.getenv('AZURE_STORAGE_CONTAINER')
        self._path = path

        # if container is specified, we are using real adls
        if self._container:
            self._path = f"abfs://{self._container}/{path}"

        self._storage_options = {'account_name': storage_account, 'anon': False}
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
        with self._lock:
            if self._container is None and self._storage_account is None:
                logger.debug(f"local cache(mode={mode}) access: {self._path}")
                return fsspec.open(self._path, mode=mode, encoding="utf-8")
            else:
                # register 'abfs:/' hanlder
                logger.debug(f"abfs cache(mode={mode}) access: {self._path}")
                AzureBlobFileSystem(**self._storage_options)

                return fsspec.open(self._path, mode=mode, **self._storage_options)

    def invalidate(self, key):
        with self._lock:
            if key in self._data:
                self._data.pop(key, None)
                self._change_counter = self._change_counter + 1
                self._auto_flush_if_needed()

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        with self._lock:
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
