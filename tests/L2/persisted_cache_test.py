import os

import pytest

from azure_dbr_scim_sync.persisted_cache import Cache


def test_local_persistance():
    file_name = '.test_cache_local_persistance.json'
    # make clear cache
    c = Cache(file_name)
    c.clear()
    assert c._data == {}

    # write some data
    c["abc"] = 1
    c["abcd"] = 2
    assert c._data == {"abc": 1, "abcd": 2}
    c.flush()

    # load cache back again
    c2 = Cache(file_name)
    assert c2._data == {"abc": 1, "abcd": 2}

    c.flush()
    os.remove(file_name)


def test_misses():
    file_name = '.test_cache_misses.json'
    c = Cache(file_name)
    c.clear()

    for idx in range(0, 50):
        c[idx] = "a" * (idx + 1)

    assert c[0] == "a"
    assert c.get(7) == "a" * 8
    assert c.get(61) == None
    assert c[61] == None

    c.flush()
    os.remove(file_name)


def test_auto_flush():
    file_name = '.test_auto_flush.json'
    c = Cache(file_name)
    c.flush()
    os.remove(file_name)

    c = Cache(file_name)

    with pytest.raises(FileNotFoundError):
        os.remove(file_name)

    for idx in range(0, 50):
        c[idx] = idx * "x"

    os.remove(file_name)

    c.flush()
    os.remove(file_name)
