import os

import pytest

from azure_dbr_scim_sync.persisted_cache import Cache


def test_local_persistance():
    file_name = '.test_cache_local_persistance.json'
    # make clear cache
    c = Cache(None, None, file_name)
    c.clear()
    assert c._data == {}

    # write some data
    c["abc"] = 1
    c["abcd"] = 2
    assert c._data == {"abc": 1, "abcd": 2}
    c.flush()

    # load cache back again
    c2 = Cache(None, None, file_name)
    assert c2._data == {"abc": 1, "abcd": 2}

    c.flush()
    os.remove(file_name)


def test_misses():
    file_name = '.test_cache_misses.json'
    c = Cache(None, None, file_name)
    c.clear()

    def _mapping_logic(idx):
        return "a" * (idx + 1)

    def _validator(idx, value):
        return value == _mapping_logic(idx)

    for idx in range(0, 50):
        c[idx] = _mapping_logic(idx)

    # no poisoning
    assert c.get_and_validate(0, _validator) == "a"
    assert c.get_and_validate(7, _validator) == "a" * 8
    assert c.get_and_validate(61, _validator) == None

    # posioning
    c._data[7] = "b"

    # explicit no validator
    assert c.get_and_validate(7, None) == "b"
    # this should clean up poisoned cache
    assert c.get_and_validate(7, _validator) == None
    assert c.get_and_validate(7, None) == None
    assert c.get_and_validate(7, _validator) == None

    c.flush()
    os.remove(file_name)


def test_auto_flush():
    file_name = '.test_auto_flush.json'
    c = Cache(None, None, file_name)
    c.flush()
    os.remove(file_name)

    c = Cache(None, None, file_name)

    with pytest.raises(FileNotFoundError):
        os.remove(file_name)

    for idx in range(0, 50):
        c[idx] = idx * "x"

    os.remove(file_name)

    c.flush()
    os.remove(file_name)
