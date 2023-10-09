from azure_dbr_scim_sync.persisted_cache import Cache


def test_local_persistance():
    # make clear cache
    c = Cache(None, None, '.test_cache_local_persistance.json')
    c.clear()
    assert c._data == {}

    # write some data
    c["abc"] = 1
    c["abcd"] = 2
    assert c._data == {"abc": 1, "abcd": 2}
    c.flush()

    # load cache back again
    c2 = Cache(None, None, '.test_cache_local_persistance.json')
    assert c2._data == {"abc": 1, "abcd": 2}


def test_misses():
    c = Cache(None, None, '.test_cache_misses.json')
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
