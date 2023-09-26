import os

import pytest
from databricks.sdk import AccountClient


@pytest.fixture()
def client():
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID")
    assert account_id
    host = os.getenv("DATABRICKS_HOST")
    assert host

    return AccountClient(host=host, account_id=account_id)


def test_smoke(client: AccountClient):
    len(client.metastores.list()) > 0
