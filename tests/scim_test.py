import logging
import os
import sys

import pytest
from databricks.sdk import AccountClient

from azure_dbr_scim_sync.scim.users import DesiredUser, create_or_update_users

logging.basicConfig(stream=sys.stderr,
                    level=logging.INFO,
                    format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')
logging.getLogger('databricks.sdk').setLevel(logging.DEBUG)


@pytest.fixture()
def client():
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID")
    assert account_id
    host = os.getenv("DATABRICKS_HOST")
    assert host

    return AccountClient(host=host, account_id=account_id)


def test_smoke(client: AccountClient):
    len(client.metastores.list()) > 0


def test_create_or_update_users(client: AccountClient):

    diff = create_or_update_users(client, [
        DesiredUser(
            user_name="test1@example.com", display_name="tester one 10", external_id="abc-1", active=True),
        DesiredUser(
            user_name="test2@example.com", display_name="tester two 10", external_id="abc-2", active=True),
        DesiredUser(
            user_name="test3@example.com", display_name="tester three 10", external_id="abc-3", active=True),
        DesiredUser(user_name="test4@example.com", display_name="tester 10", external_id="abc-4", active=True)
    ])

    print(diff)
