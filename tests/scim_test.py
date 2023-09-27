import logging
import os
import sys

import pytest
from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from azure_dbr_scim_sync.scim.users import create_or_update_users

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
    users = [
        iam.User(user_name=f"test{idx}@example.com",
                 display_name=f"tester {idx}",
                 external_id=f"abc-{idx}",
                 active=True) for idx in range(0, 5)
    ]

    # set the stage
    diff = create_or_update_users(client, users)
    assert diff is not None

    # do actual changes
    users2 = [
        iam.User(user_name=f"test{idx}@example.com",
                 display_name=f"tester {idx} v2",
                 external_id=f"abc-{idx}",
                 active=True) for idx in range(0, 3)
    ]

    diff2 = create_or_update_users(client, users2)
    assert len(diff2) == 3

    assert diff2[0].action == "change"
    assert diff2[0].changes[0].as_dict() == {'op': 'replace', 'path': 'displayName', 'value': 'tester 0 v2'}

    # next run should do no changes
    diff3 = create_or_update_users(client, users2)
    assert len(diff3) == 0
