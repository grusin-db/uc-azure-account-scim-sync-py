import logging
import os
import sys

import pytest
from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from azure_dbr_scim_sync.scim import create_or_update_users, create_or_update_groups, delete_user_if_exists, delete_group_if_exists

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

    # pre-delete
    for u in users:
        delete_user_if_exists(client, u.user_name)

    # create users
    diff = create_or_update_users(client, users)
    assert len(diff) == 5
    for u in diff:
        u.action == "new"

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


def test_create_or_update_groups(client: AccountClient):
    groups = [
        iam.Group(display_name=f"test-example-grp-{idx}", external_id=f"abc-grp-{idx}")
        for idx in range(0, 5)
    ]

    # pre-delete
    for g in groups:
        delete_group_if_exists(client, g.display_name)

    # create groups
    diff = create_or_update_groups(client, groups)
    assert len(diff) == 5
    for u in diff:
        u.action == "new"

    # next run should do no changes
    diff2 = create_or_update_groups(client, groups)
    assert len(diff2) == 0
