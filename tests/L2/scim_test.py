import logging
import sys
from typing import List

import pytest
from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from azure_dbr_scim_sync.scim import (ScimSyncObject, create_or_update_groups,
                                      create_or_update_service_principals,
                                      create_or_update_users,
                                      delete_group_if_exists,
                                      delete_service_principal_if_exists,
                                      delete_user_if_exists,
                                      get_account_client, sync)

logging.basicConfig(stream=sys.stderr,
                    level=logging.INFO,
                    format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')
logging.getLogger('databricks.sdk').setLevel(logging.DEBUG)


@pytest.fixture()
def account_client():
    return get_account_client()


def test_smoke(account_client: AccountClient):
    len(account_client.metastores.list()) > 0


def test_create_or_update_users(account_client: AccountClient):
    users = [
        iam.User(user_name=f"test{idx}@example.com",
                 display_name=f"tester {idx}",
                 external_id=f"abc-{idx}",
                 active=True) for idx in range(0, 5)
    ]

    # pre-delete
    for x in users:
        delete_user_if_exists(account_client, x.user_name)

    # create users
    diff = create_or_update_users(account_client, users)
    assert len(diff) == 5
    for x in diff:
        assert x.action == "new"
        assert x.created
        assert x.created.id

    # do actual changes
    users2 = [
        iam.User(user_name=f"test{idx}@example.com",
                 display_name=f"tester {idx} v2",
                 external_id=f"abc-{idx}",
                 active=True) for idx in range(0, 3)
    ]

    diff2 = create_or_update_users(account_client, users2)
    assert len(diff2) == 3
    assert diff2[0].action == "change"
    assert diff2[0].changes[0].as_dict() == {'op': 'replace', 'path': 'displayName', 'value': 'tester 0 v2'}

    # next run should do no changes
    diff3 = create_or_update_users(account_client, users2)
    assert len(diff3) == 3
    for x in diff3:
        assert x.action == "no change"


def test_create_or_update_groups(account_client: AccountClient):
    groups = [
        iam.Group(display_name=f"test-example-grp-{idx}", external_id=f"abc-grp-{idx}")
        for idx in range(0, 5)
    ]

    # pre-delete
    for g in groups:
        delete_group_if_exists(account_client, g.display_name)

    # create groups
    diff = create_or_update_groups(account_client, groups)
    assert len(diff) == 5
    for x in diff:
        assert x.action == "new"
        assert x.created
        assert x.created.id

    # next run should do no changes
    diff2 = create_or_update_groups(account_client, groups)
    assert len(diff2) == 5
    for x in diff2:
        assert x.action == "no change"


def test_create_or_update_service_principals(account_client: AccountClient):
    spns = [
        iam.ServicePrincipal(application_id=f"00000000-1337-1337-1337-00000000000{idx}",
                             display_name=f"test-example-spn-{idx}",
                             external_id=f"abc-spn-{idx}") for idx in range(0, 5)
    ]

    # pre-delete
    for s in spns:
        delete_service_principal_if_exists(account_client, s.application_id)

    # create service prinsipals
    diff = create_or_update_service_principals(account_client, spns)
    assert len(diff) == 5
    for x in diff:
        assert x.action == "new"
        assert x.created
        assert x.created.id

    # next run should do no changes
    diff2 = create_or_update_service_principals(account_client, spns)
    assert len(diff2) == 5
    for x in diff2:
        assert x.action == "no change"

    # make some changes
    spns2 = [
        iam.ServicePrincipal(application_id=f"00000000-1337-1337-1337-00000000000{idx}",
                             display_name=f"test-example-spn-{idx}-fancy",
                             external_id=f"abc-spn-{idx}") for idx in range(0, 5)
    ]

    diff3 = create_or_update_service_principals(account_client, spns2)
    assert len(diff3) == 5
    for x in diff3:
        assert x.action == "change"


def test_group_membership(account_client: AccountClient):

    def _verify_group_members(groups: List[iam.Group], sync_results: ScimSyncObject):
        for idx, g in enumerate(groups):
            id = sync_results.groups[idx].id
            data = account_client.groups.get(id)

            assert set(m.display for m in data.members) == set(m.display for m in g.members)

    users = [
        iam.User(user_name=f"test-end2end-{idx}@example.com",
                 display_name=f"tester {idx} grp-end2end",
                 external_id=f"test-end2end-{idx}",
                 active=True) for idx in range(0, 5)
    ]

    groups = [
        iam.Group(display_name=f"test-end2end-grp-{idx}",
                  external_id=f"test-xyz-grp-end2end-{idx}",
                  members=[
                      iam.ComplexValue(display=u.display_name, value=u.external_id)
                      for uidx, u in enumerate(users) if uidx <= idx
                  ]) for idx in range(0, 5)
    ]

    # pre-delete
    for u in users:
        delete_user_if_exists(account_client, u.user_name)

    for g in groups:
        delete_group_if_exists(account_client, g.display_name)

    # run twice, to ensure nothing changes 2nd time
    for _ in ["first", "2nd"]:
        # sync
        sync_results = sync(account_client=account_client, users=users, groups=groups, service_principals=[])

        # verify if groups mach the results
        _verify_group_members(groups, sync_results)

    # move the groups around
    groups = [
        iam.Group(display_name=f"test-end2end-grp-{idx}",
                  external_id=f"test-xyz-grp-end2end-{idx}",
                  members=[
                      iam.ComplexValue(display=u.display_name, value=u.external_id)
                      for uidx, u in enumerate(users) if uidx >= idx
                  ]) for idx in range(0, 5)
    ]

    # should fail on group members, because we did not run processing yet :P
    with pytest.raises(AssertionError):
        _verify_group_members(groups, sync_results)

    sync_results = sync(account_client=account_client, users=users, groups=groups, service_principals=[])

    _verify_group_members(groups, sync_results)
