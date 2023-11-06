from typing import List

import pytest
from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from azure_dbr_scim_sync.scim import ScimSyncObject, get_account_client, sync


@pytest.fixture()
def account_client():
    return get_account_client()


def test_300(account_client: AccountClient):

    def _verify_group_members(groups: List[iam.Group], sync_results: ScimSyncObject):
        for idx, g in enumerate(groups):
            id = sync_results.groups[idx].id
            data = account_client.groups.get(id)

            assert set(m.display for m in data.members) == set(m.display for m in g.members)

    users = [
        iam.User(user_name=f"test-end2end-{idx}@example.com",
                 display_name=f"tester {idx} grp-end2end",
                 external_id=f"test-end2end-{idx}",
                 active=True) for idx in range(0, 300)
    ]

    spns = [
        iam.ServicePrincipal(application_id=f"00000000-1337-1337-{idx}-000000000000",
                             display_name=f"tester-spn-{idx} grp-end2end",
                             external_id=f"grp-end2end-spn-{idx}") for idx in range(1000, 1300)
    ]

    groups = [
        iam.Group(display_name=f"test-end2end-grp-{idx}",
                  external_id=f"test-xyz-grp-end2end-{idx}",
                  members=[
                      iam.ComplexValue(display=u.display_name, value=u.external_id)
                      for uidx, u in enumerate(users) if uidx <= idx and abs(uidx - idx) <= 20
                  ]) for idx in range(0, 100)
    ]

    # pre-delete
    # delete_users_if_exists(account_client, [u.user_name for u in users])
    # delete_groups_if_exists(account_client, [g.display_name for g in groups])

    # sync
    sync_results = sync(account_client=account_client,
                        users=users,
                        groups=groups,
                        service_principals=spns,
                        deep_sync_group_external_ids=[g.external_id for g in groups],
                        )

    # verify if groups mach the results
    _verify_group_members(groups, sync_results)
