from typing import List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import _generic_create_or_update


def delete_group_if_exists(client: AccountClient, group_name: str):
    for g in client.groups.list(filter=f"displayName eq '{group_name}'"):
        client.groups.delete(g.id)


def create_or_update_groups(client: AccountClient, desired_users: List[iam.Group], dry_run=False):
    total_differences = []

    for desired in desired_users:
        total_differences.extend(
            _generic_create_or_update(
                desired=desired,
                actual_objects=client.groups.list(filter=f"displayName eq '{desired.display_name}'"),
                compare_fields=["displayName"],
                sdk_module=client.groups,
                dry_run=dry_run))

    return total_differences
