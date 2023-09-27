from typing import List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import generic_create_or_update


def create_or_update_users(client: AccountClient, desired_users: List[iam.Group], dry_run=False):
    total_differences = []

    for desired in desired_users:
        total_differences.extend(
            generic_create_or_update(
                desired=desired,
                actual_objects=client.groups.list(filter=f"displayName eq '{desired.display_name}'"),
                compare_fields=["displayName"],
                sdk_module=client.users,
                dry_run=dry_run))

    return total_differences
