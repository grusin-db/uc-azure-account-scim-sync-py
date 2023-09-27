from typing import List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import _generic_create_or_update


def delete_user_if_exists(client: AccountClient, email: str):
    for u in client.users.list(filter=f"userName eq '{email}'"):
        client.users.delete(u.id)


def create_or_update_users(client: AccountClient, desired_users: List[iam.User], dry_run=False):
    total_differences = []

    for desired in desired_users:
        total_differences.extend(
            _generic_create_or_update(desired=desired,
                                      actual_objects=list(
                                          client.users.list(filter=f"userName eq '{desired.user_name}'")),
                                      compare_fields=["displayName"],
                                      sdk_module=client.users,
                                      dry_run=dry_run))

    return total_differences
