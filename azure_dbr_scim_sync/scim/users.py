from typing import Iterable

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import MergeResult, _generic_create_or_update


def delete_user_if_exists(client: AccountClient, email: str):
    for u in client.users.list(filter=f"userName eq '{email}'"):
        client.users.delete(u.id)


def create_or_update_users(client: AccountClient, desired_users: Iterable[iam.User], dry_run=False):
    total_differences: MergeResult[iam.User] = []

    for desired in desired_users:
        total_differences.extend(
            _generic_create_or_update(
                desired=desired,
                actual_objects=client.users.list(filter=f"userName eq '{desired.user_name}'"),
                compare_fields=["displayName"],
                sdk_module=client.users,
                dry_run=dry_run))

    return total_differences


def get_users_xref_by_mail(users: Iterable[iam.User]):
    return {u.user_name: u.id for u in users}
