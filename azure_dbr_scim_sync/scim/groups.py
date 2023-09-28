from typing import Iterable, List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import MergeResult, _generic_create_or_update


def delete_group_if_exists(client: AccountClient, group_name: str):
    for g in client.groups.list(filter=f"displayName eq '{group_name}'"):
        client.groups.delete(g.id)


def create_or_update_groups(client: AccountClient, desired_groups: Iterable[iam.Group], dry_run=False):
    total_differences: List[MergeResult[iam.Group]] = []

    for desired in desired_groups:
        total_differences.extend(
            _generic_create_or_update(desired=desired,
                                      actual_objects=client.groups.list(
                                          filter=f"displayName eq '{desired.display_name}'",
                                          attributes="id,displayName,externalId,members"),
                                      compare_fields=["displayName"],
                                      sdk_module=client.groups,
                                      dry_run=dry_run))

    return total_differences


def get_groups_xref_by_display_name(groups: Iterable[iam.Group]):
    return {g.display_name: g.id for g in groups}
