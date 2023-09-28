import itertools
from copy import deepcopy
from dataclasses import dataclass
from typing import Generic, Iterable, List, Optional, TypeVar

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from pydantic import BaseModel, Field

T = TypeVar("T")


@dataclass
class MergeResult(Generic[T]):
    desired: T
    actual: T
    created: T
    action: str
    changes: List[iam.Patch]

    @property
    def external_id(self) -> str:
        return self.desired.external_id

    @property
    def effective(self) -> T:
        return self.created or self.actual

    @property
    def id(self) -> str:
        return self.effective.id


def _generic_create_or_update(desired: T, actual_objects: Iterable[T], compare_fields: List[str], sdk_module,
                              dry_run: bool) -> List[T]:
    total_changes = []
    ResultClass = MergeResult[T]

    assert desired.external_id

    desired_dict = desired.as_dict()
    if not len(actual_objects):
        created = None

        if not dry_run:
            created: T = sdk_module.create(**desired.__dict__)
            assert created
            assert created.id

        total_changes.append(
            ResultClass(desired=desired, actual=None, action="new", changes=[], created=created))
    else:
        for actual in actual_objects:
            actual_dict = actual.as_dict()

            operations = [
                iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_dict[field_name])
                for field_name in compare_fields if desired_dict[field_name] != actual_dict.get(field_name)
            ]

            total_changes.append(
                ResultClass(desired=desired,
                            actual=actual,
                            action="change" if operations else "no change",
                            changes=operations,
                            created=None))

            if operations:
                if not dry_run:
                    sdk_module.patch(actual.id,
                                     schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                                     operations=operations)

    return total_changes


from .groups import create_or_update_groups, delete_group_if_exists  # NOQA
from .service_principals import create_or_update_service_principals  # NOQA
from .service_principals import delete_service_principal_if_exists  # NOQA
from .users import create_or_update_users, delete_user_if_exists  # NOQA

__all__ = [
    'create_or_update_users', 'create_or_update_groups', 'delete_user_if_exists', 'delete_group_if_exists',
    'create_or_update_service_principals', 'delete_service_principal_if_exists', 'ScimSyncObject', 'sync'
]


@dataclass
class ScimSyncObject:
    users: List[MergeResult[iam.User]]
    groups: List[MergeResult[iam.Group]]
    service_principals: List[MergeResult[iam.ServicePrincipal]]


def sync(account_client: AccountClient,
         users: Iterable[iam.User],
         groups: Iterable[iam.Group],
         service_principals: Iterable[iam.ServicePrincipal],
         dry_run=False):

    result = ScimSyncObject(users=create_or_update_users(account_client, users, dry_run),
                            service_principals=create_or_update_service_principals(
                                account_client, service_principals, dry_run),
                            groups=create_or_update_groups(account_client, groups, dry_run))

    if dry_run:
        return result

    # xref both ways
    graph_to_dbr_ids = {
        u.external_id: u.id
        for u in itertools.chain(result.users, result.groups, result.service_principals)
    }

    dbr_to_graph_ids = {
        u.id: u.external_id
        for u in itertools.chain(result.users, result.groups, result.service_principals)
    }

    # check which group members to add or remove
    for group_merge_result in result.groups:
        # desired group uses external_id's to show membership
        graph_group_member_ids = set(x.value for x in group_merge_result.desired.members)

        # .effective is either created, or actual group
        dbr_group = group_merge_result.effective
        dbr_group_members = dbr_group.members or []

        # we will action that using .patch command
        to_delete_member_dbr_ids = set()

        visited_member_dbr_ids = set()
        visited_member_graph_ids = set()

        # process members that needs deleting from dbr group
        for dbr_member in dbr_group_members:
            member_dbr_id = dbr_member.value
            member_graph_id = dbr_to_graph_ids.get(member_dbr_id)

            # not a known member, or not in graph group membership, mark as to remove
            if (not member_graph_id) or (member_graph_id not in graph_group_member_ids):
                to_delete_member_dbr_ids.add(member_dbr_id)
                continue

            # mark visited ones, so we don't consider them later
            visited_member_dbr_ids.add(member_dbr_id)
            visited_member_graph_ids.add(member_graph_id)

        # process members that needs adding to dbr group
        to_add_member_graph_ids = graph_group_member_ids - visited_member_graph_ids
        to_add_member_dbr_ids = set(graph_to_dbr_ids[x] for x in to_add_member_graph_ids
                                    if x in graph_to_dbr_ids)

        # create patch entries
        # https://api-docs.databricks.com/rest/latest/account-scim-api.html
        patch_operations = []
        if to_add_member_dbr_ids:
            patch_operations.append(
                iam.Patch(op=iam.PatchOp.ADD,
                          value={'members': [{
                              'value': x
                          } for x in to_add_member_dbr_ids]}))

        if to_delete_member_dbr_ids:
            patch_operations.extend([
                iam.Patch(op=iam.PatchOp.REMOVE, path=f"members[value eq \"{x}\"]")
                for x in to_delete_member_dbr_ids
            ])

        if patch_operations:
            group_merge_result.changes.extend(patch_operations)

            account_client.groups.patch(
                id=group_merge_result.id,
                operations=patch_operations,
                schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP])

    print("dupa")
    return result
