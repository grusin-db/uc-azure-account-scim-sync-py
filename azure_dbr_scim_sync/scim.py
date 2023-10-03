import functools
import itertools
import logging
import os
import time
from dataclasses import dataclass
from typing import Generic, Iterable, List, TypeVar

from databricks.sdk import AccountClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from joblib import Parallel, delayed

from .version import __version__

T = TypeVar("T")

logger = logging.getLogger('sync.scim')


def get_account_client():
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID")
    if not account_id:
        raise ValueError("unknown account_id, set DATABRICKS_ACCOUNT_ID environment variable!")

    host = os.getenv("DATABRICKS_HOST")
    if not host:
        raise ValueError("unknown host, set DATABRICKS_HOST environment variable!")

    client_id = os.getenv('ARM_CLIENT_ID') or os.getenv('DATABRICKS_ARM_CLIENT_ID')
    client_secret = os.getenv('ARM_CLIENT_SECRET') or os.getenv('DATABRICKS_ARM_CLIENT_SECRET')

    if client_id and client_secret:
        logger.info("Using env variables auth")
        return AccountClient(host=host,
                             account_id=account_id,
                             client_id=client_id,
                             client_secret=client_secret,
                             auth_type="azure",
                             product="azure_dbr_scim_sync",
                             product_version=__version__)
    else:
        # allow AccountClient do it's own auth method
        logger.info("Using databricks.sdk auth probing")
        return AccountClient(host=host,
                             account_id=account_id,
                             product="azure_dbr_scim_sync",
                             product_version=__version__)


def retry_on_429(retry_num, retry_sleep_sec):
    """
    retry help decorator.
    :param retry_num: the retry num; retry sleep sec
    :return: decorator
    """

    def decorator(func):
        """decorator"""
        # preserve information about the original function, or the func name will be "wrapper" not "func"
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """wrapper"""
            for attempt in range(retry_num):
                try:
                    return func(*args, **kwargs)
                except DatabricksError as err:
                    msg = str(err) or ""
                    if "Too Many Requests" in msg or msg.startswith("50"):
                        logging.error("Trying attempt %s of %s. (in %s seconds)", attempt + 1, retry_num,
                                      retry_sleep_sec)
                        time.sleep(retry_sleep_sec)
                    else:
                        raise err

            logging.error("func %s retry failed", func)
            raise Exception('Exceed max retry num: {} failed'.format(retry_num))

        return wrapper

    return decorator


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

    @property
    def effecitve_change_count(self):
        if self.action == "new":
            return 1

        return len(self.changes)


def _generic_create_or_update(desired: T, actual_objects: Iterable[T], compare_fields: List[str], sdk_module,
                              dry_run: bool) -> T:
    total_changes = []
    ResultClass = MergeResult[T]

    assert desired.external_id

    desired_dict = desired.as_dict()
    if not len(actual_objects):
        created = None

        logger.info(f"[{dry_run=}] creating: {desired}")
        if not dry_run:
            created: T = sdk_module.create(**desired.__dict__)
            assert created
            assert created.id

        return ResultClass(desired=desired, actual=None, action="new", changes=[], created=created)
    elif len(actual_objects) > 1:
        raise ValueError(f"expected one object, but multiple objects were found: {actual_objects}")
    else:
        actual = actual_objects[0]
        actual_dict = actual.as_dict()

        operations = [
            iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_dict[field_name])
            for field_name in compare_fields if desired_dict[field_name] != actual_dict.get(field_name)
        ]

        if operations:
            logger.info(f"[{dry_run=}] changing, current={actual}, changes: {total_changes}")
            if not dry_run:
                sdk_module.patch(actual.id,
                                 schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                                 operations=operations)
        else:
            logger.debug(f"[{dry_run=}] no changes, current={actual}")

        return ResultClass(desired=desired,
                           actual=actual,
                           action="change" if operations else "no change",
                           changes=operations,
                           created=None)


@dataclass
class ScimSyncObject:
    users: List[MergeResult[iam.User]]
    groups: List[MergeResult[iam.Group]]
    service_principals: List[MergeResult[iam.ServicePrincipal]]

    @property
    def users_effecitve_change_count(self):
        return sum(x.effecitve_change_count for x in self.users)

    @property
    def groups_effecitve_change_count(self):
        return sum(x.effecitve_change_count for x in self.groups)

    @property
    def service_principals_effecitve_change_count(self):
        return sum(x.effecitve_change_count for x in self.service_principals)

    @property
    def effecitve_change_count(self):
        return self.users_effecitve_change_count + self.groups_effecitve_change_count + self.service_principals_effecitve_change_count


#
# Users
#
def delete_users_if_exists(client: AccountClient, user_name_list: List[str], worker_threads: int = 3):
    Parallel(backend='threading', verbose=100,
             n_jobs=worker_threads)(delayed(delete_user_if_exists)(client, user_name)
                                    for user_name in user_name_list)


@retry_on_429(10, 1)
def delete_user_if_exists(client: AccountClient, email: str):
    for u in client.users.list(filter=f"userName eq '{email}'"):
        logging.info(f"Deleting user: {u}")
        client.users.delete(u.id)


@retry_on_429(10, 1)
def create_or_update_user(client: AccountClient, desired_user: iam.User, dry_run=False):
    return _generic_create_or_update(
        desired=desired_user,
        actual_objects=client.users.list(filter=f"userName eq '{desired_user.user_name}'"),
        compare_fields=["displayName"],
        sdk_module=client.users,
        dry_run=dry_run)


def create_or_update_users(client: AccountClient,
                           desired_users: Iterable[iam.User],
                           dry_run=False,
                           worker_threads: int = 3):
    logger.info(f"[{dry_run=}] Starting processing users: total={len(desired_users)}")

    merge_results: List[MergeResult[iam.Group]] = Parallel(
        backend='threading', verbose=100,
        n_jobs=worker_threads)(delayed(create_or_update_user)(client, desired, dry_run)
                               for desired in desired_users)

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(
        f"[{dry_run=}] Finished processing users, changes={total_change_count}, total={len(desired_users)}")

    return merge_results


#
# Groups
#
def delete_groups_if_exists(client: AccountClient, group_name_list: List[str], worker_threads: int = 3):
    Parallel(backend='threading', verbose=100,
             n_jobs=worker_threads)(delayed(delete_group_if_exists)(client, group_name)
                                    for group_name in group_name_list)


@retry_on_429(10, 1)
def delete_group_if_exists(client: AccountClient, group_name: str):
    for g in client.groups.list(filter=f"displayName eq '{group_name}'"):
        logging.info(f"Deleting group: {g}")
        client.groups.delete(g.id)


@retry_on_429(10, 1)
def create_or_update_group(client: AccountClient,
                           desired_group: iam.Group,
                           dry_run=False) -> List[MergeResult[iam.Group]]:
    return _generic_create_or_update(desired=desired_group,
                                     actual_objects=client.groups.list(
                                         filter=f"displayName eq '{desired_group.display_name}'",
                                         attributes="id,displayName,externalId,members"),
                                     compare_fields=["displayName"],
                                     sdk_module=client.groups,
                                     dry_run=dry_run)


def create_or_update_groups(client: AccountClient,
                            desired_groups: Iterable[iam.Group],
                            dry_run=False,
                            worker_threads: int = 3):
    logger.info(f"[{dry_run=}] Starting processing groups, total={len(desired_groups)}")

    merge_results: List[MergeResult[iam.Group]] = Parallel(
        backend='threading', verbose=100,
        n_jobs=worker_threads)(delayed(create_or_update_group)(client, desired, dry_run)
                               for desired in desired_groups)

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(
        f"[{dry_run=}] Finished processing: changes={total_change_count}, total={len(desired_groups)}")

    return merge_results


#
# service principals
#
def delete_service_principals_if_exists(client: AccountClient,
                                        service_principals_list: List[str],
                                        worker_threads: int = 3):
    Parallel(backend='threading', verbose=100,
             n_jobs=worker_threads)(delayed(delete_service_principal_if_exists)(client, service_principal)
                                    for service_principal in service_principals_list)


@retry_on_429(10, 1)
def delete_service_principal_if_exists(client: AccountClient, application_id: str):
    for s in client.service_principals.list(filter=f"applicationId eq '{application_id}'"):
        logging.info(f"deleting service principal: {s}")
        client.service_principals.delete(s.id)


@retry_on_429(10, 1)
def create_or_update_service_principal(client: AccountClient,
                                       desired_service_principal: iam.ServicePrincipal,
                                       dry_run=False) -> List[MergeResult[iam.ServicePrincipal]]:
    return _generic_create_or_update(
        desired=desired_service_principal,
        actual_objects=client.service_principals.list(
            filter=f"applicationId eq '{desired_service_principal.application_id}'"),
        compare_fields=["displayName"],
        sdk_module=client.service_principals,
        dry_run=dry_run)


def create_or_update_service_principals(client: AccountClient,
                                        desired_service_principals: Iterable[iam.ServicePrincipal],
                                        dry_run=False,
                                        worker_threads: int = 3):

    logger.info(
        f"[{dry_run=}] Starting processing service principals: total={len(desired_service_principals)}")

    merge_results: List[MergeResult[iam.ServicePrincipal]] = Parallel(
        backend='threading', verbose=100,
        n_jobs=worker_threads)(delayed(create_or_update_service_principal)(client, desired, dry_run)
                               for desired in desired_service_principals)

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(
        f"[{dry_run=}] Finished processing service principals: changes={total_change_count}, total={len(desired_service_principals)}"
    )

    return merge_results


def sync(account_client: AccountClient,
         users: Iterable[iam.User],
         groups: Iterable[iam.Group],
         service_principals: Iterable[iam.ServicePrincipal],
         dry_run=False):

    logger.info("Starting creating or updating users, groups and service principals...")
    result = ScimSyncObject(users=create_or_update_users(account_client, users, dry_run),
                            service_principals=create_or_update_service_principals(
                                account_client, service_principals, dry_run),
                            groups=create_or_update_groups(account_client, groups, dry_run))

    logger.info(
        f"Finished creating and updating, changes counts: users={result.users_effecitve_change_count}, groups={result.groups_effecitve_change_count}, service_principals={result.service_principals_effecitve_change_count}"
    )
    if dry_run:
        if result.effecitve_change_count != 0:
            logger.warning(
                f"There are pending changes, dry run cannot continue without first applying these changes. run with --todo-flag-here to apply above changes"
            )
            return result
        else:
            logger.info(f"There are no pending changes, dry run will continue...")

    logger.info("Starting synchronization of group members...")

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
            logger.info(
                f"group {group_merge_result.desired.display_name} members changes: {patch_operations}")
            group_merge_result.changes.extend(patch_operations)

            if not dry_run:
                account_client.groups.patch(
                    id=group_merge_result.id,
                    operations=patch_operations,
                    schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP])

    return result
