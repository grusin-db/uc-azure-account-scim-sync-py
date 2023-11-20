import functools
import itertools
import logging
import os
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Callable, Generic, Iterable, List, TypeVar

from databricks.sdk import AccountClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from joblib import Parallel, delayed

from .persisted_cache import Cache
from .version import __version__

T = TypeVar("T")

logger = logging.getLogger('sync.scim')

user_cache = Cache(path='cache_user.json')
group_cache = Cache(path='cache_group.json')
spn_cache = Cache(path='cache_spn.json')


def get_account_client():
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID")
    if not account_id:
        raise ValueError("unknown account_id, set DATABRICKS_ACCOUNT_ID environment variable!")

    logger.info(f"Using Databricks Account Id={account_id}")

    host = os.getenv("DATABRICKS_HOST")
    if not host:
        raise ValueError("unknown host, set DATABRICKS_HOST environment variable!")

    logger.info(f"Using Databricks Host={host}")

    client_id = os.getenv('DATABRICKS_ARM_CLIENT_ID') or os.getenv('ARM_CLIENT_ID')
    logger.info(f"Using Client ID={client_id}")
    client_secret = os.getenv('DATABRICKS_ARM_CLIENT_SECRET') or os.getenv('ARM_CLIENT_SECRET')
    logger.info(f"Using Client Secret={'[REDACTED]' if client_secret else ''}")

    if client_id and client_secret:
        logger.info("Using env variables auth")
        return AccountClient(host=host,
                             account_id=account_id,
                             client_id=client_id,
                             client_secret=client_secret,
                             auth_type="azure-client-secret",
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


_generic_type_map = {
    'user': {
        'key_obj_field': 'user_name',
        'key_api_field': 'userName',
        'cache': user_cache
    },
    'group': {
        'key_obj_field': 'display_name',
        'key_api_field': 'displayName',
        'cache': group_cache
    },
    'spn': {
        'key_obj_field': 'application_id',
        'key_api_field': 'applicationId',
        'cache': spn_cache
    }
}


def _generic_get_by_human_name(mapper, sdk_module, search_name):
    cache = mapper['cache']
    key_obj_field = mapper['key_obj_field']
    key_api_field = mapper['key_api_field']

    cached_id = cache[search_name]
    obj = None

    if cached_id:
        # verify cache
        try:
            obj = sdk_module.get(cached_id)
            if obj.__dict__[key_obj_field] == search_name:
                # hit!
                logger.debug(f"Cache hit: {search_name=}, {obj.id=}")
                return obj
        except Exception:
            logger.debug(f"Cache poison! {cached_id=} does not exist")

        cache.invalidate(search_name)

    # cache miss or cache poison scenario
    res = sdk_module.list(filter=f"{key_api_field} eq '{search_name}'")
    if res and len(res) == 1:
        obj = res[0]
        cache[search_name] = obj.id
        logger.debug(f"Found, {search_name=}, {obj.id=}")
        return obj

    # user does not exist
    logger.debug(f"{search_name=} does not exist")
    return None


def _delete_if_exists_by_human_name(mapper, sdk_module, search_name):
    obj = _generic_get_by_human_name(mapper, sdk_module, search_name)
    if obj:
        logging.info(f"Deleting: {obj}")
        sdk_module.delete(obj.id)
        user_cache.invalidate(search_name)


def _delete_if_exists_by_human_name_parallel(mapper, sdk_module, search_names, worker_threads):
    Parallel(backend='threading', verbose=100,
             n_jobs=worker_threads)(delayed(_delete_if_exists_by_human_name)(mapper, sdk_module, search_name)
                                    for search_name in search_names)


def _generic_create_or_update(mapper, desired: T, actual: T, compare_fields: List[str], sdk_module,
                              dry_run: bool) -> T:
    ResultClass = MergeResult[T]
    cache = mapper['cache']
    key_obj_field = mapper['key_obj_field']
    mapper['key_api_field']

    desired = deepcopy(desired)
    desired_dict = desired.as_dict()

    if not actual:
        created = None

        logger.info(f"[{dry_run=}] creating: {desired}")
        if not dry_run:
            d = desired.__dict__

            # dont create members, they might not be yet existing
            if isinstance(desired, iam.Group):
                d['members'] = []

            created: T = sdk_module.create(**d)
            assert created
            assert created.id

            cache[created.__dict__[key_obj_field]] = created.id

        return ResultClass(desired=desired, actual=None, action="new", changes=[], created=created)
    else:
        assert actual.id
        actual_dict = actual.as_dict()

        operations = [
            iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_dict[field_name])
            for field_name in compare_fields if desired_dict[field_name] != actual_dict.get(field_name)
        ]

        if operations:
            logger.info(f"[{dry_run=}] changing, current={actual}, changes: {operations}")
            if not dry_run:
                sdk_module.patch(actual.id,
                                 schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                                 operations=operations)

                cache.invalidate(desired.__dict__[key_obj_field])
        else:
            logger.debug(f"[{dry_run=}] no changes, current={actual}")

        return ResultClass(desired=desired,
                           actual=actual,
                           action="change" if operations else "no change",
                           changes=operations,
                           created=None)


def _generic_create_or_update_parallel(client: AccountClient,
                                       desired_objs: Iterable[T],
                                       create_fun: Callable,
                                       dry_run=False,
                                       worker_threads: int = 3):
    logger.info(f"[{dry_run=}] Starting processing: total={len(desired_objs)}")

    merge_results: List[MergeResult[T]] = Parallel(backend='threading', verbose=100, n_jobs=worker_threads)(
        delayed(create_fun)(client, desired, dry_run) for desired in desired_objs)

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(f"[{dry_run=}] Finished processing, changes={total_change_count}, total={len(desired_objs)}")

    return merge_results


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
def get_user_by_email(client: AccountClient, user_name: str) -> iam.User:
    return _generic_get_by_human_name(_generic_type_map['user'], client.users, user_name)


def delete_users_if_exists(client: AccountClient, user_name_list: List[str], worker_threads: int = 3):
    _delete_if_exists_by_human_name_parallel(_generic_type_map['user'], client.users, user_name_list,
                                             worker_threads)


@retry_on_429(100, 1)
def delete_user_if_exists(client: AccountClient, email: str):
    _delete_if_exists_by_human_name(_generic_type_map['user'], client.users, email)


@retry_on_429(100, 1)
def create_or_update_user(client: AccountClient, desired_user: iam.User, dry_run=False):
    return _generic_create_or_update(mapper=_generic_type_map['user'],
                                     desired=desired_user,
                                     actual=get_user_by_email(client, desired_user.user_name),
                                     compare_fields=["displayName", "active"],
                                     sdk_module=client.users,
                                     dry_run=dry_run)


def create_or_update_users(client: AccountClient,
                           desired_users: Iterable[iam.User],
                           dry_run=False,
                           worker_threads: int = 3):

    ret = _generic_create_or_update_parallel(client=client,
                                             desired_objs=desired_users,
                                             create_fun=create_or_update_user,
                                             dry_run=dry_run,
                                             worker_threads=worker_threads)
    user_cache.flush()
    return ret


#
# Groups
#
def get_group_by_name(client: AccountClient, group_name: str) -> iam.Group:
    return _generic_get_by_human_name(_generic_type_map['group'], client.groups, group_name)


def delete_groups_if_exists(client: AccountClient, group_name_list: List[str], worker_threads: int = 3):
    _delete_if_exists_by_human_name_parallel(_generic_type_map['group'], client.groups, group_name_list,
                                             worker_threads)


@retry_on_429(100, 1)
def delete_group_if_exists(client: AccountClient, group_name: str):
    _delete_if_exists_by_human_name(_generic_type_map['group'], client.groups, group_name)


@retry_on_429(100, 1)
def create_or_update_group(client: AccountClient,
                           desired_group: iam.Group,
                           dry_run=False) -> List[MergeResult[iam.Group]]:
    return _generic_create_or_update(mapper=_generic_type_map['group'],
                                     desired=desired_group,
                                     actual=get_group_by_name(client, desired_group.display_name),
                                     compare_fields=["displayName"],
                                     sdk_module=client.groups,
                                     dry_run=dry_run)


def create_or_update_groups(client: AccountClient,
                            desired_groups: Iterable[iam.Group],
                            dry_run=False,
                            worker_threads: int = 3):
    ret = _generic_create_or_update_parallel(client=client,
                                             desired_objs=desired_groups,
                                             create_fun=create_or_update_group,
                                             dry_run=dry_run,
                                             worker_threads=worker_threads)

    group_cache.flush()
    return ret


#
# Service principals
#
def get_service_principals_by_app(client: AccountClient, application_id: str) -> iam.ServicePrincipal:
    return _generic_get_by_human_name(_generic_type_map['spn'], client.service_principals, application_id)


def delete_service_principals_if_exists(client: AccountClient,
                                        application_id_list: List[str],
                                        worker_threads: int = 3):
    _delete_if_exists_by_human_name_parallel(_generic_type_map['spn'], client.service_principals,
                                             application_id_list, worker_threads)


@retry_on_429(100, 1)
def delete_service_principal_if_exists(client: AccountClient, application_id: str):
    _delete_if_exists_by_human_name(_generic_type_map['spn'], client.service_principals, application_id)


@retry_on_429(100, 1)
def create_or_update_service_principal(client: AccountClient,
                                       desired_service_principal: iam.ServicePrincipal,
                                       dry_run=False) -> List[MergeResult[iam.ServicePrincipal]]:
    return _generic_create_or_update(mapper=_generic_type_map['spn'],
                                     desired=desired_service_principal,
                                     actual=get_service_principals_by_app(
                                         client, desired_service_principal.application_id),
                                     compare_fields=["displayName", "active"],
                                     sdk_module=client.service_principals,
                                     dry_run=dry_run)


def create_or_update_service_principals(client: AccountClient,
                                        desired_service_principals: Iterable[iam.ServicePrincipal],
                                        dry_run=False,
                                        worker_threads: int = 3):

    ret = _generic_create_or_update_parallel(client=client,
                                             desired_objs=desired_service_principals,
                                             create_fun=create_or_update_service_principal,
                                             dry_run=dry_run,
                                             worker_threads=worker_threads)
    spn_cache.flush()
    return ret


#
# Sync
#


def sync(*,
         account_client: AccountClient,
         users: Iterable[iam.User],
         groups: Iterable[iam.Group],
         service_principals: Iterable[iam.ServicePrincipal],
         deep_sync_group_external_ids: Iterable[str],
         dry_run_security_principals=False,
         dry_run_members=False,
         worker_threads: int = 10):

    logger.info("Starting creating or updating users, groups and service principals...")
    result = ScimSyncObject(users=create_or_update_users(account_client,
                                                         users,
                                                         dry_run=dry_run_security_principals,
                                                         worker_threads=worker_threads),
                            service_principals=create_or_update_service_principals(
                                account_client,
                                service_principals,
                                dry_run=dry_run_security_principals,
                                worker_threads=worker_threads),
                            groups=create_or_update_groups(account_client,
                                                           groups,
                                                           dry_run=dry_run_security_principals,
                                                           worker_threads=worker_threads))

    logger.info(
        f"Finished creating and updating, changes counts: users={result.users_effecitve_change_count}, groups={result.groups_effecitve_change_count}, service_principals={result.service_principals_effecitve_change_count}"
    )
    if dry_run_security_principals:
        if result.effecitve_change_count != 0:
            logger.warning(
                "There are pending changes, dry run cannot continue without first applying these changes. Run with --dry-run-members to apply above changes and display changes to group membership without applying them."
            )
            return result
        else:
            logger.info("There are no pending changes, dry run will continue...")

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

    # FIXME: is there possibility that this would ever fail?
    assert len(graph_to_dbr_ids) == len(dbr_to_graph_ids)

    # check which group members to add or remove
    for group_merge_result in result.groups:
        if group_merge_result.external_id not in deep_sync_group_external_ids:
            logger.warning(
                f"Shallow synced group detected, skipping member sync for: name={group_merge_result.effective.display_name}, id={group_merge_result.external_id}"
            )
            continue

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

            # if not in graph group membership, mark as to remove
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

            if not dry_run_members:
                account_client.groups.patch(
                    id=group_merge_result.id,
                    operations=patch_operations,
                    schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP])

    return result
