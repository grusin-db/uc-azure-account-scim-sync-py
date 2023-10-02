import logging
from typing import Iterable, List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from joblib import Parallel, delayed

from . import MergeResult, _generic_create_or_update, retry_on_429

logger = logging.getLogger('sync.scim.users')


def delete_groups_if_exists(client: AccountClient, group_name_list: List[str], worker_threads: int = 3):
    Parallel(backend='multiprocessing', verbose=100,
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
                           dry_run=False,
                           logger=None) -> List[MergeResult[iam.Group]]:
    return _generic_create_or_update(desired=desired_group,
                                     actual_objects=client.groups.list(
                                         filter=f"displayName eq '{desired_group.display_name}'",
                                         attributes="id,displayName,externalId,members"),
                                     compare_fields=["displayName"],
                                     sdk_module=client.groups,
                                     dry_run=dry_run,
                                     logger=logger)


def create_or_update_groups(client: AccountClient,
                            desired_groups: Iterable[iam.Group],
                            dry_run=False,
                            worker_threads: int = 3):
    logger.info(f"[{dry_run=}] Starting processing groups, total={len(desired_groups)}")

    merge_results: List[MergeResult[iam.Group]] = Parallel(
        backend='multiprocessing', verbose=100,
        n_jobs=worker_threads)(delayed(create_or_update_group)(client, desired, dry_run, logger)
                               for desired in desired_groups)

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(
        f"[{dry_run=}] Finished processing: changes={total_change_count}, total={len(desired_groups)}")

    return merge_results
