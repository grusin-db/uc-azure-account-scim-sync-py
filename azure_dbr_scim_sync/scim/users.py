import logging
from typing import Iterable, List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import MergeResult, _generic_create_or_update

logger = logging.getLogger('sync.scim.users')


def delete_user_if_exists(client: AccountClient, email: str):
    for u in client.users.list(filter=f"userName eq '{email}'"):
        logging.info(f"deleting user: {u}")
        client.users.delete(u.id)


def create_or_update_users(client: AccountClient, desired_users: Iterable[iam.User], dry_run=False):
    logger.info(f"[{dry_run=}] Starting processing users: total={len(desired_users)}")

    merge_results: List[MergeResult[iam.User]] = []

    for desired in desired_users:
        merge_results.extend(
            _generic_create_or_update(
                desired=desired,
                actual_objects=client.users.list(filter=f"userName eq '{desired.user_name}'"),
                compare_fields=["displayName"],
                sdk_module=client.users,
                dry_run=dry_run,
                logger=logger))

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(
        f"[{dry_run=}] Finished processing users, changes={total_change_count}, total={len(desired_users)}")

    return merge_results
