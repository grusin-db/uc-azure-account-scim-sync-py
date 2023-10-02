import logging
from typing import Iterable, List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam
from joblib import Parallel, delayed

from . import MergeResult, _generic_create_or_update

logger = logging.getLogger('sync.scim.service_principals')


def delete_service_principals_if_exists(client: AccountClient,
                                        service_principals_list: List[str],
                                        worker_threads: int = 3):
    Parallel(backend='threading', verbose=100,
             n_jobs=worker_threads)(delayed(delete_service_principal_if_exists)(client, service_principal)
                                    for service_principal in service_principals_list)


def delete_service_principal_if_exists(client: AccountClient, application_id: str):
    for s in client.service_principals.list(filter=f"applicationId eq '{application_id}'"):
        logging.info(f"deleting service principal: {s}")
        client.service_principals.delete(s.id)


def create_or_update_service_principal(client: AccountClient,
                                       desired_service_principal: iam.ServicePrincipal,
                                       dry_run=False,
                                       logger=None) -> List[MergeResult[iam.ServicePrincipal]]:
    return _generic_create_or_update(
        desired=desired_service_principal,
        actual_objects=client.service_principals.list(
            filter=f"applicationId eq '{desired_service_principal.application_id}'"),
        compare_fields=["displayName"],
        sdk_module=client.service_principals,
        dry_run=dry_run,
        logger=logger)


def create_or_update_service_principals(client: AccountClient,
                                        desired_service_principals: Iterable[iam.ServicePrincipal],
                                        dry_run=False,
                                        worker_threads: int = 3):

    logger.info(
        f"[{dry_run=}] Starting processing service principals: total={len(desired_service_principals)}")

    merge_results: List[MergeResult[iam.ServicePrincipal]] = Parallel(
        backend='threading', verbose=100,
        n_jobs=worker_threads)(delayed(create_or_update_service_principal)(client, desired, dry_run, logger)
                               for desired in desired_service_principals)

    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(
        f"[{dry_run=}] Finished processing service principals: changes={total_change_count}, total={len(desired_service_principals)}"
    )

    return merge_results
