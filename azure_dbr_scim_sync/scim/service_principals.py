from typing import Iterable, List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import MergeResult, _generic_create_or_update

import logging

logger = logging.getLogger('sync.scim.service_principals')

def delete_service_principal_if_exists(client: AccountClient, application_id: str):
    for s in client.service_principals.list(filter=f"applicationId eq '{application_id}'"):
        logging.info(f"deleting service principal: {s}")
        client.service_principals.delete(s.id)


def create_or_update_service_principals(client: AccountClient,
                                        desired_service_principals: Iterable[iam.ServicePrincipal],
                                        dry_run=False):
    
    logger.info(f"[{dry_run=}] Starting processing service principals: total={len(desired_service_principals)}")

    merge_results: List[MergeResult[iam.ServicePrincipal]] = []

    for desired in desired_service_principals:
        merge_results.extend(
            _generic_create_or_update(desired=desired,
                                      actual_objects=client.service_principals.list(
                                          filter=f"applicationId eq '{desired.application_id}'"),
                                      compare_fields=["displayName"],
                                      sdk_module=client.service_principals,
                                      dry_run=dry_run,
                                      logger=logger))
        
    total_change_count = sum(x.effecitve_change_count for x in merge_results)
    logger.info(f"[{dry_run=}] Finished processing service principals: changes={total_change_count}, total={len(desired_service_principals)}")

    return merge_results

