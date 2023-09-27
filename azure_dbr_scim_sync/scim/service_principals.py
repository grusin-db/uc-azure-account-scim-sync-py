from typing import List

from databricks.sdk import AccountClient
from databricks.sdk.service import iam

from . import _generic_create_or_update


def delete_service_principal_if_exists(client: AccountClient, application_id: str):
    for s in client.service_principals.list(filter=f"applicationId eq '{application_id}'"):
        client.service_principals.delete(s.id)


def create_or_update_service_principals(client: AccountClient,
                                        desired_service_principals: List[iam.ServicePrincipal],
                                        dry_run=False):
    total_differences = []

    for desired in desired_service_principals:
        total_differences.extend(
            _generic_create_or_update(
                desired=desired,
                actual_objects=list(
                    client.service_principals.list(filter=f"applicationId eq '{desired.application_id}'")),
                compare_fields=["displayName"],
                sdk_module=client.service_principals,
                dry_run=dry_run))

    return total_differences
