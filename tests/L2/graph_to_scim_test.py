import logging
import os
import sys

import pytest
from databricks.sdk import AccountClient

from azure_dbr_scim_sync.graph import GraphAPIClient
from azure_dbr_scim_sync.scim import (create_or_update_groups,
                                      create_or_update_service_principals,
                                      create_or_update_users)

logging.basicConfig(stream=sys.stderr,
                    level=(logging.DEBUG),
                    format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')

logger = logging.getLogger('sync')


@pytest.fixture()
def graph_client():
    tenant_id = os.getenv('ARM_TENANT_ID')
    assert tenant_id

    spn_id = os.getenv('ARM_CLIENT_ID')
    assert spn_id

    spn_key = os.getenv('ARM_CLIENT_SECRET')
    assert spn_key

    return GraphAPIClient(tenant_id=tenant_id, spn_id=spn_id, spn_key=spn_key)


@pytest.fixture()
def account_client():
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID")
    assert account_id

    host = os.getenv("DATABRICKS_HOST")
    assert host

    return AccountClient(host=host, account_id=account_id)


def test_graph_sync_object(graph_client: GraphAPIClient, account_client: AccountClient):
    stuff_to_sync = graph_client.get_objects_for_sync([
        'team02-admin', 'team01-admin', 'team02-eng', 'team01-eng', 'uc-metastore-playground-admin',
        'uc-account-admin'
    ])

    create_or_update_users(account_client, [x.to_sdk_user() for x in stuff_to_sync.users.values()])
    create_or_update_groups(account_client, [x.to_sdk_group() for x in stuff_to_sync.groups.values()])
    create_or_update_service_principals(
        account_client, [x.to_sdk_service_principal() for x in stuff_to_sync.service_principals.values()])
