import logging
import sys

import pytest
from databricks.sdk import AccountClient

from azure_dbr_scim_sync.graph import GraphAPIClient
from azure_dbr_scim_sync.scim import get_account_client, sync

logging.basicConfig(stream=sys.stderr,
                    level=(logging.DEBUG),
                    format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')

logger = logging.getLogger('sync')


@pytest.fixture()
def graph_client():
    return GraphAPIClient()


@pytest.fixture()
def account_client():
    return get_account_client()


def test_graph_sync_object(graph_client: GraphAPIClient, account_client: AccountClient):
    stuff_to_sync = graph_client.get_objects_for_sync([
        'uc-metastore-playground-admin', 'team02-admin', 'team01-admin', 'team02-eng', 'team01-eng',
        'uc-account-admin'
    ])

    sync_results = sync(
        account_client=account_client,
        users=[x.to_sdk_user() for x in stuff_to_sync.users.values()],
        groups=[x.to_sdk_group() for x in stuff_to_sync.groups.values()],
        deep_sync_group_external_ids=list(stuff_to_sync.deep_sync_groups),
        service_principals=[x.to_sdk_service_principal() for x in stuff_to_sync.service_principals.values()])

    print(sync_results)
