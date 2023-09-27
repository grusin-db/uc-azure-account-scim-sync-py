import json
import logging
import os
import sys

import pytest

from azure_dbr_scim_sync.graph import GraphAPIClient

logging.basicConfig(stream=sys.stderr,
                    level=(logging.DEBUG),
                    format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')

logger = logging.getLogger('sync')


@pytest.fixture()
def client():
    tenant_id = os.getenv('ARM_TENANT_ID')
    assert tenant_id

    spn_id = os.getenv('ARM_CLIENT_ID')
    assert spn_id

    spn_key = os.getenv('ARM_CLIENT_SECRET')
    assert spn_key

    return GraphAPIClient(tenant_id=tenant_id, spn_id=spn_id, spn_key=spn_key)


def test_get_group_members(client: GraphAPIClient):
    group_name = 'team02-admin'
    group_info = client.get_group_by_name(group_name)

    assert group_info
    assert isinstance(group_info, dict)

    logging.info(f"group: {group_name}: {json.dumps(group_info, indent=4)}")

    group_members = client.get_group_members(group_info['id'])
    assert group_members
    assert isinstance(group_members, list)
    logging.info(f"members: {json.dumps(group_members, indent=4)}")


def test_non_existing_group(client: GraphAPIClient):
    group_info = client.get_group_by_name("bla-bla-does-not-exist")
    assert group_info is None


