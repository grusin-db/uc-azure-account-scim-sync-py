import json
import logging
import sys

import pytest

from azure_dbr_scim_sync.graph import GraphAPIClient

logging.basicConfig(stream=sys.stderr,
                    level=(logging.DEBUG),
                    format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')

logger = logging.getLogger('sync')


@pytest.fixture()
def graph_client():
    return GraphAPIClient()


def test_me(graph_client: GraphAPIClient):
    assert graph_client.get_me() is not None


def test_get_group_members(graph_client: GraphAPIClient):
    group_name = 'team02-admin'
    group_info = graph_client.get_group_by_name(group_name)

    assert group_info
    assert isinstance(group_info, dict)

    logging.info(f"group: {group_name}: {json.dumps(group_info, indent=4)}")

    group_members = graph_client.get_group_members(group_info['id'])
    assert group_members
    assert isinstance(group_members, list)
    logging.info(f"members: {json.dumps(group_members, indent=4)}")


def test_non_existing_group(graph_client: GraphAPIClient):
    group_info = graph_client.get_group_by_name("bla-bla-does-not-exist")
    assert group_info is None
