import json
import logging
import sys
import coloredlogs

import click

from azure_dbr_scim_sync.graph import GraphAPIClient
from azure_dbr_scim_sync.scim import get_account_client, sync as scim_sync

@click.command()
@click.option('--groups-json-file', help="list of AAD groups to sync (json formatted)", required=True)
@click.option('--verbose', default=False, is_flag=True, help="verbose information about changes", show_default=True)
@click.option('--debug', default=False, is_flag=True, help="show API call", show_default=True)
@click.option('--dry-run',
              default=False,
              is_flag=True,
              help="dont make any changes, just display",
              show_default=True)
def sync(groups_json_file, verbose, debug, dry_run):
    logging.basicConfig(stream=sys.stderr,
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')
    logger = logging.getLogger('sync')

    # colored logs sets all loggers to this level
    coloredlogs.install(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG if debug else logging.INFO)

    if verbose:
        logger.setLevel(logging.DEBUG)


    graph_client = GraphAPIClient()
    account_client = get_account_client()

    logger.debug(f"Opening {groups_json_file}...")
    with open(groups_json_file, 'r', encoding='utf-8') as f:
        aad_groups = json.load(f)

    logger.info(f"Loaded {len(aad_groups)} groups from {groups_json_file}")
    stuff_to_sync = graph_client.get_objects_for_sync(aad_groups)

    sync_results = scim_sync(
        account_client=account_client,
        users=[x.to_sdk_user() for x in stuff_to_sync.users.values()],
        groups=[x.to_sdk_group() for x in stuff_to_sync.groups.values()],
        service_principals=[x.to_sdk_service_principal() for x in stuff_to_sync.service_principals.values()],
        dry_run=dry_run)
    
    logger.info("Sync finished!")

if __name__ == '__main__':
    sync()