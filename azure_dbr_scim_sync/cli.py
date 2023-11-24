import json
import logging
import sys

import click
import coloredlogs

from azure_dbr_scim_sync.graph import GraphAPIClient
from azure_dbr_scim_sync.scim import get_account_client, sync


@click.command()
@click.option('--groups-json-file', help="list of AAD groups to sync (json formatted)", required=True)
@click.option('--verbose',
              default=False,
              is_flag=True,
              help="verbose information about changes",
              show_default=True)
@click.option('--debug', default=False, is_flag=True, help="show API call", show_default=True)
@click.option('--dry-run-security-principals',
              default=False,
              is_flag=True,
              help="dont make any changes to users, groups or service principals, just display changes",
              show_default=True)
@click.option('--dry-run-members',
              default=False,
              is_flag=True,
              help="dont make any changes to group members, just display changes",
              show_default=True)
@click.option('--worker-threads', default=10, show_default=True)
@click.option('--save-graph-response-json', required=False)
def sync_cli(groups_json_file, verbose, debug, dry_run_security_principals, dry_run_members, worker_threads, save_graph_response_json):
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')
    logger = logging.getLogger('sync')

    # colored logs sets all loggers to this level
    coloredlogs.install(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG if debug else logging.INFO)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(
        logging.DEBUG if debug else logging.WARNING)

    if verbose:
        logger.setLevel(logging.DEBUG)

    graph_client = GraphAPIClient()
    account_client = get_account_client()

    logger.debug(f"Opening {groups_json_file}...")
    with open(groups_json_file, 'r', encoding='utf-8') as f:
        aad_groups = json.load(f)

    logger.info(f"Loaded {len(aad_groups)} groups from {groups_json_file}")
    stuff_to_sync = graph_client.get_objects_for_sync(aad_groups)

    if save_graph_response_json:
        stuff_to_sync.save_to_json_file(save_graph_response_json)

    sync_results = sync(
        account_client=account_client,
        users=[x.to_sdk_user() for x in stuff_to_sync.users.values()],
        groups=[x.to_sdk_group() for x in stuff_to_sync.groups.values()],
        service_principals=[x.to_sdk_service_principal() for x in stuff_to_sync.service_principals.values()],
        deep_sync_group_external_ids=list(stuff_to_sync.deep_sync_groups),
        dry_run_security_principals=dry_run_security_principals,
        dry_run_members=dry_run_members,
        worker_threads=worker_threads)

    logger.info("Sync finished!")


if __name__ == '__main__':
    sync_cli()
