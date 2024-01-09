import json
import logging
import sys

import click
import coloredlogs

from .graph import GraphAPIClient
from .persisted_cache import Cache
from .scim import get_account_client, sync


@click.command()
@click.option('--groups-json-file', help="list of AAD groups to sync (json formatted)", required=False)
@click.option('--verbose',
              default=False,
              is_flag=True,
              help="verbose information about changes",
              show_default=True)
@click.option('--debug', default=False, is_flag=True, help="more verbose, shows API calls", show_default=True)
@click.option(
    '--dry-run-security-principals',
    default=False,
    is_flag=True,
    help="dont make any changes to users, groups or service principals, just display pending changes",
    show_default=True)
@click.option('--dry-run-members',
              default=False,
              is_flag=True,
              help="dont make any changes to group members, just display pending membership changes",
              show_default=True)
@click.option('--worker-threads',
              default=10,
              show_default=True,
              help="number of concurent web requests to perform against SCIM")
@click.option('--save-graph-response-json', required=False, help="saves graph response into json file")
@click.option('--query-graph-only',
              required=False,
              is_flag=True,
              default=False,
              show_default=True,
              help="only downloads information from graph (does not perform SCIM sync)")
@click.option(
    '--group-search-depth',
    default=1,
    show_default=True,
    help="defines nested group recursion search depth, default is to search only groups provided as input")
@click.option(
    '--full-sync',
    default=False,
    is_flag=True,
    show_default=True,
    help="synchronizes all groups defined in `groups-json-file` instead of using graph api change feed")
def sync_cli(groups_json_file, verbose, debug, dry_run_security_principals, dry_run_members, worker_threads,
             save_graph_response_json, query_graph_only, group_search_depth, full_sync):
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

    if groups_json_file:
        logger.debug(f"Opening {groups_json_file}...")
        with open(groups_json_file, 'r', encoding='utf-8') as f:
            aad_groups = json.load(f)

        logger.info(f"Loaded {len(aad_groups)} groups from {groups_json_file}")
    else:
        aad_groups = []

    if full_sync:
        logger.info("Entering full graph query mode...")
        if not aad_groups:
            raise ValueError("no groups provided")
        
        stuff_to_sync = graph_client.get_objects_for_sync(group_names=aad_groups,
                                                          group_search_depth=group_search_depth)
    else:
        logger.info("Entering incremental graph query mode...")
        incremental_token_cache = Cache(path="graph_incremental_token.json")
        delta_link = incremental_token_cache.get('delta_link')
        delta_link, stuff_to_sync = graph_client.get_objects_for_sync_incremental(
            delta_link=delta_link, group_names=aad_groups, group_search_depth=group_search_depth)

    if save_graph_response_json:
        stuff_to_sync.save_to_json_file(save_graph_response_json)

    if query_graph_only:
        logger.info("--query-graph-only is set, terminating")
        return

    sync_results = sync(
        account_client=account_client,
        users=[x.to_sdk_user() for x in stuff_to_sync.users.values()],
        groups=[x.to_sdk_group() for x in stuff_to_sync.groups.values()],
        service_principals=[x.to_sdk_service_principal() for x in stuff_to_sync.service_principals.values()],
        deep_sync_group_names=list(stuff_to_sync.deep_sync_group_names),
        dry_run_security_principals=dry_run_security_principals,
        dry_run_members=dry_run_members,
        worker_threads=worker_threads)

    if not full_sync:
        logger.info(f"Saving graph delta token: ..{delta_link[-32:]}")
        incremental_token_cache['delta_link'] = delta_link
        incremental_token_cache.flush()

    logger.info("Sync finished!")


if __name__ == '__main__':
    sync_cli()
