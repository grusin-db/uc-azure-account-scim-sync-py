import logging
import os
import sys

import requests


class GraphAPIClient:

    def __init__(self, tenant_id: str, spn_id: str, spn_key: str):
        self._tenant_id = tenant_id
        self._token = self._get_access_token(tenant_id, spn_id, spn_key)
        self._header = {"Authorization": f"Bearer {self._token}"}
        self._base_url = "https://graph.microsoft.com/"

    @classmethod
    def _get_access_token(cls, tenant_id, spn_id, spn_key):
        post_data = {
            'client_id': spn_id,
            'scope': 'https://graph.microsoft.com/.default',
            'client_secret': spn_key,
            'grant_type': 'client_credentials'
        }
        initial_header = {'Content-type': 'application/x-www-form-urlencoded'}
        res = requests.post(f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
                            data=post_data,
                            headers=initial_header)
        res.raise_for_status()
        return res.json().get("access_token")

    def get_group_by_name(self, name: str) -> dict:
        res = requests.get(
            f"https://graph.microsoft.com/v1.0/groups?$filter=displayName eq '{name}'&$select=id,displayName",
            headers=self._header)

        res.raise_for_status()

        data = res.json().get("value")

        if data and len(data) == 1:
            return data[0]

        return None

    def get_group_members(
            self,
            group_id: str,
            select="id,displayName,mail,appId,accountEnabled,createdDateTime,deletedDateTime") -> dict:
        res = requests.get(f"{self._base_url}/beta/groups/{group_id}/members?$select={select}",
                           headers=self._header)

        res.raise_for_status()

        return res.json().get("value")

    def _find_app(self):
        res = requests.get(
            f"{self._base_url}/servicePrincipals?$filter=startswith(displayName,'{self._app_name}')&$count=true&$top=1",
            headers=self._header)
        res.raise_for_status()

        value = res.json().get("value")

        if len(value) == 0:
            raise ValueError(f"Failed to find app: {self._app_name}")

        group = value[0]

        return {
            "appId": group.get("appId"),
            "roleId": group.get("appRoles")[0].get("id"),
            "objectId": group.get("id")
        }


if __name__ == "__main__":
    # arg_parser = argparse.ArgumentParser()
    # arg_parser.add_argument("--tenant_id", help="Azure Tenant Id", required=True)
    # arg_parser.add_argument("--spn_id", help="Deployment SPN Id", required=False)
    # arg_parser.add_argument("--spn_key", help="Deployment SPN Secret Key", required=False)
    # arg_parser.add_argument("--groups_json_file_name", help="JSON file containing all groups", required=True)
    # arg_parser.add_argument("--verbose",
    #                         help="Verbose logs",
    #                         default=False,
    #                         required=False,
    #                         action='store_true')
    # args = vars(arg_parser.parse_args())

    tenant_id = os.getenv('ARM_TENANT_ID')
    spn_id = os.getenv('ARM_CLIENT_ID')
    spn_key = os.getenv('ARM_CLIENT_SECRET')

    logging.basicConfig(stream=sys.stderr,
                        level=(logging.DEBUG),
                        format='%(asctime)s %(levelname)s %(threadName)s [%(name)s] %(message)s')

    logger = logging.getLogger('sync')

    logger.info(f"creds {tenant_id=}, {spn_id=}")

    ec = GraphAPIClient(tenant_id=tenant_id, spn_id=spn_id, spn_key=spn_key)

    group_name = 'team02-admin'
    group_info = ec.get_group_by_name(group_name)

    logger.info(f"group by id: {ec.get_group_by_id('bd6f0f0c-e528-47f8-b67e-a3d299852083')}")
    logger.info(f"group by name: {ec.get_group_by_name('team02-admin')}")
