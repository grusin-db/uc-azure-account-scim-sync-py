
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
