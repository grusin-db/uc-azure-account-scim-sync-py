from typing import Dict, Optional, List

import requests
from pydantic import AliasChoices, BaseModel, Field


class GraphBase(BaseModel):
    id: str = Field()
    display_name: str = Field(validation_alias=AliasChoices('displayName'))
    created_ts: Optional[str] = Field(validation_alias=AliasChoices('createdDateTime'), default=None)
    deleted_ts: Optional[str] = Field(validation_alias=AliasChoices('deletedDateTime'), default=None)


class GraphUser(GraphBase):
    user_name: str = Field(validation_alias=AliasChoices('mail'))
    active: bool = Field(validation_alias=AliasChoices('accountEnabled'))


class GraphServicePrincipal(GraphBase):
    application_id: str = Field(validation_alias=AliasChoices('appId'))


class GraphGroup(GraphBase):
    members: Optional[Dict[str, GraphBase]] = Field(default_factory=lambda: {})

class GraphSyncObject(BaseModel):
    users: Optional[Dict[str, GraphUser]] = Field(default_factory=lambda: {})
    service_principals: Optional[Dict[str, GraphServicePrincipal]] = Field(default_factory=lambda: {})
    groups: Optional[Dict[str, GraphGroup]] = Field(default_factory=lambda: {})
    errors: Optional[List] = Field(default_factory=lambda: [])


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

    def get_objects_for_sync(self, group_names):
        sync_data = GraphSyncObject()

        def _register_user(d):
            id = d['id']
            if id not in sync_data.users:
                try:
                    sync_data.users[id] = GraphUser(**d)
                except Exception as e:
                    return e

            return sync_data.users[id]

        def _register_service_principal(d):
            id = d['id']
            if id not in sync_data.service_principals:
                try:
                    sync_data.service_principals[id] = GraphServicePrincipal(**d)
                except Exception as e:
                    return e

            return sync_data.service_principals[id]

        def _register_group(d):
            id = d['id']
            if id not in sync_data.groups:
                try:
                    sync_data.groups[id] = GraphGroup(**d)
                except Exception as e:
                    return e

            return sync_data.groups[id]

        for group_name in group_names:
            group_info = self.get_group_by_name(group_name)
            group_members = self.get_group_members(group_info['id'])

            _register_group(group_info)

            group = sync_data.groups[group_info['id']]

            for m in group_members:
                if m['@odata.type'] == '#microsoft.graph.user':
                    r = _register_user(m)

                if m['@odata.type'] == '#microsoft.graph.servicePrincipal':
                    r = _register_service_principal(m)

                if m['@odata.type'] == '#microsoft.graph.group':
                    r = _register_group(m)

                if isinstance(r, Exception):
                    sync_data.errors.append((m, r))
                else:
                    group.members[r.id] = r

        return sync_data
