import logging
from typing import Dict, List, Optional, Set
import json
import os

import requests
from azure.identity import DefaultAzureCredential, DeviceCodeCredential
from databricks.sdk.service import iam
from pydantic import AliasChoices, BaseModel, Field
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from copy import deepcopy
from .persisted_cache import Cache

logger = logging.getLogger('sync.graph')


class GraphBase(BaseModel):
    id: str
    display_name: str = Field(validation_alias=AliasChoices('displayName'))
    extra_data: Dict[str, str] = Field(default_factory=lambda: {})


class GraphUser(GraphBase):
    mail: str = Field(validation_alias=AliasChoices('mail', 'mailNickname'))
    active: bool = Field(validation_alias=AliasChoices('accountEnabled'), default=True)

    def to_sdk_user(self):
        return iam.User(user_name=self.mail,
                        display_name=self.display_name,
                        active=self.active,
                        external_id=self.id)


class GraphServicePrincipal(GraphBase):
    application_id: str = Field(validation_alias=AliasChoices('appId'))
    active: bool = Field(validation_alias=AliasChoices('accountEnabled'), default=True)

    def to_sdk_service_principal(self):
        return iam.ServicePrincipal(application_id=self.application_id,
                                    display_name=self.display_name,
                                    active=self.active,
                                    external_id=self.id)


class GraphGroup(GraphBase):
    members: Optional[Dict[str, GraphBase]] = Field(default_factory=lambda: {})

    def to_sdk_group(self):
        return iam.Group(
            display_name=self.display_name,
            external_id=self.id,
            members=[iam.ComplexValue(display=x.display_name, value=x.id) for x in self.members.values()])


class GraphSyncObject(BaseModel):
    users: Optional[Dict[str, GraphUser]] = Field(default_factory=lambda: {})
    service_principals: Optional[Dict[str, GraphServicePrincipal]] = Field(default_factory=lambda: {})
    groups: Optional[Dict[str, GraphGroup]] = Field(default_factory=lambda: {})
    errors: Optional[List] = Field(default_factory=lambda: [])
    deep_sync_group_names: Optional[List[str]] = Field(default_factory=lambda: [])

    def save_to_json_file(self, file_name: str):
        logger.info(f"Saving GraphSyncObject to {file_name}")
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(self.model_dump_json(indent=4))

class GraphAPIClient:

    def __init__(self, tenant_id: str = None, spn_id: str = None, spn_key: str = None):
        self._tenant_id = None

        retry_strategy = Retry(
            total=6,
            backoff_factor=1,
            status_forcelist=[429],
            respect_retry_after_header=True,
            raise_on_status=False, # return original response when retries have been exhausted
        )

        self._session = requests.Session()

        http_adapter = HTTPAdapter(max_retries=retry_strategy,
                                   pool_connections=20,
                                   pool_maxsize=20,
                                   pool_block=True)
        self._session.mount("https://", http_adapter)

        self._token = None
        self._header = None
        self._base_url = None

        self._authenticate()

    def _authenticate(self):
        if os.environ.get('AZURE_CLIENT_ID') == 'DeviceCodeAuth' and os.environ.get('AZURE_CLIENT_SECRET') == 'DeviceCodeAuth':
            logger.info("Using device authentication auth!")
            credential = DeviceCodeCredential()
        else:
            credential = DefaultAzureCredential()
        
        self._token = credential.get_token('https://graph.microsoft.com/.default')
        self._header = {"Authorization": f"Bearer {self._token.token}"}
        self._base_url = "https://graph.microsoft.com/"

    def get_group_by_name(self, name: str) -> dict:
        res = self._session.get(
            f"https://graph.microsoft.com/v1.0/groups?$filter=displayName eq '{name}'&$select=id,displayName",
            headers=self._header)

        res.raise_for_status()

        data = res.json().get("value")

        if data and len(data) == 1:
            return data[0]

        return None

    def get_group_members(self,
                          group_id: str,
                          select="id,displayName,mail,mailNickname,appId,accountEnabled") -> dict:
        members = []
        query = f"{self._base_url}/beta/groups/{group_id}/members?$select={select}"
        
        while query:
            res = self._session.get(query, headers=self._header)
            
            res.raise_for_status()

            j = res.json()
            query = j.get('@odata.nextLink')
            value = j.get("value")
            if value:
                members.extend(value)

        return members
    
    def get_objects_for_sync_incremental(self, delta_link: str, group_names, group_search_depth: int):
        # take list of all groups from cache
        cached_group_names = list(Cache(path='cache_group.json').keys())
        group_names: Set[str] = set(group_names) or set()
        if not cached_group_names:
            raise ValueError("No whitelisted groups to sync found. Run full sync before running incremental sync in order to build list of whitelisted groups")
        
        logger.info(f"Found {len(cached_group_names)} whitelisted group(s) for incremental sync")

        if not delta_link:
            logger.warning("Incremental mode: initial run detected: downloading all whitelisted groups")
            group_names.update(cached_group_names)
            query = f"{self._base_url}/v1.0/groups/delta/?$select=members,id,displayName"
        else:
            logger.info(f"Incremental mode: delta token: d..{delta_link[-16:]}")
            query = delta_link

        while query:
            r = self._session.get(query, headers=self._header)
            r.raise_for_status()
            j = r.json()
            next_link = j.get('@odata.nextLink')
            delta_link = j.get('@odata.deltaLink')
            
            query = next_link

            if not next_link and not delta_link:
                raise RuntimeError("delta_link is empty")

            for g in j.get('value', []):
                name = g.get('displayName')
                if name and (name not in group_names) and (name in cached_group_names):
                    logger.info(f"Incremental mode: group change: {name}")
                    group_names.add(name)

        sync_obj = self.get_objects_for_sync(group_names=group_names, group_search_depth=group_search_depth)
        return delta_link, sync_obj

    def get_objects_for_sync(self, group_names, group_search_depth: int=1):
        sync_data = GraphSyncObject()

        def _register_user(d):
            id = d['id']
            if id not in sync_data.users:
                try:
                    obj = GraphUser.model_validate(d)
                    sync_data.users[id] = obj
                    logger.debug(f"Downloaded GraphUser: {obj}")
                except Exception as e:
                    logger.error(f"Invalid GraphUser: {d}", exc_info=e)
                    raise e

            return sync_data.users[id]

        def _register_service_principal(d):
            id = d['id']
            if id not in sync_data.service_principals:
                try:
                    obj = GraphServicePrincipal.model_validate(d)
                    sync_data.service_principals[id] = obj
                    logger.debug(f"Downloaded GraphServicePrincipal: {obj}")
                except Exception as e:
                    logger.error(f"Invalid GraphServicePrincipal: {d}", exc_info=e)
                    raise e

            return sync_data.service_principals[id]

        def _register_group(d):
            id = d['id']
            if id not in sync_data.groups:
                try:
                    obj = GraphGroup.model_validate(d)
                    sync_data.groups[id] = obj
                    logger.debug(f"Downloaded GraphGroup: {obj}")
                except Exception as e:
                    logger.error(f"Invalid GraphGroup: {d}", exc_info=e)
                    raise e

            return sync_data.groups[id]
        
        deep_sync_groups_xref: Dict[str, Dict] = {}
        visited_group_names: Set[str] = set()

        group_names = set(group_names)

        for depth in range(group_search_depth):
            logger.info(f"Performing group search (depth {depth+1} of {group_search_depth})")

            for group_name in deepcopy(sorted(set(group_names))):
                if group_name in visited_group_names:
                    continue
                
                visited_group_names.add(group_name)

                logger.info(f"Resolving group by name: {group_name}")
                group_info = self.get_group_by_name(group_name)
                if not group_info:
                    logger.warning(f"Group not found, skipping: {group_name}")
                    continue

                group_id = group_info['id']
                deep_sync_groups_xref[group_id] = group_info

                logger.info(f"Downloading members of group_name: {group_name} (id={group_id})")
                group_members = self.get_group_members(group_id)

                _register_group(group_info)

                group = sync_data.groups[group_id]

                for m in group_members:
                    # remove any None values, without that aliases dont work well
                    m = {k: v for k, v in m.items() if v is not None}

                    if m['@odata.type'] == '#microsoft.graph.user':
                        r = _register_user(m)

                    if m['@odata.type'] == '#microsoft.graph.servicePrincipal':
                        r = _register_service_principal(m)

                    if m['@odata.type'] == '#microsoft.graph.group':
                        r = _register_group(m)
                        group_names.add(r.display_name)

                    if isinstance(r, Exception):
                        sync_data.errors.append((m, r))
                    else:
                        group.members[r.id] = r
                        r.extra_data["search_depth"] = depth+1

        
        
        msg = f"Downloaded: errors={len(sync_data.errors)}, groups={len(sync_data.groups)}, users={len(sync_data.users)}, service_principals={len(sync_data.service_principals)}"
        
        if sync_data.errors:
            logger.error(msg)
            raise ValueError(msg)
        else:
            logger.info(msg)

        sync_data.deep_sync_group_names = sorted(list({ 
            v['displayName']
            for _, v in deep_sync_groups_xref.items()
        }))

        logger.debug(f"Effective deep sync group names: {sync_data.deep_sync_group_names}")

        return sync_data