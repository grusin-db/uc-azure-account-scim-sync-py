import os
import time
import pytest
from typing import List, Dict, Optional
from databricks.sdk import AccountClient
from pydantic import BaseModel, Field
from dataclasses import dataclass

from databricks.sdk.service import iam

class DesiredUser(BaseModel):
    user_name: str
    display_name: str
    external_id: str
    active: bool

    @classmethod
    def get_diff_fields(cls):
        return { 
            'display_name': 'displayName',
            'external_id': 'externalId',
            'active': 'active'
        }

@dataclass
class UserDiff:
    desired: DesiredUser
    actual: iam.User
    operations: List[iam.Patch]

def create_or_update_users(client: AccountClient, desired_users: List[DesiredUser], dry_run=False):
    total_differences = []
    
    for desired_user in desired_users:
        dbr_users = list(client.users.list(filter=f"userName eq '{desired_user.user_name}'"))
        if not len(dbr_users):
            client.users.create(
                user_name=desired_user.user_name
                ,display_name=desired_user.display_name
                ,external_id=desired_user.external_id
            )
        else:
            for u in dbr_users:
                desired_user_dict = desired_user.model_dump()
                u_dict = u.as_dict()

                operations = [
                    iam.Patch(
                        op=iam.PatchOp.ADD,
                        path=scim_field_name,
                        value=desired_user_dict[model_field_name]
                    )
                    for model_field_name, scim_field_name in DesiredUser.get_diff_fields().items()
                    if desired_user_dict[model_field_name] != u_dict.get(scim_field_name)
                ]

                if operations:
                    client.users.patch(
                        u.id
                        ,schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
                        ,operations=operations
                    )

                    total_differences.append(
                        UserDiff(desired=desired_user, actual=u, operations=operations)
                    )

    return total_differences

