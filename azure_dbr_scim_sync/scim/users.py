from dataclasses import dataclass
from typing import List, Optional

from databricks.sdk import AccountClient
from databricks.sdk.service import iam


@dataclass
class UserDiff:
    desired: iam.User
    actual: iam.User
    action: str
    changes: Optional[List[iam.Patch]]


def create_or_update_users(client: AccountClient, desired_users: List[iam.User], dry_run=False):
    total_differences: List[UserDiff] = []

    for desired_user in desired_users:
        dbr_users = list(client.users.list(filter=f"userName eq '{desired_user.user_name}'"))
        if not len(dbr_users):
            total_differences.append(UserDiff(desired=desired_user, actual=None, action="new", changes=None))
            if not dry_run:
                client.users.create(user_name=desired_user.user_name,
                                    display_name=desired_user.display_name,
                                    external_id=desired_user.external_id)
        else:
            for u in dbr_users:
                desired_user_dict = desired_user.as_dict()
                u_dict = u.as_dict()

                operations = [
                    iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_user_dict[field_name])
                    for field_name in ["displayName"]
                    if desired_user_dict[field_name] != u_dict.get(field_name)
                ]

                if operations:
                    total_differences.append(
                        UserDiff(desired=desired_user, actual=u, action="change", changes=operations))

                    if not dry_run:
                        client.users.patch(
                            u.id,
                            schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                            operations=operations)

    return total_differences
