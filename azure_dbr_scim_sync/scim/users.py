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
    DiffClass = UserDiff
    total_differences: List[DiffClass] = []

    for desired in desired_users:
        desired_dict = desired.as_dict()
        sdk_module = client.users
        actual_objects = list(sdk_module.list(filter=f"userName eq '{desired.user_name}'"))
        compare_fields = ["displayName"]
        

        if not len(actual_objects):
            total_differences.append(DiffClass(desired=desired, actual=None, action="new", changes=None))
            if not dry_run:
                sdk_module.create(**desired_dict)
        else:
            for actual in actual_objects:
                actual_dict = actual.as_dict()

                operations = [
                    iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_dict[field_name])
                    for field_name in compare_fields
                    if desired_dict[field_name] != actual_dict.get(field_name)
                ]

                if operations:
                    total_differences.append(
                        DiffClass(desired=desired, actual=actual, action="change", changes=operations))

                    if not dry_run:
                        sdk_module.patch(
                            actual.id,
                            schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                            operations=operations)

    return total_differences
