from dataclasses import dataclass
from typing import Generic, Iterable, List, Optional, TypeVar

from databricks.sdk.service import iam

T = TypeVar("T")


@dataclass
class MergeResult(Generic[T]):
    desired: T
    actual: T
    action: str
    changes: Optional[List[iam.Patch]]


def _generic_create_or_update(desired: T, actual_objects: Iterable[T], compare_fields: List[str], sdk_module,
                              dry_run: bool) -> List[T]:
    total_changes = []
    DiffClass = MergeResult[T]

    desired_dict = desired.as_dict()
    if not len(actual_objects):
        total_changes.append(DiffClass(desired=desired, actual=None, action="new", changes=None))
        if not dry_run:
            sdk_module.create(**desired.__dict__)
    else:
        for actual in actual_objects:
            actual_dict = actual.as_dict()

            operations = [
                iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_dict[field_name])
                for field_name in compare_fields if desired_dict[field_name] != actual_dict.get(field_name)
            ]

            total_changes.append(
                DiffClass(desired=desired,
                          actual=actual,
                          action="change" if operations else "no change",
                          changes=operations))

            if operations:
                if not dry_run:
                    sdk_module.patch(actual.id,
                                     schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                                     operations=operations)

    return total_changes


from .groups import create_or_update_groups, delete_group_if_exists  # NOQA
from .service_principals import create_or_update_service_principals  # NOQA
from .service_principals import delete_service_principal_if_exists  # NOQA
from .users import create_or_update_users, delete_user_if_exists  # NOQA

__all__ = [
    'create_or_update_users', 'create_or_update_groups', 'delete_user_if_exists', 'delete_group_if_exists',
    'create_or_update_service_principals', 'delete_service_principal_if_exists'
]
