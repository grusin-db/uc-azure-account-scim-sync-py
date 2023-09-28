from dataclasses import dataclass
from typing import Generic, Iterable, List, Optional, TypeVar

from databricks.sdk.service import iam
from copy import deepcopy

T = TypeVar("T")

@dataclass
class MergeResult(Generic[T]):
    desired: T
    actual: T
    created: T
    action: str
    changes: Optional[List[iam.Patch]]


    @property
    def external_id(self) -> str:
        return self.desired.external_id

    @property
    def effective(self) -> T:
        return self.created or self.actual

    @property
    def id(self) -> str:
        return self.effective.id


def _generic_create_or_update(desired: T, actual_objects: Iterable[T], compare_fields: List[str], sdk_module,
                              dry_run: bool) -> List[T]:
    total_changes = []
    ResultClass = MergeResult[T]

    desired_dict = desired.as_dict()
    if not len(actual_objects):
        created = None

        if not dry_run:
            created: T = sdk_module.create(**desired.__dict__)
            assert created
            assert created.id

        total_changes.append(
            ResultClass(desired=desired, actual=None, action="new", changes=None, created=created))
    else:
        for actual in actual_objects:
            actual_dict = actual.as_dict()

            operations = [
                iam.Patch(op=iam.PatchOp.REPLACE, path=field_name, value=desired_dict[field_name])
                for field_name in compare_fields if desired_dict[field_name] != actual_dict.get(field_name)
            ]

            total_changes.append(
                ResultClass(desired=desired,
                            actual=actual,
                            action="change" if operations else "no change",
                            changes=operations,
                            created=None))

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
