
from databricks.sdk.service import iam
from typing import List, Iterable, TypeVar, Generic, Optional
from dataclasses import dataclass

T = TypeVar("T")

@dataclass
class GenericDiffClass(Generic[T]):
    desired: T
    actual: T
    action: str
    changes: Optional[List[iam.Patch]]

def generic_create_or_update(desired: T, actual_objects: Iterable[T], compare_fields: List[str], sdk_module, dry_run:bool) -> List[T]:
    total_changes = []
    DiffClass = GenericDiffClass[T]
    
    desired_dict = desired.as_dict()
    if not len(actual_objects):
        total_changes.append(DiffClass(desired=desired, actual=None, action="new", changes=None))
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
                total_changes.append(DiffClass(desired=desired, actual=actual, action="change", changes=operations))

                if not dry_run:
                    sdk_module.patch(
                        actual.id,
                        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
                        operations=operations)
                    
    return total_changes