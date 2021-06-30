from typing import TYPE_CHECKING, Type, List, Dict, cast
import json

if TYPE_CHECKING:
    from django.db import models


def get_model_dependencies(
    model: "Type[models.Model]", all_models: "List[str]"
) -> "Dict":
    dependencies = []

    for field in model._meta.fields:
        if field.is_relation:
            relation_spec = get_model_dependencies(
                model=field.remote_field.model, all_models=all_models
            )
            dependencies.append(relation_spec)

    full_label = (
        cast("str", model._meta.app_label) + "." + cast("str", model._meta.model_name)
    )
    if full_label not in all_models:
        all_models.append(full_label)

    spec = {
        "model": full_label,
        "dependencies": dependencies,
    }

    return spec


def print_model_dependencies(model: "Type[models.Model]"): # pragma: no cover
    all_models: "List[str]" = []
    spec = get_model_dependencies(model=model, all_models=all_models)
    result = json.dumps(spec, indent=4)
    print("Models dependency tree:")
    print(result)
    print()
    print("All models:", ", ".join(['"' + model + '"' for model in all_models]))
