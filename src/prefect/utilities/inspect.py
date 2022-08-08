import ast
import inspect
import re
import types
import tokenize

from prefect.utilities.callables import parameter_schema

from prefect.utilities.importtools import objects_from_script


def export_fn_parameters(fn):
    param_schema = parameter_schema(fn)
    properties = []
    for pk, pv in param_schema.properties.items():
        property_type = pv.get('type', None)
        property_items = pv.get('items', {})
        property_sub_type = None
        allOf = pv.get('allOf', [])
        if allOf:
            property_type = '|'.join([af.get('$ref', '').split('/')[-1] for af in allOf])
        if property_items:
            if '$ref' in property_items:
                property_sub_type = property_items.get('$ref').split('/')[-1]
            else:
                property_sub_type = property_items.get('type')
        properties.append({
            'name': pk,
            'default': pv.get('default', None),
            'type': property_type,
            'subType': property_sub_type
        })
    params = {
        'required': param_schema.required if param_schema.required is not None else [],
        'definitions': param_schema.definitions,
        'properties': properties
    }
    return params


def export_tasks(script_path):
    from prefect.tasks import Task
    from prefect.flows import Flow

    export_data = {
        "tasks": [],
        "flows": [],
        'source': ''
    }
    result = objects_from_script(script_path)
    import_lines: list[str] = []
    import_names: list[str] = []
    with open(script_path, "r") as f:
        file_content = f.read()
    parse_result = ast.parse(file_content, type_comments=True)
    for bd in parse_result.body:
        import_line = ""
        if isinstance(bd, ast.ImportFrom):
            names = list(map(lambda n: n.name, bd.names))
            import_names += names
            import_line = f'from {bd.module} import {", ".join(names)}'
        elif isinstance(bd, ast.Import):
            names = list(map(lambda n: n.name, bd.names))
            import_names += names
            import_line = f'import {", ".join(names)}'
        if import_line:
            import_lines.append(import_line)
    unique_import_names = set(import_names)
    for k, v in dict(result).items():
        if isinstance(v, Flow):
            flow_source = inspect.getsource(v.fn).strip()
            flow_idx = file_content.find(flow_source)
            file_content = file_content[:flow_idx] + file_content[flow_idx + len(flow_source) + 1:]
            export_data["flows"].append(
                {
                    "name": v.name,
                    "fn_name": k,
                    "description": v.description,
                    "isasync": v.isasync,
                    "version": v.version,
                    "retry_delay_seconds": v.retry_delay_seconds,
                    "retries": v.retries,
                    "timeout_seconds": v.timeout_seconds,
                    "parameters": export_fn_parameters(v.fn),
                    "source": flow_source,
                    "task_runner": v.task_runner.name,
                }
            )
        elif k not in unique_import_names and isinstance(v, Task):
            task_source = inspect.getsource(v.fn).strip()
            task_idx = file_content.find(task_source)
            file_content = file_content[:task_idx] + file_content[task_idx + len(task_source) + 1:]
            export_data["tasks"].append(
                {
                    "name": v.name,
                    "fn_name": k,
                    "description": v.description,
                    "isasync": v.isasync,
                    "tags": list(v.tags),
                    "retries": v.retries,
                    "retry_delay_seconds": v.retry_delay_seconds,
                    "source": task_source,
                    "parameters": export_fn_parameters(v.fn),
                }
            )
    export_data['source'] = file_content
    return export_data
