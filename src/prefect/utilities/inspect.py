import ast
import inspect
import re

from prefect.utilities.importtools import objects_from_script


def export_fn_parameters(fn):
    parameters = inspect.signature(fn).parameters
    params = []
    for param in parameters.values():
        param_type = None
        param_default = None
        param = str(param)
        if param.find(':') >= 0:
            param_split_arr = re.split(r':\s*', str(param))
            param_name = param_split_arr[0]
            if len(param_split_arr) == 2:
                param_value = param_split_arr[1]
                split_arr = re.split(r'\s*=\s*', param_value)
                param_type = split_arr[0]
                if len(split_arr) == 2:
                    param_default = split_arr[1]
        else:
            split_arr = re.split(r'\s*=\s*', param)
            param_name = split_arr[0]
            if len(split_arr) == 2:
                param_default = split_arr[1]
        params.append({
            'name': param_name,
            'type': param_type,
            'default': param_default if param_default is None or param_default != 'None' else None
        })
    return params


def export_tasks(script_path):
    from prefect.tasks import Task
    from prefect.flows import Flow
    export_data = {
        'imports': [],
        'tasks': [],
        'flows': []
    }
    result = objects_from_script(script_path)
    import_lines: list[str] = []
    with open(script_path, 'r') as f:
        parse_result = ast.parse(f.read())
        for bd in parse_result.body:
            import_line = ''
            if isinstance(bd, ast.ImportFrom):
                import_line = f'from {bd.module} import {", ".join(map(lambda n: n.name, bd.names))}'
            elif isinstance(bd, ast.Import):
                import_line = f'import {", ".join(map(lambda n: n.name, bd.names))}'
            if import_line:
                import_lines.append(import_line)
    export_data['imports'] = sorted(import_lines)
    for k, v in dict(result).items():
        if isinstance(v, Flow):
            export_data['flows'].append({
                'name': v.name,
                'fn_name': k,
                'description': v.description,
                'isasync': v.isasync,
                'version': v.version,
                'retry_delay_seconds': v.retry_delay_seconds,
                'retries': v.retries,
                'timeout_seconds': v.timeout_seconds,
                'parameters': export_fn_parameters(v.fn),
                'source': inspect.getsource(v.fn),
                'task_runner': v.task_runner.name
            })
        if isinstance(v, Task):
            export_data['tasks'].append({
                'name': v.name,
                'fn_name': k,
                'description': v.description,
                'isasync': v.isasync,
                'tags': list(v.tags),
                'retries': v.retries,
                'retry_delay_seconds': v.retry_delay_seconds,
                'source': inspect.getsource(v.fn),
                'parameters': export_fn_parameters(v.fn)
            })
    return export_data
