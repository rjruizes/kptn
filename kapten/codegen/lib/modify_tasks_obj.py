
def build_import_str(name, task):
    """ Set the Python import string for the task """
    qualified_name = ''
    if 'py' in task:
        qualified_name = task['py']['file'].replace('.py', '').replace('/', '.')
    else:
        qualified_name = name
    return f"from {qualified_name} import {task['exports']}"

def modify_tasks_obj(tasks, pipeline_config_lookup):# 
    """ Modify `tasks` to include function calls for Jinja templates """
    # - If a task is mapped, caller flows call it as a subflow
    for name, task in tasks.items():
        wait_for_str = None
        if 'r_script' not in task:
            tasks[name]['r_script'] = f"{name}/run.R"
        if 'deps' in task and task['deps']:
            wait_for_str = 'wait_for=[{}]'.format(', '.join(task['deps']))

        if 'iterable' in task:
            func_name = name + '_flow'
            tasks[name]['exports'] = func_name
            tasks[name]['func_call'] = f"{func_name}(pipeline_config)"
        else:
            func_call = ''
            # If task is a function, call it as a function
            if 'py' in task and 'is_func' in task['py']:
                func_name = name + '_func'
                tasks[name]['exports'] = func_name
                func_call = f"{func_name}("
            # Else it is a Prefect task, submit it
            else:
                func_name = name + '_task'
                tasks[name]['exports'] = func_name
                # func_call = f"{func_name}.submit("
                func_call = f"tscache.submit({func_name}, "

            # Add params if they exist and then close parentheses
            if 'params' in task:
                # task_args = ['pipeline_config']
                task_args = []
                for param in task['params']:
                    if param not in pipeline_config_lookup:
                        task_args.append(f'pipeline_config.{param}')
                param_str = ', '.join([f'{k}' for k in task_args])
                if wait_for_str:
                    if param_str:
                        param_str = param_str + ', ' + wait_for_str
                    else:
                        param_str = wait_for_str

                tasks[name]['func_call'] = func_call + param_str + ')'
            else:
                if wait_for_str:
                    func_call = func_call + wait_for_str
                tasks[name]['func_call'] = func_call + ')'
        tasks[name]['import_str'] = build_import_str(name, task)
    return tasks