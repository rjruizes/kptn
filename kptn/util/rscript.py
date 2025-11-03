import os
from pathlib import Path
import subprocess
from string import Template
import sys
from kptn.util.pipeline_config import PipelineConfig, get_scratch_dir
from kptn.util.filepaths import project_root


def write_command_output_to_file(command, output_filename, cwd, env={"PATH": os.getenv("PATH", "")}):
    """
    Execute a command and write its output to a file. Raise an exception if the command fails.
    """

    with open(output_filename, "w") as output_file:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            env=env,
            text=True,
        )
        # Capture 2 lines of error (if R traceback is enabled) for Exception message (surfaced in python/Prefect)
        err_msg = ""
        next_line = False
        for line in process.stdout:
            # sys.stdout.write(line)
            output_file.write(line)
            if line.startswith("Error in "):
                err_msg = line
                next_line = True
            elif next_line:
                err_msg += f"\n{line}"
                next_line = False
        process.wait()

        if process.returncode != 0:
            raise Exception(f"Rscript {err_msg}" if err_msg else f"Rscript failed with return code {process.returncode}")

def r_script_log_path(task_name, pipeline_config: PipelineConfig, key=None, custom_log_path=None) -> tuple[Path, Path]:
    """
    Returns the log file path for an R script and its path relative to the scratch directory (for the UI)
    If key is provided, the log file will be named {task_name}_{key}.log
    Otherwise, the log file will be named {task_name}.log
    """
    base_dir = Path("log") / task_name
    if custom_log_path:
        custom_log_path = Path(custom_log_path)
        relative_log_filepath = base_dir / custom_log_path
    else:
        log_filename = f"{task_name}_{key}.log" if key else f"{task_name}.log"
        relative_log_filepath = base_dir / log_filename
    log_filepath = Path(pipeline_config.scratch_dir) / relative_log_filepath
    log_filepath.parent.mkdir(exist_ok=True, parents=True)
    return log_filepath, relative_log_filepath

def r_script(task_name, key, pipeline_config: PipelineConfig, script: str, task_env={}, prefix_args_str=None, cli_args=None, custom_log_path=None):
    """Run Rscript"""
    default_env = {
        "PATH": os.getenv("PATH", ""),
        "OUTPUT_PIPELINE_DIR": pipeline_config.scratch_dir,
        "INPUT_PIPELINE_DIR": pipeline_config.scratch_dir,
        "EXTERNAL_FILE_PATH": os.path.join(pipeline_config.scratch_dir, "externals"),
        "DATA_YEAR": pipeline_config.DATA_YEAR,
        "PGHOST": os.getenv("PGHOST", ""),
        "PGPORT": os.getenv("PGPORT", ""),
        "PGUSER": os.getenv("PGUSER", ""),
        "PGPASSWORD": os.getenv("PGPASSWORD", ""),
        "PGDATABASE": os.getenv("PGDATABASE", ""),
        "HOME": os.getenv("HOME", ""),
        "LD_PRELOAD": "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
    }
    # Convert task_env values to strings
    task_env = {k: str(v) for k, v in task_env.items()}

    # Substitute task_env values into script filepath if needed
    if "$" in script:
        script_template = Template(script)
        script = script_template.substitute(task_env)

    args_str = cli_args if cli_args else ""
    if "$" in args_str:
        args_template = Template(args_str)
        args_str = args_template.substitute(task_env)

    if custom_log_path and "$" in custom_log_path:
        custom_log_path_template = Template(custom_log_path)
        custom_log_path = custom_log_path_template.substitute(task_env)

    env = {**default_env, **task_env}
    script_dir = os.path.dirname(script)
    log_filepath, rel_log_path = r_script_log_path(task_name, pipeline_config, key, custom_log_path)

    if prefix_args_str:
        prefix_args = prefix_args_str.split(" ")

    message = "[Rscript] command:" + " ".join(
        [prefix_args_str, "Rscript", script, args_str]
    )
    print(message)
    full_cmd = [*prefix_args, "Rscript", script, args_str] if prefix_args_str else ["Rscript", script, args_str]

    public_endpoint = os.getenv('AWS_EC2_EIP')
    if public_endpoint:
        scratch_dir = get_scratch_dir(pipeline_config)
        rel_log_path = f"http://{public_endpoint}/efs/{scratch_dir}/{rel_log_path}"
    print(f"R script log: {rel_log_path}")

    write_command_output_to_file(full_cmd, log_filepath, script_dir, env)
