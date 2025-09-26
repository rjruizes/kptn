import os
from pathlib import Path
import typer
from typing import Optional
import json
import yaml
from kapten.codegen.codegen import generate_files
from kapten.read_config import read_config

app = typer.Typer()


@app.command()
def codegen(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Generate Prefect flows (Python files) from the tasks.yaml file
    """
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            generate_files()
        finally:
            os.chdir(original_dir)
    else:
        generate_files()

@app.command()
def serve_docker(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Start a docker API server to allow the UI to trigger building and pushing docker images
    """
    from kapten.dockerbuild.dockerbuild import docker_api_server
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            docker_api_server()
        finally:
            os.chdir(original_dir)
    else:
        docker_api_server()

@app.command()
def watch_files(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    """
    Start a file watcher to monitor for changes in the code and send updates to the UI
    """
    from kapten.filewatcher.filewatcher import start_watching
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            start_watching()
        finally:
            os.chdir(original_dir)
    else:
        start_watching()

@app.command()
def backend(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration")
):
    from kapten.watcher.app import start
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            start()
        finally:
            os.chdir(original_dir)
    else:
        start()

@app.command()
def config(
    project_dir: Optional[Path] = typer.Option(None, "--project-dir", "-p", help="Project directory containing kapten configuration"),
    format: str = typer.Option("yaml", "--format", "-f", help="Output format: yaml, json, or table")
):
    """
    Display the current kapten configuration
    """
    if project_dir:
        original_dir = os.getcwd()
        os.chdir(project_dir)
        try:
            kap_conf = read_config()
        finally:
            os.chdir(original_dir)
    else:
        kap_conf = read_config()
    
    if format.lower() == "json":
        typer.echo(json.dumps(kap_conf, indent=2))
    elif format.lower() == "table":
        typer.echo("=" * 40)
        for key, value in kap_conf.items():
            typer.echo(f"{key:<20}: {value}")
    else:  # default to yaml
        typer.echo(yaml.dump(kap_conf, default_flow_style=False))

if __name__ == "__main__":
    app()