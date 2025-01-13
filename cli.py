import typer
from kapten.codegen.codegen import generate_files
from kapten.dockerbuild.dockerbuild import docker_api_server
from kapten.filewatcher.filewatcher import start_watching

app = typer.Typer()


@app.command()
def codegen():
    """
    Generate Prefect flows (Python files) from the tasks.yaml file
    """
    generate_files()

@app.command()
def serve_docker():
    """
    Start a docker API server to allow the UI to trigger building and pushing docker images
    """
    docker_api_server()

@app.command()
def watch_files():
    """
    Start a file watcher to monitor for changes in the code and send updates to the UI
    """
    start_watching()

@app.command()
def backend():
    from kapten.watcher.app import start
    start()

if __name__ == "__main__":
    app()