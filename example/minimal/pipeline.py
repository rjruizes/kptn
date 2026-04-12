import kptn
from pathlib import Path


def get_greeting() -> str:
    return "Hello, kptn!"


@kptn.task(outputs=["output/extract.txt"])
def extract(greeting: str = "Hello") -> None:
    Path("output").mkdir(exist_ok=True)
    Path("output/extract.txt").write_text(greeting)


@kptn.task(outputs=["output/transform.txt"])
def transform(greeting: str = "Hello") -> None:
    data = Path("output/extract.txt").read_text()
    Path("output/transform.txt").write_text(data.upper())


@kptn.task(outputs=["output/load.txt"])
def load(greeting: str = "Hello") -> None:
    data = Path("output/transform.txt").read_text()
    Path("output/load.txt").write_text(f"Loaded: {data}")


deps = kptn.config(greeting=get_greeting)
graph = deps >> extract >> transform >> load
pipeline = kptn.Pipeline("hello_kptn", graph)

if __name__ == "__main__":
    kptn.run(pipeline)
