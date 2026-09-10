"""``python -m kptn`` entry point.

The console script installed as ``kptn`` is not always on ``PATH`` -- the VS
Code extension, for instance, knows the user's selected interpreter and
nothing else, so it invokes ``python -m kptn ui`` with that interpreter. This
module makes the package's CLI reachable that way.
"""

from kptn.cli import app

if __name__ == "__main__":
    app()
