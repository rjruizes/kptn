"""A project pipeline module that raises while being imported.

This is the single most common project-authoring mistake: an error in the
module *body* -- a typo, a bad constant, a step defined against a name that
does not exist -- rather than a missing file or a bad import. Such an error is
not an ``ImportError``, so it travels straight through
``importlib.import_module`` and out of ``load_pipeline`` untouched.

``ProjectContext.load`` has to turn it into a ``ProjectError`` anyway, because
that is the contract ``create_app`` documents and the only thing ``kptn ui``
knows how to report cleanly.
"""

from __future__ import annotations

import kptn

MARKER = "broken fixture project"


@kptn.task(outputs=[], description="Never actually defined.")
def only_task(mode: str = "success") -> None:  # pragma: no cover - never runs
    print(mode)


raise ValueError(f"{MARKER}: the pipeline module failed at import time")


pipeline = kptn.Pipeline("broken", only_task)  # pragma: no cover - unreachable
