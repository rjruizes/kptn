# CLI version selector — change _CLI_VERSION to switch between "v1" and "v2".
# "v1": full _v01.py app (codegen, serve-docker, lineage, etc.)
# "v2": v0.2.0 thin shell with run + plan (commands.py)
_CLI_VERSION = "v2"

if _CLI_VERSION == "v1":
    from kptn.cli._v01 import app  # noqa: F401
    from kptn.cli._v01 import *  # noqa: F401, F403
else:
    from kptn.cli.commands import app  # noqa: F401

# v0.1.x internals re-exported for kptn_server and tests — version-independent.
from kptn.cli._v01 import (  # noqa: F401
    _validate_python_tasks,
    _build_lineage_payload,
    _infer_lineage_dialect,
    _task_order_from_graph,
)
