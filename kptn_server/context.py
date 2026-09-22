"""What one request knows about the project it is serving.

The app used to answer "which project" from ``app.state``, which is right
when a process serves one project for its lifetime and wrong the moment it
can serve several. This is the same answer, per request.

The split matters more than the move. ``entry`` is cheap metadata -- slug,
root, profiles, display name -- readable without importing anything, and it
is what the app bar and the run history render from. ``project()`` is the
loaded pipeline, and it comes through :class:`~kptn_server.slot.ProjectSlot`,
which allows one at a time. A route that takes the second when the first
would do puts itself behind a lock for no reason; see the module docstring
there for what that costs.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Callable, Optional

from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from kptn_server.processes import RunProcessManager
from kptn_server.project import ProjectContext
from kptn_server.registry import ProjectEntry
from kptn_server.run_store import RunStore
from kptn_server.slot import ProjectSlot


@dataclass(frozen=True)
class RequestUI:
    """Everything a handler needs, resolved once per request."""

    entry: ProjectEntry
    store: RunStore
    processes: RunProcessManager
    slot: ProjectSlot
    base: str
    project_base: str
    templates: Jinja2Templates

    def project(self) -> AbstractContextManager[ProjectContext]:
        """The loaded pipeline, for as long as the ``with`` block lasts.

        Only for handlers that need the graph itself -- the plan, the
        walkthrough, and the lineage surfaces. Everything else works from
        ``entry`` and ``store`` and stays off the lock.
        """
        return self.slot.use(self.entry)


def ui(request: Request) -> RequestUI:
    """The request's :class:`RequestUI`.

    A function rather than an attribute access at every call site, so the
    typing is in one place and so the failure mode of a route mounted without
    the resolver is a clear ``AttributeError`` here.
    """
    return request.state.ui


#: Builds the :class:`RequestUI` for one request, from its ASGI scope.
#:
#: A scope rather than a ``Request`` because the middleware below is raw ASGI
#: (see its docstring). ``scope["app"]`` is the application, which is how a
#: resolver reaches whatever the app was configured with. Returning ``None``
#: means "no project for this request" -- what the multi-project project list
#: will want, and what leaves ``request.state.ui`` unset.
UIResolver = Callable[[Scope], Optional[RequestUI]]


class RequestUIMiddleware:
    """Put a :class:`RequestUI` on every request's state, before any route.

    **Raw ASGI, deliberately, and not** ``BaseHTTPMiddleware``.

    Starlette's ``BaseHTTPMiddleware`` is not a thin wrapper. Each instance
    runs the rest of the application in an ``anyio`` task group and pumps the
    response back through a memory object stream. That machinery has a known
    sharp edge: a ``BaseException`` that is not an ``Exception`` --
    ``KeyboardInterrupt`` above all -- raised by an endpoint escapes the
    inner task without ever completing the stream handshake, and the caller
    waiting on the response never gets one. With a single such middleware in
    the stack the failure happens to unwind; add a second and the request
    wedges forever. ``tests/test_ui_runs.py`` raises exactly that exception
    on purpose, to pin that an interrupted launch releases the project lock,
    and a second ``BaseHTTPMiddleware`` turned that test into a hang with no
    traceback and no timeout.

    This class adds no task group, no stream and no request/response objects.
    It writes one key into ``scope["state"]`` -- the dict that backs
    ``request.state`` -- and calls through. It is also simply cheaper, which
    matters for the SSE stream that every open run console holds.

    ``enforce_same_origin`` stays a ``BaseHTTPMiddleware``: it has to build a
    ``Request`` to read a header and return a rendered response, which is
    what that class is for, and there is only ever one of it.
    """

    def __init__(self, app: ASGIApp, resolver: UIResolver) -> None:
        self.app = app
        self.resolver = resolver

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] in ("http", "websocket"):
            current = self.resolver(scope)
            if current is not None:
                # ``Request.state`` is a view onto ``scope["state"]``, so
                # writing here is what ``request.state.ui = ...`` would do
                # from a middleware that had a ``Request`` to write it on.
                scope.setdefault("state", {})["ui"] = current
        await self.app(scope, receive, send)


__all__ = ["RequestUI", "RequestUIMiddleware", "UIResolver", "ui"]
