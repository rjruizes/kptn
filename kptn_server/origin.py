"""Cross-origin defence for every state-changing request in this UI.

This server has no authentication, by design: it binds loopback and serves
one developer their own project. What that leaves exposed is not *reading* --
it is that ``POST /runs`` takes ``application/x-www-form-urlencoded``, which
makes it a **simple** request in CORS terms. No preflight, no ``Access-
Control-Allow-Origin`` to fail: any page a developer happens to be browsing
while ``kptn ui`` is running can point a hidden form at
``http://127.0.0.1:8000/runs``, submit it, and execute their pipeline. The
attacker never sees the response and does not need to: the run is the effect.

``Sec-Fetch-Site`` is the header that closes it. Browsers set it on every
request and script cannot forge it, so the server can tell a form its own
page submitted from a form some other origin submitted:

``same-origin``
    from this application's own pages. Allowed.
``none``
    user-initiated with no initiator page -- a typed URL, a bookmark, and
    what a non-browser client (``curl``, jupyter-server-proxy's own health
    check, a test) sends by sending nothing at all. Allowed: an absent
    header cannot be a cross-site *browser* request, and refusing it would
    break every non-browser caller for no gain.
``same-site`` / ``cross-site``
    another origin drove this. Refused.

The check lives in middleware rather than in each route on purpose. A
per-route decorator is a thing a new route can forget to wear, and the one it
forgets is the one that matters.

Safe methods are never refused. This is not a general CSRF token scheme and
does not pretend to be one -- it is the specific, sufficient defence for a
loopback server whose danger is a drive-by form post.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response

#: Methods that cannot change state, and are therefore never refused here.
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

#: ``Sec-Fetch-Site`` values that mean "this application, or no page at all".
ALLOWED_FETCH_SITES = frozenset({"same-origin", "none"})

REFUSAL_TITLE = "This request came from another site"
REFUSAL_DETAIL = (
    "kptn refuses a state-changing request that a page on another origin "
    "initiated. This UI can start and stop pipeline runs, and it has no "
    "authentication because it binds loopback -- so a form posted from any "
    "site you happen to have open must not be able to run your pipeline. "
    "Use the UI's own pages."
)


def is_cross_site(method: str, fetch_site: str | None) -> bool:
    """True when this request must be refused as cross-origin.

    Pure, and separately testable: the middleware below is a thin wrapper so
    that the decision has one definition and one place to read it.
    """
    if method.upper() in SAFE_METHODS:
        return False
    if not fetch_site:
        return False
    return fetch_site.strip().lower() not in ALLOWED_FETCH_SITES


async def enforce_same_origin(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Refuse a cross-origin state-changing request, before any route sees it."""
    if is_cross_site(request.method, request.headers.get("sec-fetch-site")):
        # Imported here rather than at module scope: ``routes.support`` pulls
        # in the run store, and this module is imported by the app factory
        # before the routers are.
        from kptn_server.routes.support import error_response  # noqa: PLC0415

        return error_response(
            request,
            status_code=403,
            title=REFUSAL_TITLE,
            detail=REFUSAL_DETAIL,
        )
    return await call_next(request)


__all__ = [
    "ALLOWED_FETCH_SITES",
    "REFUSAL_DETAIL",
    "REFUSAL_TITLE",
    "SAFE_METHODS",
    "enforce_same_origin",
    "is_cross_site",
]
