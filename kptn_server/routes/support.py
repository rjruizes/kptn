"""Rendering helpers shared by every router in this package.

There is exactly one way to render an error in this UI, and it lives here so
that the run routes and the inspection routes cannot drift into two. The
decision it encodes is small but load-bearing: **the page shell is the default
and the fragment is the exception.**

Both kinds of request are real. Every form in this UI is a plain
``<form method="post">`` and every walkthrough task is a plain ``<a href>``,
so a browser with JavaScript switched off navigates to these routes directly
and must receive a whole document -- an orphan ``<div>`` served to a
navigation is a page with no stylesheet and no nav. htmx, which the
walkthrough uses to swap one task detail at a time, sets ``HX-Request: true``
and wants only the fragment. Anything that is not that header is treated as a
navigation.
"""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import HTMLResponse

from kptn_server.project import ProjectContext
from kptn_server.run_store import TERMINAL_STATUSES, RunRecord


def is_fragment_request(request: Request) -> bool:
    """True only for an htmx-initiated request.

    The single place this header is interpreted. A route that guessed from
    ``Accept`` or from a query parameter instead would answer a real browser
    navigation with a fragment.
    """
    return request.headers.get("HX-Request", "").lower() == "true"


def error_response(
    request: Request,
    *,
    status_code: int,
    title: str,
    detail: str,
    nav_active: str = "run",
    run_id: str | None = None,
    run: RunRecord | None = None,
) -> HTMLResponse:
    """Render an error as a page, or as a fragment for an htmx request.

    *run_id* names a run to link to; *run* embeds that run's status fragment,
    which is what the conflict response needs -- the run holding the lock is
    the thing the reader has to act on. *nav_active* keeps the nav's current
    marker on the section the reader was in when the error happened.
    """
    template = "_error.html" if is_fragment_request(request) else "error.html"
    return request.app.state.templates.TemplateResponse(
        request,
        template,
        {
            "nav_active": nav_active,
            "error_title": title,
            "error_detail": detail,
            "error_run_id": run_id,
            "run": run,
            "is_terminal": (
                run.status in TERMINAL_STATUSES if run is not None else False
            ),
        },
        status_code=status_code,
    )


__all__ = ["error_response", "is_fragment_request"]


def requested_profile(profile: str | None) -> str | None:
    """Normalize the query parameter: a blank selection means no profile.

    The profile ``<select>`` submits ``profile=`` for its ``(no profile)``
    option, and "no profile" is a legitimate, documented way to inspect a
    pipeline -- the raw graph, unpruned.
    """
    return profile or None


def unknown_profile(request: Request, profile: str, *, nav_active: str) -> HTMLResponse:
    """The one answer for a profile this project does not declare.

    Shared by every page that takes a ``?profile=``, because the profile now
    travels between them in the nav: a renamed profile turns a stale link
    into this error on whichever page the reader lands, and one wording that
    names the real profiles is the difference between a dead end and a fix.
    """
    project: ProjectContext = request.app.state.project
    return error_response(
        request,
        status_code=400,
        title="Unknown profile",
        detail=(
            f"{profile!r} is not a profile of this project. "
            f"Declared profiles: {', '.join(project.profiles) or 'none'}."
        ),
        nav_active=nav_active,
    )
