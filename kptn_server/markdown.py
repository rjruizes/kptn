"""Project-confined Markdown rendering for the walkthrough's docs panel.

A task's *short* documentation is code metadata: a ``description``, declared
``inputs``, declared ``outputs``. Its *long* documentation is a Markdown file
in the project, referenced by a ``docs="path/to/file.md#anchor"`` string on
the task, ``Stage`` or ``Pipeline``. This module turns that reference into
HTML, and it is the only place in the UI that opens a file whose path a
project author chose.

Three rules, in the order they matter.

**Confinement.** The path is resolved and then checked with
``Path.is_relative_to(project_root)`` -- after ``resolve()``, so a symlink
inside the project pointing outside it is caught along with the obvious
``../``. The check is shared with :func:`kptn.inspection.resolve_docs_path`
rather than reimplemented, so the read model and the renderer can never
disagree about what "inside the project" means. This is a second gate, not the
only one: ``inspect_pipeline`` has already rejected an escaping reference by
the time a route gets here, and both gates are tested.

**Raw HTML is text.** The renderer is built with ``{"html": False}``, so a
``<script>`` in a docs file arrives as visible characters. A docs file is
project content, and project content is never trusted markup.

**Markup only for our own output.** :class:`markupsafe.Markup` is applied to
exactly one string in this module -- what the renderer returned -- and never
to a path, a message, an anchor, or anything else a caller passes in. Those
travel as plain strings and are escaped by Jinja like every other
project-authored value in this UI.

Read-only throughout: nothing here writes, creates, or edits documentation.
"""

from __future__ import annotations

from pathlib import Path

from markdown_it import MarkdownIt
from markupsafe import Markup

from kptn.exceptions import KptnError
from kptn.inspection import resolve_docs_path


class DocumentationError(KptnError):
    """A ``docs`` reference that cannot be rendered.

    Raised for a reference that escapes the project root, names a file that
    does not exist, names something that is not a file, or holds bytes that
    are not UTF-8. The message is written for the project author, because they
    are the only person who can fix any of those, and the task detail route
    renders it visibly rather than leaving a blank panel -- a silent empty
    docs panel is indistinguishable from a task with no documentation.
    """


def split_docs_ref(docs_ref: str) -> tuple[str, str | None]:
    """Split ``"path/to/file.md#anchor"`` into its path and its anchor.

    Split once, on the first ``#``: a fragment cannot contain another ``#``,
    and a filename that does is not a path this UI will open.
    """
    path_part, separator, anchor = docs_ref.partition("#")
    return path_part, (anchor if separator else None)


def _renderer() -> MarkdownIt:
    """The one renderer construction in the UI.

    ``commonmark`` is the strict preset. ``html: False`` is the security
    setting -- it makes raw HTML in the source escape rather than pass
    through. ``linkify: True`` turns bare URLs into links *textually*; it
    fetches nothing, so a docs file cannot make the server reach the network
    while a page renders.
    """
    return MarkdownIt("commonmark", {"html": False, "linkify": True})


def render_project_markdown(project_root: Path, docs_ref: str) -> Markup:
    """Render the project Markdown file *docs_ref* names.

    The anchor is parsed off and ignored here: the whole file is rendered and
    the reader lands on the section themselves. Honouring an anchor would mean
    slicing the document, and a wrong slice hides documentation without saying
    so.

    Raises:
        DocumentationError: for an empty reference, a path outside
            *project_root*, a missing path, a non-file, or non-UTF-8 bytes.
    """
    path_part, _anchor = split_docs_ref(docs_ref)
    if not path_part.strip():
        raise DocumentationError(f"Documentation reference {docs_ref!r} names no file.")

    try:
        resolved = resolve_docs_path(project_root, path_part)
    except ValueError as exc:
        # kptn.inspection.resolve_docs_path signals containment failure with
        # ValueError; the UI needs one exception type for "cannot render this".
        raise DocumentationError(str(exc)) from exc

    if not resolved.is_file():
        raise DocumentationError(
            f"Documentation file {path_part!r} does not exist in this project "
            f"(looked for {resolved})."
        )

    try:
        text = resolved.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise DocumentationError(
            f"Documentation file {path_part!r} is not valid UTF-8 text."
        ) from exc
    except OSError as exc:
        raise DocumentationError(
            f"Documentation file {path_part!r} could not be read: {exc}."
        ) from exc

    # The only Markup in this module, and only ever over what the renderer
    # itself produced with html=False.
    return Markup(_renderer().render(text))


__all__ = ["DocumentationError", "render_project_markdown", "split_docs_ref"]
