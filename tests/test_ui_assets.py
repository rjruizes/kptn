"""Cache-busting the vendored assets by content, not by kptn version.

A wheel reinstalled under the same version -- or swapped under a running
server -- leaves ``kptn.__version__`` unchanged, so a version in the URL
would leave browsers on their cached copies. These tests pin the two halves
of :mod:`kptn_server.assets`: the hash in the URL follows the file's bytes
with no restart, and ``/static`` sends cache headers that match the URL.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.routing import Mount

from kptn_server import assets
from kptn_server.app import create_app
from kptn_server.assets import (
    IMMUTABLE,
    REVALIDATE,
    VersionedStaticFiles,
    asset_version,
    content_hash,
)

pytestmark = pytest.mark.ui_hygiene


@pytest.fixture
def static_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "static"
    directory.mkdir()
    (directory / "app.css").write_text("body { color: black; }\n")
    return directory


@pytest.fixture
def static_client(static_dir: Path) -> TestClient:
    app = Starlette(
        routes=[Mount("/static", VersionedStaticFiles(directory=str(static_dir)))]
    )
    return TestClient(app)


def test_the_hash_follows_the_bytes(static_dir: Path) -> None:
    css = static_dir / "app.css"
    before = content_hash(css)

    css.write_text("body { color: red; }\n")

    assert content_hash(css) != before


def test_a_replaced_file_with_the_same_mtime_and_size_gets_a_new_hash(
    static_dir: Path,
) -> None:
    """What an install does: a new file renamed over the old one.

    Starlette's own ETag is built from mtime and size alone, so a build that
    matched both would look unchanged to it. The replacement is a new inode,
    so the hash is recomputed from the new bytes.
    """
    css = static_dir / "app.css"
    before = content_hash(css)
    old = css.stat()

    replacement = static_dir / "app.css.new"
    replacement.write_text(css.read_text().replace("black", "khaki"))
    os.utime(replacement, ns=(old.st_atime_ns, old.st_mtime_ns))
    os.replace(replacement, css)

    assert (css.stat().st_mtime_ns, css.stat().st_size) == (
        old.st_mtime_ns,
        old.st_size,
    )
    assert content_hash(css) != before


def test_the_page_links_new_bytes_without_a_restart(
    ui_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The case the kptn version missed: files replaced under a running app.

    One app serves two renders of the same page; between them the stylesheet
    is rewritten -- as a wheel swapped in place would -- and the second render
    links it under a new URL.
    """
    copy = tmp_path / "static-copy"
    shutil.copytree(assets.STATIC_DIR, copy)
    monkeypatch.setattr(assets, "STATIC_DIR", copy)
    client = TestClient(create_app(ui_project))

    def css_query() -> str:
        match = re.search(
            r'href="/static/app\.css\?v=([0-9a-f]+)"', client.get("/").text
        )
        assert match, "the page does not link app.css by hash"
        return match.group(1)

    before = css_query()
    (copy / "app.css").write_text((copy / "app.css").read_text() + "\n/* swapped */\n")

    assert css_query() != before


def test_a_current_version_is_cached_for_good(
    static_client: TestClient, static_dir: Path
) -> None:
    current = content_hash(static_dir / "app.css")

    response = static_client.get(f"/static/app.css?v={current}")

    assert response.status_code == 200
    assert response.headers["cache-control"] == IMMUTABLE


@pytest.mark.parametrize("query", ["", "?v=000000000000", "?v="])
def test_anything_else_is_revalidated(static_client: TestClient, query: str) -> None:
    """No version, or a stale one from a page rendered before the file
    changed: serve the current bytes, but never let them be kept unchecked."""
    response = static_client.get(f"/static/app.css{query}")

    assert response.status_code == 200
    assert response.headers["cache-control"] == REVALIDATE


def test_the_etag_is_the_content_hash(
    static_client: TestClient, static_dir: Path
) -> None:
    """Revalidation compares bytes, not Starlette's default mtime and size."""
    current = content_hash(static_dir / "app.css")

    response = static_client.get("/static/app.css")

    assert response.headers["etag"] == f'"{current}"'


def test_revalidation_answers_not_modified_with_the_same_policy(
    static_client: TestClient, static_dir: Path
) -> None:
    etag = static_client.get("/static/app.css").headers["etag"]

    response = static_client.get("/static/app.css", headers={"If-None-Match": etag})

    assert response.status_code == 304
    assert response.headers["cache-control"] == REVALIDATE


def test_revalidation_after_a_rewrite_sends_the_new_bytes(
    static_client: TestClient, static_dir: Path
) -> None:
    etag = static_client.get("/static/app.css").headers["etag"]
    (static_dir / "app.css").write_text("body { color: red; }\n")

    response = static_client.get("/static/app.css", headers={"If-None-Match": etag})

    assert response.status_code == 200
    assert "red" in response.text


def test_the_ui_serves_static_files_with_these_headers(ui_project: Path) -> None:
    """The real mount, not just the class: ``create_app`` uses it."""
    client = TestClient(create_app(ui_project))

    response = client.get(f"/static/app.js?v={asset_version('app.js')}")

    assert response.headers["cache-control"] == IMMUTABLE
    assert response.headers["etag"] == f'"{asset_version("app.js")}"'
