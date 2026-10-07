"""Cache-busting for the vendored assets: content hashes and cache headers.

Every asset link carries ``?v=<hash>``, where the hash is of the file's
bytes. The kptn version cannot do this job: a wheel rebuilt and reinstalled
under the same version -- or swapped under a running server, which reads
templates and static files from disk on every request but keeps the
``kptn.__version__`` it imported at startup -- would leave every URL
unchanged and every browser on its cached copy.

So :func:`asset_version` hashes the file, and remembers the hash only for as
long as the file's ``stat`` is unchanged. Rewriting, replacing, or
reinstalling a file changes its inode, mtime, or size, and the next page
render links the new bytes under a new URL. Each file is versioned on its
own, so a stylesheet change does not invalidate the scripts.

:class:`VersionedStaticFiles` then serves those URLs with headers to match:

* a request whose ``v`` is the file's current hash is ``immutable`` for a
  year -- a different file can only ever be a different URL;
* anything else -- no ``v``, or a stale one from a page rendered before the
  file changed -- is ``no-cache``, revalidated on every use;

and the ``ETag`` is the content hash too, rather than Starlette's default of
mtime and size: an install renames a new file over the old one, and that new
inode is rehashed even if a build happened to match both.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from urllib.parse import parse_qs

from starlette.datastructures import Headers
from starlette.responses import FileResponse, Response
from starlette.staticfiles import NotModifiedResponse, StaticFiles
from starlette.types import Scope

# Package-relative, never cwd-relative: an installed (non-editable) wheel is
# not served out of the developer's working directory.
STATIC_DIR = Path(__file__).parent / "static"

#: Hex digits of the sha256 kept in a URL. 48 bits: ample for a few files.
HASH_LENGTH = 12

IMMUTABLE = "public, max-age=31536000, immutable"
REVALIDATE = "no-cache"

#: path -> (the stat it was hashed under, its hash).
_hashes: dict[Path, tuple[tuple[int, int, int], str]] = {}


def content_hash(path: Path, stat_result: os.stat_result | None = None) -> str:
    """The first :data:`HASH_LENGTH` hex digits of *path*'s sha256.

    Recomputed only when the file's inode, mtime, or size has changed since
    the last call, so a page render costs a ``stat`` per asset, not a read.
    """
    stat_result = stat_result or path.stat()
    key = (stat_result.st_ino, stat_result.st_mtime_ns, stat_result.st_size)
    cached = _hashes.get(path)
    if cached is not None and cached[0] == key:
        return cached[1]
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:HASH_LENGTH]
    _hashes[path] = (key, digest)
    return digest


def asset_version(name: str) -> str:
    """The ``v`` query for ``static/<name>``; a Jinja global in both template
    environments, used as ``?v={{ asset_version("app.js") }}``."""
    return content_hash(STATIC_DIR / name)


class VersionedStaticFiles(StaticFiles):
    """``StaticFiles`` with content-hash ETags and versioned cache headers."""

    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        current = content_hash(Path(full_path), stat_result)
        query = parse_qs(scope.get("query_string", b"").decode("latin-1"))
        cache_control = IMMUTABLE if query.get("v") == [current] else REVALIDATE
        # FileResponse only sets an ETag the caller has not, so this one wins.
        response = FileResponse(
            full_path,
            status_code=status_code,
            stat_result=stat_result,
            headers={"etag": f'"{current}"', "cache-control": cache_control},
        )
        if self.is_not_modified(response.headers, Headers(scope=scope)):
            return NotModifiedResponse(response.headers)
        return response
