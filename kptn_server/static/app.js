/*
 * Page-local behaviour for the shared pipeline UI.
 *
 * Vendored and dependency-free. htmx does the request plumbing; this file
 * holds only what htmx cannot express declaratively. It is intentionally
 * near-empty at this point: the run console's live-stream behaviour is added
 * here rather than in a new file, so base.html's script tag never changes.
 */

(function () {
  "use strict";

  window.kptn = window.kptn || {};
})();
