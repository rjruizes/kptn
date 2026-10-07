/*
 * Auto-scroll: keep the bottom of the page in view while the reader is there.
 *
 * An ES module, imported by app.js for the run console: `autoScroll()`
 * starts watching the reader and returns an object whose `keep()` the
 * console calls after each change to the page. Vendored and
 * dependency-free, like app.js.
 */

/* How close to the bottom of the page still counts as "at the bottom".
 * Browser zoom and fractional device pixels leave scroll positions a pixel
 * or so short of the exact end. */
var AUTO_SCROLL_SLACK_PX = 8;

export function autoScroll() {
  /* Keep the bottom of the page in view while the reader is there: the
   * page follows until the reader scrolls up to read something, and
   * follows again once they scroll back to the bottom. Not a setting --
   * where the reader has scrolled is the whole of it.
   *
   * Intent is read from the input, not only from the scroll position. A
   * trackpad's first nudge moves the page a pixel or two -- still inside
   * AUTO_SCROLL_SLACK_PX of the bottom -- and the next frame of new content
   * would scroll it straight back down, so the reader could never get
   * away. Any upward wheel, key, or scroll therefore stops following at
   * once; only reaching the bottom while *not* moving up resumes it.
   * Content growing under a still viewport fires no scroll event, so it
   * never counts as the reader moving.
   *
   * Returns an object whose ``keep()`` the caller invokes after each
   * change to the page: it scrolls to the bottom if, and only if, the
   * reader is still following. The page scrolls, not an element, so this
   * owns window-level listeners and there is one per page. */
  var following = true;
  var lastScrollY = window.scrollY;
  var UP_KEYS = { ArrowUp: true, PageUp: true, Home: true };

  function atBottom() {
    var root = document.documentElement;
    return window.scrollY + window.innerHeight >= root.scrollHeight - AUTO_SCROLL_SLACK_PX;
  }

  function stopFollowing() {
    /* At the very top there is nowhere further up to go, and no scroll
     * event would ever come to turn following back on. */
    if (window.scrollY > 0) {
      following = false;
    }
  }

  window.addEventListener(
    "wheel",
    function (event) {
      if (event.deltaY < 0) {
        stopFollowing();
      }
    },
    { passive: true }
  );
  window.addEventListener("keydown", function (event) {
    if (UP_KEYS[event.key] || (event.key === " " && event.shiftKey)) {
      stopFollowing();
    }
  });
  window.addEventListener(
    "scroll",
    function () {
      var movedUp = window.scrollY < lastScrollY;
      lastScrollY = window.scrollY;
      if (movedUp) {
        following = false;
      } else if (atBottom()) {
        following = true;
      }
    },
    { passive: true }
  );

  return {
    keep: function () {
      /* Noting where this left the page means a swap above the bottom
       * that scroll anchoring nudged upward on the way is not mistaken
       * for the reader leaving. */
      if (following) {
        window.scrollTo(0, document.documentElement.scrollHeight);
        lastScrollY = window.scrollY;
      }
    }
  };
}

