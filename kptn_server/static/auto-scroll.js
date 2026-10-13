/*
 * Auto-scroll: keep the bottom of the run log in view while the reader is
 * there.
 *
 * An ES module, imported by app.js for the run log: `autoScroll(element)`
 * starts watching the reader and returns an object whose `keep()` the log
 * calls after each change to it. Vendored and dependency-free, like app.js.
 */

/* How close to the bottom of the page still counts as "at the bottom".
 * Browser zoom and fractional device pixels leave scroll positions a pixel
 * or so short of the exact end. */
var AUTO_SCROLL_SLACK_PX = 8;

export function autoScroll(scroller) {
  /* Keep the bottom of *scroller* in view while the reader is there -- an
   * element that scrolls on its own, like the run page's log, or the page
   * itself when none is given. It follows until the reader scrolls up to
   * read something, and follows again once they scroll back to the bottom.
   * Not a setting -- where the reader has scrolled is the whole of it.
   *
   * Intent is read from the input, not only from the scroll position. A
   * trackpad's first nudge moves it a pixel or two -- still inside
   * AUTO_SCROLL_SLACK_PX of the bottom -- and the next frame of new content
   * would scroll it straight back down, so the reader could never get
   * away. Any upward wheel, key, or scroll therefore stops following at
   * once; only reaching the bottom while *not* moving up resumes it.
   * Content growing under a still viewport fires no scroll event, so it
   * never counts as the reader moving.
   *
   * Returns an object whose ``keep()`` the caller invokes after each
   * change: it scrolls to the bottom if, and only if, the reader is still
   * following. */
  var page = !scroller;
  var target = page ? window : scroller;
  var following = true;
  var UP_KEYS = { ArrowUp: true, PageUp: true, Home: true };

  function top() {
    return page ? window.scrollY : scroller.scrollTop;
  }

  var lastTop = top();

  function atBottom() {
    if (page) {
      var root = document.documentElement;
      return window.scrollY + window.innerHeight >= root.scrollHeight - AUTO_SCROLL_SLACK_PX;
    }
    return scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - AUTO_SCROLL_SLACK_PX;
  }

  function stopFollowing() {
    /* At the very top there is nowhere further up to go, and no scroll
     * event would ever come to turn following back on. */
    if (top() > 0) {
      following = false;
    }
  }

  target.addEventListener(
    "wheel",
    function (event) {
      if (event.deltaY < 0) {
        stopFollowing();
      }
    },
    { passive: true }
  );
  (page ? window : scroller).addEventListener("keydown", function (event) {
    if (UP_KEYS[event.key] || (event.key === " " && event.shiftKey)) {
      stopFollowing();
    }
  });
  target.addEventListener(
    "scroll",
    function () {
      var movedUp = top() < lastTop;
      lastTop = top();
      if (movedUp) {
        following = false;
      } else if (atBottom()) {
        following = true;
      }
    },
    { passive: true }
  );

  return {
    /* Whether it is following the output, for callers that must not move
     * it out from under a reader who has scrolled up to read. */
    following: function () {
      return following;
    },
    keep: function () {
      /* Noting where this left it means a swap above the bottom that scroll
       * anchoring nudged upward on the way is not mistaken for the reader
       * leaving. */
      if (following) {
        if (page) {
          window.scrollTo(0, document.documentElement.scrollHeight);
        } else {
          scroller.scrollTop = scroller.scrollHeight;
        }
        lastTop = top();
      }
    }
  };
}
