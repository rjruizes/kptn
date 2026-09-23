/*
 * Page-local behaviour for the shared pipeline UI.
 *
 * Vendored and dependency-free. htmx does the request plumbing; this file
 * holds only what htmx cannot express declaratively: the run console's live
 * event stream, the nav's profile, and the app bar's active-run poll.
 *
 * Two rules govern everything below.
 *
 * 1. **The server renders; this file appends.** Every frame the stream sends
 *    carries the same HTML fragment the run page would have rendered for that
 *    event, already escaped by Jinja. Pipeline output is untrusted text -- a
 *    dependency's banner, a filename, a SQL error echoing a value -- so it is
 *    never formatted, concatenated, or templated here. There is exactly one
 *    `innerHTML` write in this file and it takes a server-rendered fragment,
 *    never log text.
 *
 * 2. **The cursor lives in the DOM.** A reconnect resumes from the highest
 *    `data-sequence` already on the page, and the browser's own
 *    `Last-Event-ID` covers the reconnects it makes itself. Nothing about the
 *    run's progress is kept in a variable that a reload would lose.
 */

(function () {
  "use strict";

  window.kptn = window.kptn || {};

  /* The path prefix a reverse proxy strips before requests reach the
   * server -- empty when served at the root. Every URL this script builds
   * itself has to carry it, or it resolves against the proxy's host and
   * leaves the proxy. URLs that come from the server already carry it. */
  var BASE = (document.body && document.body.getAttribute("data-base")) || "";

  // Two prefixes, for the same reason the templates have two. BASE reaches
  // the application itself -- the /static mount and /healthz live there and
  // belong to no project. PROJECT_BASE reaches the project this page is
  // showing: it is BASE plus /p/<slug> when one server offers several
  // working directories, and identical to BASE when it serves just one.
  // Every URL this file builds is project-scoped, so it uses PROJECT_BASE;
  // naming both makes the choice explicit for the next one.
  var PROJECT_BASE =
    (document.body && document.body.getAttribute("data-project-base")) || BASE;

  var FOLLOW_STORAGE_KEY = "kptn.followOutput";
  var RECONNECT_DELAY_MS = 1000;
  var ACTIVE_RUN_URL = PROJECT_BASE + "/active-run";
  var ACTIVE_RUN_POLL_MS = 3000;

  /* Every event name the server can send. EventSource dispatches by name, so
   * an unlisted kind would arrive and be silently dropped. */
  var EVENT_KINDS = [
    "run_started",
    "task_started",
    "task_skipped",
    "log",
    "warning",
    "task_finished",
    "run_finished"
  ];
  var STATUS_EVENT = "run_status";

  /* Regions of the run page the server re-renders and this file swaps in by
   * id. What is in them comes from the run row rather than from the event
   * list, so a run that ends under an open stream leaves them showing what
   * was true when the page was rendered -- a Stop button for a process that
   * is gone. Must stay in step with
   * kptn_server.routes.runs.REGION_EVENT_TARGETS; a test enforces it,
   * because a region nobody listens for is dropped in silence. */
  var REGION_EVENTS = { run_header: "run-header" };

  function highestSequence(list) {
    var rows = list.querySelectorAll("[data-sequence]");
    var highest = 0;
    for (var i = 0; i < rows.length; i += 1) {
      var sequence = parseInt(rows[i].getAttribute("data-sequence"), 10);
      if (!isNaN(sequence) && sequence > highest) {
        highest = sequence;
      }
    }
    return highest;
  }

  function resumeFrom(console_, list) {
    /* The page's own cursor: whichever of "what the server had rendered" and
     * "what is actually in the list" is further along. They agree on a fresh
     * page; they diverge if a fragment swap ever replaces the list. */
    var rendered = parseInt(console_.getAttribute("data-last-sequence"), 10);
    return Math.max(isNaN(rendered) ? 0 : rendered, highestSequence(list));
  }

  function fragmentFrom(html) {
    /* A <template> parses the fragment without running it or reparenting it
     * into the document. The fragment is server-rendered and already escaped;
     * this is the only markup-parsing call in the file. */
    var holder = document.createElement("template");
    holder.innerHTML = html;
    return holder.content;
  }

  function updateCounters(console_, list) {
    /* Counted off the DOM rather than tallied in a variable, so the numbers
     * survive a reconnect that re-sends nothing and a reload that re-renders
     * everything.
     *
     * Searched from the document, not from the console: the counters sit in
     * the run header now, beside the controls that act on the run. Scoped to
     * the console they would simply never be found, and would sit frozen at
     * their server-rendered values for the life of the connection. */
    var counters = document.querySelectorAll("[data-counter]");
    for (var i = 0; i < counters.length; i += 1) {
      var kind = { tasks: "task_started", skipped: "task_skipped", warnings: "warning" }[
        counters[i].getAttribute("data-counter")
      ];
      if (kind) {
        counters[i].textContent = String(
          list.querySelectorAll('[data-kind="' + kind + '"]').length
        );
      }
    }
  }

  function setUpFollowToggle(toggle) {
    if (!toggle) {
      return;
    }
    try {
      var stored = window.localStorage.getItem(FOLLOW_STORAGE_KEY);
      if (stored !== null) {
        toggle.checked = stored === "true";
      }
    } catch (err) {
      /* Private browsing, or storage disabled. The default stands. */
    }
    toggle.addEventListener("change", function () {
      try {
        window.localStorage.setItem(FOLLOW_STORAGE_KEY, String(toggle.checked));
      } catch (err) {
        /* Not being able to remember the choice is not a reason to refuse it. */
      }
    });
  }

  function initConsole() {
    var console_ = document.getElementById("console");
    if (!console_ || typeof window.EventSource !== "function") {
      return;
    }

    var list = document.getElementById("console-events");
    var empty = document.getElementById("console-empty");
    var follow = document.getElementById("follow-output");
    setUpFollowToggle(follow);

    if (console_.getAttribute("data-terminal") === "true") {
      /* Already over. Its whole history is on the page, and its status came
       * from the run row -- there is nothing left to stream. */
      return;
    }

    var source = null;
    var closed = false;

    function appendEvent(event) {
      var frame = JSON.parse(event.data);
      if (document.getElementById("event-" + frame.sequence)) {
        return; /* A resumed stream may overlap by one; never duplicate. */
      }
      list.appendChild(fragmentFrom(frame.html));
      if (empty) {
        empty.remove();
        empty = null;
      }
      updateCounters(console_, list);
      if (!follow || follow.checked) {
        /* The list has no scroller of its own -- the page scrolls -- so
         * following the output means keeping the newest row in view. */
        window.scrollTo(0, document.documentElement.scrollHeight);
      }
    }

    function applyRegion(event) {
      /* Server-rendered and already escaped, like every other frame: the
       * region is swapped wholesale, never assembled here. */
      var frame = JSON.parse(event.data);
      var current = document.getElementById(frame.target);
      if (current) {
        current.replaceWith(fragmentFrom(frame.html));
      }
    }

    function applyStatus(event) {
      var frame = JSON.parse(event.data);
      var current = document.getElementById("run-status");
      if (current) {
        current.replaceWith(fragmentFrom(frame.html));
      }
      console_.setAttribute("data-status", frame.status);
      if (frame.terminal) {
        /* The server closes a terminal run's stream. Closing this side too
         * stops EventSource from reconnecting into the same immediate close,
         * forever. */
        console_.setAttribute("data-terminal", "true");
        closed = true;
        if (source) {
          source.close();
        }
      }
    }

    function connect() {
      if (closed) {
        return;
      }
      source = new EventSource(
        console_.getAttribute("data-stream-url") + "?after=" + resumeFrom(console_, list)
      );
      for (var i = 0; i < EVENT_KINDS.length; i += 1) {
        source.addEventListener(EVENT_KINDS[i], appendEvent);
      }
      for (var name in REGION_EVENTS) {
        if (Object.prototype.hasOwnProperty.call(REGION_EVENTS, name)) {
          source.addEventListener(name, applyRegion);
        }
      }
      source.addEventListener(STATUS_EVENT, applyStatus);
      source.addEventListener("error", function () {
        /* EventSource retries on its own with Last-Event-ID, but only while
         * it considers the connection recoverable. Once it gives up, rebuild
         * it from the cursor the page can see. */
        if (source.readyState === EventSource.CLOSED && !closed) {
          window.setTimeout(connect, RECONNECT_DELAY_MS);
        }
      });
    }

    connect();
    window.addEventListener("pagehide", function () {
      closed = true;
      if (source) {
        source.close();
      }
    });
  }

  function initProfileNav() {
    /* The app bar's nav links already carry the profile the server rendered
     * the page with. On the run console the selector is an input for
     * `run-form`, so a fresh choice reaches the server only on submit --
     * nothing server-rendered can know it, and clicking Plan would navigate
     * to the profile the page loaded with rather than the one on screen.
     * Keeping the hrefs in step is the whole job.
     *
     * With JavaScript off the links keep their rendered profile, which is
     * the no-JS behaviour the nav is built for; this only ever improves on
     * it. */
    var select = document.getElementById("profile-select");
    var links = document.querySelectorAll("[data-profile-link]");
    if (!select) {
      return;
    }

    var planInputs = document.querySelectorAll("[data-profile-input]");

    function sync() {
      for (var i = 0; i < links.length; i += 1) {
        var base = links[i].getAttribute("data-profile-link");
        links[i].setAttribute(
          "href",
          select.value
            ? base + "?profile=" + encodeURIComponent(select.value)
            : base
        );
      }
      /* The Plan button submits a form rather than following a link, so its
       * profile rides in a hidden input. Disabled when there is none: a
       * disabled control is not submitted, which keeps the URL a clean
       * "/plan" instead of "/plan?profile=". */
      for (var k = 0; k < planInputs.length; k += 1) {
        planInputs[k].value = select.value;
        planInputs[k].disabled = !select.value;
      }
    }

    select.addEventListener("change", sync);
  }

  function initRunLock() {
    /* The app bar renders its controls disabled while a run holds the
     * project lock, which is right at the moment the page is drawn and stale
     * the moment the run ends. Only the run console has a stream to hear
     * that on, and the bar is on every page -- so the bar asks.
     *
     * Polling rather than swapping a server-rendered fragment in: the group
     * holds the profile <select>, and replacing it every few seconds would
     * discard a choice the reader had not submitted yet. `disabled` is the
     * one thing that can be toggled without touching what is in the
     * controls. Same reason the console is not special-cased -- one
     * mechanism on every page cannot disagree with itself.
     *
     * With JavaScript off the controls keep whatever the server rendered,
     * which is correct for that page load; this only ever improves on it. */
    var controls = document.querySelectorAll("[data-run-control]");
    var busy = document.querySelector("[data-run-busy]");
    if (!controls.length || typeof window.fetch !== "function") {
      return;
    }

    var timer = null;

    function apply(state) {
      for (var i = 0; i < controls.length; i += 1) {
        controls[i].disabled = state.active;
      }
      if (busy) {
        /* Property assignments, never markup: the run id is the only value
         * that moves, and it goes in through `href`. */
        busy.href = state.run_id
          ? PROJECT_BASE + "/runs/" + encodeURIComponent(state.run_id)
          : "";
        busy.hidden = !state.active;
      }
    }

    function poll() {
      window
        .fetch(ACTIVE_RUN_URL, { headers: { Accept: "application/json" } })
        .then(function (response) {
          return response.ok ? response.json() : null;
        })
        .then(function (state) {
          if (state) {
            apply(state);
          }
        })
        .catch(function () {
          /* A server that is restarting under a running dev session is the
           * common case. Leave the bar as it is and ask again. */
        });
    }

    function start() {
      if (timer === null) {
        timer = window.setInterval(poll, ACTIVE_RUN_POLL_MS);
      }
    }

    function stop() {
      if (timer !== null) {
        window.clearInterval(timer);
        timer = null;
      }
    }

    document.addEventListener("visibilitychange", function () {
      /* A backgrounded tab polling every three seconds is a run store query
       * every three seconds for nobody. Ask once on the way back, so the bar
       * is right before the reader can reach for it. */
      if (document.hidden) {
        stop();
      } else {
        poll();
        start();
      }
    });

    if (!document.hidden) {
      start();
    }
    window.addEventListener("pagehide", stop);
  }

  window.kptn.initConsole = initConsole;
  window.kptn.initProfileNav = initProfileNav;
  window.kptn.initRunLock = initRunLock;
  initConsole();
  initProfileNav();
  initRunLock();
})();
