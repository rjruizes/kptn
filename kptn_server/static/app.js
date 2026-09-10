/*
 * Page-local behaviour for the shared pipeline UI.
 *
 * Vendored and dependency-free. htmx does the request plumbing; this file
 * holds only what htmx cannot express declaratively, which right now is one
 * thing: the run console's live event stream.
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

  var FOLLOW_STORAGE_KEY = "kptn.followOutput";
  var RECONNECT_DELAY_MS = 1000;

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
     * everything. */
    var counters = console_.querySelectorAll("[data-counter]");
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
        list.scrollTop = list.scrollHeight;
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

  window.kptn.initConsole = initConsole;
  initConsole();
})();
