/*
 * Page-local behaviour for the shared pipeline UI.
 *
 * Vendored and dependency-free. htmx does the request plumbing; this file
 * holds only what htmx cannot express declaratively: the run console's live
 * event stream, the nav's profile, the app bar's active-run poll, and the
 * settings modal.
 *
 * An ES module, loaded by ``_modules.html``. Its imports are bare specifiers
 * ("kptn/...") that the import map in that partial resolves to versioned
 * URLs: a relative import would drop the ``?v=`` cache-buster, and a
 * browser would keep running the old module after kptn is upgraded.
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

import { autoScroll as startAutoScroll } from "kptn/auto-scroll";

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

var SOUND_STORAGE_KEY = "kptn.runSound";

/* The end of a run, heard: two short sine tones, rising for a run that
 * succeeded and falling for any other ending (failed, errored, interrupted,
 * stopped) -- the same completed/failed split VS Code's task signals make.
 * Generated rather than shipped, so there is no audio file to vendor. */
var TONES_SUCCEEDED = [523.25, 783.99]; /* C5 up to G5 */
var TONES_ENDED_OTHERWISE = [392.0, 261.63]; /* G4 down to C4 */
var TONE_SECONDS = 0.18;
var TONE_SPACING_SECONDS = 0.14;
var TONE_VOLUME = 0.2;
/* How late a tone may still start after the run ended. Past this the reader
 * has moved on, and a chime would announce nothing. */
var TONE_DEADLINE_MS = 1500;
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

/* Task folds left open behind the newest one -- OPEN_TASKS in
 * kptn_server/console_layout.py, which a reloaded page is rendered with. */
var OPEN_TASKS = 2;

function foldOf(element) {
  /* The fold whose body holds *element*, or null at the top level. */
  var body = element.parentElement;
  return body ? body.closest("[data-fold]") : null;
}

function setCount(fold, attribute, singular, plural, delta) {
  /* A task fold's "2,310 lines": the number lives in the attribute, and the
   * text is written back from it in the form _event.html renders. */
  var counter = fold.querySelector(":scope > details > summary [" + attribute + "]");
  if (!counter || !delta) {
    return;
  }
  var total = (parseInt(counter.getAttribute(attribute), 10) || 0) + delta;
  counter.setAttribute(attribute, String(total));
  counter.textContent = total.toLocaleString("en-US") + " " + (total === 1 ? singular : plural);
  counter.hidden = total === 0;
}

function countRow(row) {
  var fold = foldOf(row);
  if (!fold || fold.getAttribute("data-fold") !== "task") {
    return;
  }
  setCount(fold, "data-fold-lines", "line", "lines", parseInt(row.getAttribute("data-lines"), 10) || 0);
  if (row.getAttribute("data-kind") === "warning") {
    setCount(fold, "data-fold-warnings", "warning", "warnings", 1);
  }
}

function foldFailed(fold) {
  return fold.querySelector(".event--failed") !== null;
}

function settleFolds(list, newest) {
  /* Fold away what the run has moved past, as build_console() renders a
   * reloaded page: only the newest OPEN_TASKS task folds stay open, and only
   * the groups the newest row sits in -- so a pipeline folds once the run
   * leaves it. A failure is never folded away, and neither is anything the
   * reader opened or closed themselves. */
  var tasks = list.querySelectorAll('[data-fold="task"]');
  for (var i = 0; i < tasks.length - OPEN_TASKS; i += 1) {
    closeFold(tasks[i]);
  }
  var current = [];
  for (var fold = newest && foldOf(newest); fold; fold = foldOf(fold)) {
    current.push(fold);
  }
  var groups = list.querySelectorAll('[data-fold="group"]');
  for (var k = 0; k < groups.length; k += 1) {
    if (current.indexOf(groups[k]) === -1) {
      closeFold(groups[k]);
    }
  }
}

function closeFold(fold) {
  var details = fold.firstElementChild;
  if (
    details &&
    details.open &&
    !fold.hasAttribute("data-fold-touched") &&
    !foldFailed(fold)
  ) {
    details.open = false;
  }
}

function rememberTouchedFolds(console_) {
  /* A fold the reader has opened or closed is theirs from then on. Read off
   * the click rather than the ``toggle`` event, which fires for the folding
   * this file does too; Enter and Space on a summary arrive as clicks. */
  console_.addEventListener("click", function (event) {
    var summary = event.target.closest("summary.fold__head");
    if (!summary || !console_.contains(summary)) {
      return;
    }
    summary.closest("[data-fold]").setAttribute("data-fold-touched", "");
    /* A task's heading sticks to the top of the screen while its output
     * scrolls by, so it can be clicked from far down the task. Closing it
     * there pulls everything below up past the reader; bring the heading
     * back to the top instead, where it was. */
    var details = summary.parentElement;
    if (
      details.open &&
      !event.target.closest("[data-fold-jump]") &&
      details.getBoundingClientRect().top < 0
    ) {
      window.requestAnimationFrame(function () {
        summary.scrollIntoView({ block: "start" });
      });
    }
  });
}

function collapseFromGutters(console_) {
  /* The strip down the left of an open fold's body closes it, so a reader
   * deep in a long task does not have to scroll back to its heading. The
   * rows below then move up, so the heading is brought back into view if
   * it was above the screen -- the place the reader just folded away is
   * where they expect to be. Mouse-only: from the keyboard, the heading is
   * the control. */
  console_.addEventListener("click", function (event) {
    var gutter = event.target.closest("[data-fold-gutter]");
    if (!gutter || !console_.contains(gutter)) {
      return;
    }
    var fold = gutter.closest("[data-fold]");
    var details = fold.firstElementChild;
    details.open = false;
    fold.setAttribute("data-fold-touched", "");
    details.querySelector(":scope > summary").scrollIntoView({ block: "nearest" });
  });
}

function jumpToFoldEnds(console_) {
  /* The "end" button in a task's heading: open the task if it is folded and
   * bring its last row into view. The button sits inside the <summary>, so
   * the click is kept from toggling the fold as well -- a folded task opens,
   * an open one stays open. The click still reaches the listener above, so
   * a task opened this way is the reader's and is not folded under them. */
  console_.addEventListener("click", function (event) {
    var button = event.target.closest("[data-fold-jump]");
    if (!button || !console_.contains(button)) {
      return;
    }
    event.preventDefault();
    var details = button.closest("details");
    details.open = true;
    var body = details.querySelector(":scope > .fold__body");
    var last = body && body.lastElementChild;
    (last || details).scrollIntoView({ block: "end" });
  });
}

function setUpRememberedToggle(toggle, storageKey) {
  if (!toggle) {
    return;
  }
  try {
    var stored = window.localStorage.getItem(storageKey);
    if (stored !== null) {
      toggle.checked = stored === "true";
    }
  } catch (err) {
    /* Private browsing, or storage disabled. The default stands. */
  }
  toggle.addEventListener("change", function () {
    try {
      window.localStorage.setItem(storageKey, String(toggle.checked));
    } catch (err) {
      /* Not being able to remember the choice is not a reason to refuse it. */
    }
  });
}

var audio = null;

function audioContext() {
  /* Made on first use, never at load: a context created before the page
   * may play sound starts suspended, and there is nothing to create one
   * for until a run is watched. */
  if (audio === null) {
    var Context = window.AudioContext || window.webkitAudioContext;
    if (!Context) {
      return null;
    }
    try {
      audio = new Context();
    } catch (err) {
      return null;
    }
  }
  return audio;
}

function unlockSoundOnFirstGesture() {
  /* Browsers let a page play sound only after the reader has clicked or
   * typed on it. Reaching the run page by pressing Run carries that over
   * (checked in Chrome), so this is for a run page reached some other way
   * -- a reopened tab. The first click or keypress, anywhere, unlocks it. */
  function unlock() {
    document.removeEventListener("pointerdown", unlock, true);
    document.removeEventListener("keydown", unlock, true);
    var context = audioContext();
    if (context && context.state === "suspended") {
      context.resume().catch(function () {});
    }
  }
  document.addEventListener("pointerdown", unlock, true);
  document.addEventListener("keydown", unlock, true);
}

function playTones(frequencies) {
  var context = audioContext();
  if (!context) {
    return;
  }
  var requested = Date.now();

  function play() {
    /* Still blocked, or allowed only much later: stay silent. A suspended
     * context would otherwise hold the tones and sound them whenever the
     * reader next happened to click. */
    if (context.state !== "running" || Date.now() - requested > TONE_DEADLINE_MS) {
      return;
    }
    var start = context.currentTime + 0.02;
    for (var i = 0; i < frequencies.length; i += 1) {
      var at = start + i * TONE_SPACING_SECONDS;
      var oscillator = context.createOscillator();
      var envelope = context.createGain();
      oscillator.type = "sine";
      oscillator.frequency.value = frequencies[i];
      /* Fade in and out: a sine that starts or stops at full volume clicks. */
      envelope.gain.setValueAtTime(0.0001, at);
      envelope.gain.exponentialRampToValueAtTime(TONE_VOLUME, at + 0.015);
      envelope.gain.exponentialRampToValueAtTime(0.0001, at + TONE_SECONDS);
      oscillator.connect(envelope);
      envelope.connect(context.destination);
      oscillator.start(at);
      oscillator.stop(at + TONE_SECONDS + 0.02);
    }
  }

  if (context.state === "running") {
    play();
  } else {
    context.resume().then(play, function () {});
  }
}

function announceRunEnd(status) {
  /* Read at the moment the run ends, not when the page loaded: the reader
   * may have changed it in the settings modal while watching. */
  var toggle = document.getElementById("run-sound");
  if (toggle && !toggle.checked) {
    return;
  }
  playTones(status === "succeeded" ? TONES_SUCCEEDED : TONES_ENDED_OTHERWISE);
}

function initConsole() {
  var console_ = document.getElementById("console");
  if (!console_ || typeof window.EventSource !== "function") {
    return;
  }

  var list = document.getElementById("console-events");
  var empty = document.getElementById("console-empty");
  rememberTouchedFolds(console_);
  jumpToFoldEnds(console_);
  collapseFromGutters(console_);
  if (console_.getAttribute("data-terminal") === "true") {
    /* Already over. Its whole history is on the page, and its status came
     * from the run row -- there is nothing left to stream, and no ending
     * left to hear. */
    return;
  }
  unlockSoundOnFirstGesture();

  var autoScroll = startAutoScroll();

  var source = null;
  var closed = false;

  /* Rows from the stream wait here and are added once per animation frame.
   * Following the output means scrolling to the bottom, and reading the
   * page's height forces a layout of the whole console: per row, that was
   * ~20ms at 8,000 rows on a fast laptop, so a chatty run outpaced the
   * browser and froze the tab for tens of seconds. Per frame, the cost is
   * paid once however many rows arrived. A hidden tab gets no frames and
   * so does no layout at all until it is looked at. */
  var queued = [];
  var queuedSequences = {};
  var flushScheduled = false;
  var nextFrame =
    window.requestAnimationFrame ||
    function (callback) {
      return window.setTimeout(callback, 16);
    };

  function appendEvent(event) {
    var frame = JSON.parse(event.data);
    if (
      queuedSequences[frame.sequence] ||
      document.getElementById("event-" + frame.sequence)
    ) {
      return; /* A resumed stream may overlap by one; never duplicate. */
    }
    queuedSequences[frame.sequence] = true;
    queued.push(frame);
    if (!flushScheduled) {
      flushScheduled = true;
      nextFrame(flushQueued);
    }
  }

  function flushQueued() {
    flushScheduled = false;
    if (!queued.length) {
      return;
    }
    /* Each frame says where its row goes: the folds to open first, the
     * element to append to, and a finished task's heading to update. All of
     * it was decided by the server; this only finds the ids. */
    var newest = null;
    for (var i = 0; i < queued.length; i += 1) {
      var frame = queued[i];
      var opens = frame.opens || [];
      for (var k = 0; k < opens.length; k += 1) {
        if (!document.getElementById(opens[k].id)) {
          (document.getElementById(opens[k].parent) || list).appendChild(
            fragmentFrom(opens[k].html)
          );
        }
      }
      var parent = (frame.parent && document.getElementById(frame.parent)) || list;
      parent.appendChild(fragmentFrom(frame.html));
      newest = parent.lastElementChild;
      countRow(newest);
      if (frame.outcome) {
        var outcome = document.getElementById(frame.outcome.id);
        if (outcome) {
          outcome.replaceWith(fragmentFrom(frame.outcome.html));
        }
      }
    }
    queued = [];
    queuedSequences = {};
    if (empty) {
      empty.remove();
      empty = null;
    }
    updateCounters(console_, list);
    /* Folding while the reader is scrolled up would pull the text they are
     * reading out from under them. The next batch that arrives while they
     * are following catches up. */
    if (autoScroll.following()) {
      settleFolds(list, newest);
    }
    /* The list has no scroller of its own -- the page scrolls -- so
     * following the output means keeping the newest row in view. */
    autoScroll.keep();
  }

  function streamCursor() {
    /* Rows still waiting for a frame count as received: reconnecting from
     * the list alone would ask for them again. */
    var cursor = resumeFrom(console_, list);
    for (var i = 0; i < queued.length; i += 1) {
      cursor = Math.max(cursor, queued[i].sequence);
    }
    return cursor;
  }

  function applyRegion(event) {
    /* Rows first: a region or status frame describes the run *after* the
     * events sent before it, and must not land ahead of them. */
    flushQueued();
    /* Server-rendered and already escaped, like every other frame: the
     * region is swapped wholesale, never assembled here. */
    var frame = JSON.parse(event.data);
    var current = document.getElementById(frame.target);
    if (current) {
      current.replaceWith(fragmentFrom(frame.html));
      autoScroll.keep();
    }
  }

  function applyStatus(event) {
    flushQueued();
    var frame = JSON.parse(event.data);
    var current = document.getElementById("run-status");
    if (current) {
      current.replaceWith(fragmentFrom(frame.html));
      autoScroll.keep();
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
      announceRunEnd(frame.status);
    }
  }

  function connect() {
    if (closed) {
      return;
    }
    source = new EventSource(
      console_.getAttribute("data-stream-url") + "?after=" + streamCursor()
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

function initProjectPicker() {
  /* The project picker is a <details>, which opens and closes on its own
   * and needs nothing from here to work. What it lacks as a menu is the
   * two ways out a reader expects: Escape, and a click anywhere else. */
  var picker = document.querySelector("[data-project-picker]");
  if (!picker) {
    return;
  }
  document.addEventListener("click", function (event) {
    if (picker.open && !picker.contains(event.target)) {
      picker.open = false;
    }
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && picker.open) {
      picker.open = false;
      picker.querySelector("summary").focus();
    }
  });
}

function initSettings() {
  /* The gear in the app bar opens a native <dialog>, which brings its own
   * focus trap, Escape, and backdrop; its Close button is a
   * ``method="dialog"`` submit and needs nothing from here. What it lacks is
   * closing on a click outside the box -- the backdrop is the dialog
   * element itself, so a click whose target *is* the dialog landed on it. */
  var dialog = document.getElementById("settings-dialog");
  var opener = document.querySelector("[data-settings-open]");
  setUpRememberedToggle(document.getElementById("run-sound"), SOUND_STORAGE_KEY);
  /* The preview buttons play an ending's tones on demand. The click is the
   * gesture that lets the audio context start, so these always sound. */
  var previews = document.querySelectorAll("[data-sound-preview]");
  for (var i = 0; i < previews.length; i += 1) {
    previews[i].addEventListener("click", function (event) {
      var button = event.currentTarget;
      var tones =
        button.getAttribute("data-sound-preview") === "succeeded"
          ? TONES_SUCCEEDED
          : TONES_ENDED_OTHERWISE;
      playTones(tones);
      /* Lit for as long as the tones last, so a click is seen even with the
       * volume down. A second click restarts the timer rather than racing it. */
      var lastsMs =
        ((tones.length - 1) * TONE_SPACING_SECONDS + TONE_SECONDS) * 1000 + 120;
      button.classList.add("is-playing");
      window.clearTimeout(button.kptnPlayingTimer);
      button.kptnPlayingTimer = window.setTimeout(function () {
        button.classList.remove("is-playing");
      }, lastsMs);
    });
  }
  if (!dialog || !opener || typeof dialog.showModal !== "function") {
    return;
  }
  opener.addEventListener("click", function () {
    dialog.showModal();
  });
  dialog.addEventListener("click", function (event) {
    if (event.target === dialog) {
      dialog.close();
    }
  });
}

initSettings();
initConsole();
initProfileNav();
initRunLock();
initProjectPicker();
