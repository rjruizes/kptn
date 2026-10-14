/*
 * Page-local behaviour for the shared pipeline UI.
 *
 * Vendored and dependency-free. htmx does the request plumbing; this file
 * holds only what htmx cannot express declaratively: the run console's live
 * event stream, the sidebar's panes, the profile selector, the active-run
 * poll, and the settings modal with the reader's theme.
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
/* Read before the page paints by _theme.html, under the same name. */
var THEME_STORAGE_KEY = "kptn.theme";
var SIDEBAR_STORAGE_KEY = "kptn.sidebarOpen";
var PANES_STORAGE_KEY = "kptn.closedPanes";

/* Fired on the document when a run may have started or ended: the Runs
 * pane and the Next run card fetch themselves again. */
var RUNS_CHANGED = "kptn:runs-changed";

/* The workspace's rail (the Run pane's tasks) and sidebar, once set up. */
var RAIL = null;
var SIDEBAR = null;

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
  "run_finished",
  "database"
];
var STATUS_EVENT = "run_status";

/* Regions of the run page the server re-renders and this file swaps in by
 * id. What is in them comes from the run row rather than from the event
 * list, so a run that ends under an open stream leaves them showing what
 * was true when the page was rendered -- a Stop button for a process that
 * is gone. Must stay in step with
 * kptn_server.routes.runs.REGION_EVENT_TARGETS; a test enforces it,
 * because a region nobody listens for is dropped in silence. */
var REGION_EVENTS = { run_header: "run-header", run_info: "run-info" };

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

function numberLines(list) {
  /* Lines are numbered across the whole run. The server numbers what it
   * renders; a row the stream appends arrives unnumbered, and takes the next
   * numbers after the last line on the page. */
  var numbers = list.querySelectorAll(".ln__no");
  var last = 0;
  for (var i = 0; i < numbers.length; i += 1) {
    var text = numbers[i].textContent;
    if (text) {
      last = parseInt(text, 10) || last;
    } else {
      last += 1;
      numbers[i].textContent = String(last);
    }
  }
}

function linesText(count) {
  /* The form _event.html's ``lines_text`` renders. */
  if (!count) {
    return "no output";
  }
  return count === 1 ? "1 line" : count.toLocaleString("en-US") + " lines";
}

function refreshSection(section) {
  /* A section's heading, brought up to date with what streamed into it: how
   * many lines it printed, how many warnings, and -- once its outcome has
   * been swapped in -- how it ended. */
  var body = section.querySelector(".sec__lines");
  var lines = 0;
  var rows = body ? body.querySelectorAll(".ev[data-kind='log']:not([hidden]) .ln") : [];
  lines = rows.length;
  var warnings = body ? body.querySelectorAll(".ev[data-kind='warning']").length : 0;
  section.setAttribute("data-warnings", String(warnings));
  var counter = section.querySelector("[data-sec-lines]");
  if (counter) {
    counter.setAttribute("data-sec-lines", String(lines));
    counter.textContent = linesText(lines);
  }
  var outcome = section.querySelector(".sec-h__end[data-state]");
  if (outcome) {
    section.setAttribute("data-state", outcome.getAttribute("data-state"));
  }
  /* Its first line makes a section foldable, as the page renders it. */
  var empty = !body || !body.querySelector(".ev:not([hidden]) .ln");
  var heading = section.querySelector(".sec-h");
  if (!empty && section.classList.contains("is-empty")) {
    section.classList.remove("is-empty");
    if (heading) {
      heading.removeAttribute("tabindex");
      heading.setAttribute("aria-expanded", section.classList.contains("is-closed") ? "false" : "true");
    }
  }
}

function setSectionOpen(section, open) {
  if (section.classList.contains("is-empty")) {
    return; /* nothing to fold */
  }
  section.classList.toggle("is-closed", !open);
  var heading = section.querySelector(".sec-h");
  if (heading) {
    heading.setAttribute("aria-expanded", open ? "true" : "false");
  }
  /* Folded lines stay searchable: the browser's find looks inside
   * ``hidden="until-found"`` and, on a match, removes it and fires
   * ``beforematch`` (initLogControls). A browser without it treats the
   * value as plain ``hidden``, which is how folding worked before. */
  var body = section.querySelector(".sec__lines");
  if (body) {
    if (open) {
      body.removeAttribute("hidden");
    } else {
      body.setAttribute("hidden", "until-found");
    }
  }
}

function initLogControls(console_) {
  /* The log's own controls: a section's heading folds it, Collapse all folds
   * or unfolds every section, and View hides the timestamps or the line
   * numbers -- remembered for this reader. All by delegation from the
   * document, because the run bar holding Collapse all and View is replaced
   * wholesale by the stream's closing frame. View's menu opens and closes
   * like every other (initMenus). */
  var list = document.getElementById("console-events");
  var VIEW_KEYS = { timestamps: "kptn.showTimestamps", numbers: "kptn.showLineNumbers" };
  /* Until the reader says otherwise: line numbers on, timestamps off -- the
   * numbers place a line, and the times mostly repeat one another. */
  var VIEW_DEFAULTS = { timestamps: false, numbers: true };

  function anyOpen() {
    return list.querySelector("[data-section]:not(.is-closed):not(.is-empty)") !== null;
  }

  function syncFoldAll() {
    /* The button says what it will do next, and is off while no section
     * has anything to fold. */
    var buttons = document.querySelectorAll("[data-fold-all]");
    var open = anyOpen();
    var foldable = list.querySelector("[data-section]:not(.is-empty)") !== null;
    for (var i = 0; i < buttons.length; i += 1) {
      buttons[i].disabled = !foldable;
      var label = buttons[i].querySelector("span");
      var path = buttons[i].querySelector("path");
      if (label) {
        label.textContent = open ? "Collapse all" : "Expand all";
      }
      if (path) {
        path.setAttribute("d", open ? "M7 4l5 5 5-5M7 20l5-5 5 5" : "M7 9l5-5 5 5M7 15l5 5 5-5");
      }
    }
  }

  function viewOption(name) {
    try {
      var stored = window.localStorage.getItem(VIEW_KEYS[name]);
      return stored === null ? VIEW_DEFAULTS[name] : stored === "1";
    } catch (err) {
      return VIEW_DEFAULTS[name]; /* private browsing, or storage disabled */
    }
  }

  function applyViewOptions() {
    console_.classList.toggle("no-timestamps", !viewOption("timestamps"));
    console_.classList.toggle("no-numbers", !viewOption("numbers"));
    var boxes = document.querySelectorAll("[data-view-option]");
    for (var i = 0; i < boxes.length; i += 1) {
      boxes[i].checked = viewOption(boxes[i].getAttribute("data-view-option"));
    }
  }

  console_.addEventListener("click", function (event) {
    var heading = event.target.closest(".sec-h");
    if (heading && console_.contains(heading)) {
      var section = heading.closest("[data-section]");
      setSectionOpen(section, section.classList.contains("is-closed"));
      syncFoldAll();
    }
  });

  /* The browser's find matched text in a folded section and is about to
   * reveal it: open the section properly, so its heading and Collapse all
   * agree with what is on screen. Captured, because the event is fired on
   * the lines themselves. */
  list.addEventListener(
    "beforematch",
    function (event) {
      var section = event.target.closest && event.target.closest("[data-section]");
      if (section) {
        setSectionOpen(section, true);
        syncFoldAll();
      }
    },
    true
  );

  document.addEventListener("click", function (event) {
    if (event.target.closest("[data-fold-all]")) {
      var close = anyOpen();
      var sections = list.querySelectorAll("[data-section]");
      for (var i = 0; i < sections.length; i += 1) {
        setSectionOpen(sections[i], !close);
      }
      syncFoldAll();
    }
  });

  document.addEventListener("change", function (event) {
    var box = event.target.closest && event.target.closest("[data-view-option]");
    if (!box) {
      return;
    }
    try {
      window.localStorage.setItem(VIEW_KEYS[box.getAttribute("data-view-option")], box.checked ? "1" : "0");
    } catch (err) {
      /* Not being able to remember the choice is not a reason to refuse it. */
    }
    applyViewOptions();
  });

  applyViewOptions();
  syncFoldAll();
  return { syncFoldAll: syncFoldAll, applyViewOptions: applyViewOptions };
}

/* States of a task that did not run in this run, which the "Ran" filter
 * leaves out. kptn_server.run_outline holds the same list. */
var NOT_RAN = ["cached", "waiting", "not_run", "bypassed"];

/* What the "Issues" filter shows besides a task that warned; the first two
 * turn it red. kptn_server.run_outline holds the same lists. */
var FAILED = ["failed", "interrupted"];
var ISSUES = FAILED.concat(["not_run"]);

/* The rail's filters, by a task's state and whether it warned. The log's
 * sections are filtered by the same rules, in app.css. */
var RAIL_FILTERS = {
  all: function () {
    return true;
  },
  ran: function (state) {
    return NOT_RAN.indexOf(state) === -1;
  },
  cached: function (state) {
    return state === "cached";
  },
  issues: function (state, warned) {
    return warned || ISSUES.indexOf(state) !== -1;
  }
};

/* The rail (_outline.html) beside the log. Clicking a task takes the log to
 * its section, opens it, and lights it for a moment; clicking a step's
 * heading does the same for every task beneath it, from the first. The rail
 * marks what was clicked -- not where the log is scrolled -- and clicking it
 * again clears the mark. Its parts are rendered by the server -- the stream
 * sends a step's part again whenever it changes -- so all this file does
 * with them is swap them in, keep the mark, and keep the filters.
 *
 * The filters at its head narrow the rail and the log to the tasks that
 * ran, the cached ones, or those that warned, and are the run's tallies:
 * recounted here off the rail's tasks whenever a frame changes it.
 *
 * A group's caret folds its tasks in the rail alone. The server renders
 * every group open, so the groups this reader folded are kept here and
 * folded again when the stream redraws them. */
function initRail(console_, list) {
  var rail = document.getElementById("outline");
  if (!rail) {
    return null;
  }
  var active = null; /* the clicked item's data-unit or data-units */
  var filter = "all";
  var folded = {}; /* ids of the rail-steps this reader folded */
  var reduce = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function matches(item) {
    return RAIL_FILTERS[filter](item.getAttribute("data-state"), item.hasAttribute("data-warned"));
  }

  function visibleUnits(scope) {
    var units = scope.querySelectorAll(".rail-item[data-unit]");
    var shown = 0;
    for (var i = 0; i < units.length; i += 1) {
      if (!units[i].hidden) {
        shown += 1;
      }
    }
    return shown;
  }

  function applyFilter() {
    /* The log by its own rules (app.css); the rail by hiding what does not
     * match, then each label and step left with nothing under it. */
    if (filter === "all") {
      console_.removeAttribute("data-show");
    } else {
      console_.setAttribute("data-show", filter);
    }
    var all = filter === "all";
    var units = rail.querySelectorAll(".rail-item[data-unit]");
    for (var i = 0; i < units.length; i += 1) {
      units[i].hidden = !all && !matches(units[i]);
    }
    var labels = rail.querySelectorAll(".rail-label");
    for (var k = 0; k < labels.length; k += 1) {
      var kids = labels[k].nextElementSibling;
      var empty = !all && !(kids && visibleUnits(kids));
      labels[k].hidden = empty;
      if (kids && kids.classList.contains("rail-kids")) {
        kids.hidden = empty;
      }
    }
    var steps = rail.querySelectorAll(".rail-step");
    var anyShown = false;
    for (var n = 0; n < steps.length; n += 1) {
      var shown = visibleUnits(steps[n]);
      steps[n].hidden = !all && !shown;
      anyShown = anyShown || !steps[n].hidden;
      var count = steps[n].querySelector(".rail-item__count[data-count]");
      if (count) {
        count.textContent = all ? count.getAttribute("data-count") : String(shown);
      }
    }
    var note = rail.querySelector("[data-rail-empty]");
    if (note) {
      note.hidden = all || anyShown;
    }
    var buttons = rail.querySelectorAll("[data-filter]");
    for (var b = 0; b < buttons.length; b += 1) {
      buttons[b].setAttribute("aria-pressed", buttons[b].getAttribute("data-filter") === filter ? "true" : "false");
    }
  }

  function recount() {
    /* The filters' counts, off every task in the rail: the same four the
     * server rendered (run_outline.filter_counts), and the issues' colour. */
    var units = rail.querySelectorAll(".rail-item[data-unit]");
    var buttons = rail.querySelectorAll("[data-filter]");
    var failed = false;
    for (var f = 0; f < units.length; f += 1) {
      failed = failed || FAILED.indexOf(units[f].getAttribute("data-state")) !== -1;
    }
    for (var b = 0; b < buttons.length; b += 1) {
      var test = RAIL_FILTERS[buttons[b].getAttribute("data-filter")];
      var count = 0;
      for (var i = 0; i < units.length; i += 1) {
        if (test(units[i].getAttribute("data-state"), units[i].hasAttribute("data-warned"))) {
          count += 1;
        }
      }
      var counter = buttons[b].querySelector("[data-counter]");
      if (counter) {
        counter.textContent = String(count);
      }
      /* The pressed one stays pressable, so the way back is never disabled. */
      buttons[b].disabled = count === 0 && buttons[b].getAttribute("data-filter") !== filter;
      if (buttons[b].getAttribute("data-filter") === "issues") {
        buttons[b].classList.toggle("rail-filter--err", count > 0 && failed);
        buttons[b].classList.toggle("rail-filter--warn", count > 0 && !failed);
      }
    }
  }

  function setFolded(step, fold) {
    step.classList.toggle("is-folded", fold);
    var caret = step.querySelector("[data-rail-fold]");
    if (caret) {
      caret.setAttribute("aria-expanded", fold ? "false" : "true");
      caret.title = fold ? "Expand" : "Collapse";
    }
    if (fold) {
      folded[step.id] = true;
    } else {
      delete folded[step.id];
    }
  }

  function setFilter(name) {
    if (!Object.prototype.hasOwnProperty.call(RAIL_FILTERS, name) || name === filter) {
      return;
    }
    filter = name;
    active = null;
    mark();
    clearLit();
    applyFilter();
    recount();
    console_.scrollTop = 0;
  }

  function sectionFor(unit) {
    var sections = list.querySelectorAll("[data-section]");
    for (var i = 0; i < sections.length; i += 1) {
      if (sections[i].getAttribute("data-unit") === unit) {
        return sections[i];
      }
    }
    return null;
  }

  function keyOf(item) {
    return item.hasAttribute("data-units")
      ? "units:" + item.getAttribute("data-units")
      : "unit:" + item.getAttribute("data-unit");
  }

  function mark() {
    var items = rail.querySelectorAll(".rail-item");
    for (var i = 0; i < items.length; i += 1) {
      items[i].classList.toggle("is-active", active !== null && keyOf(items[i]) === active);
    }
  }

  function clearLit() {
    var lit = list.querySelectorAll(".sec.is-selected");
    for (var i = 0; i < lit.length; i += 1) {
      lit[i].classList.remove("is-selected");
    }
  }

  function select(item) {
    var units = item.hasAttribute("data-units")
      ? item.getAttribute("data-units").split(" ")
      : [item.getAttribute("data-unit")];
    var sections = [];
    for (var i = 0; i < units.length; i += 1) {
      var section = units[i] ? sectionFor(units[i]) : null;
      /* A section the filter hides has no place to scroll to. */
      if (section && section.getClientRects().length) {
        sections.push(section);
      }
    }
    active = keyOf(item);
    mark();
    clearLit();
    if (!sections.length) {
      return; /* nothing of it in the log yet */
    }
    for (var k = 0; k < sections.length; k += 1) {
      setSectionOpen(sections[k], true);
    }
    console_.scrollTo({
      top: sections[0].offsetTop - list.offsetTop,
      behavior: reduce ? "auto" : "smooth"
    });
    void list.offsetWidth; /* restart the light on a second click */
    for (var n = 0; n < sections.length; n += 1) {
      sections[n].classList.add("is-selected");
    }
  }

  rail.addEventListener("click", function (event) {
    var button = event.target.closest("[data-filter]");
    if (button && rail.contains(button)) {
      setFilter(button.getAttribute("data-filter"));
      return;
    }
    var caret = event.target.closest("[data-rail-fold]");
    if (caret && rail.contains(caret)) {
      var step = caret.closest(".rail-step");
      setFolded(step, !step.classList.contains("is-folded"));
      return;
    }
    var item = event.target.closest(".rail-item");
    if (!item || !rail.contains(item)) {
      return;
    }
    if (active !== null && keyOf(item) === active) {
      active = null;
      mark();
      clearLit();
      return;
    }
    select(item);
  });

  list.addEventListener("animationend", function (event) {
    if (event.animationName === "sec-selected-head") {
      var section = event.target.closest(".sec");
      if (section) {
        section.classList.remove("is-selected");
      }
    }
  });

  /* A finished run opens on its failure. */
  var opening = rail.getAttribute("data-selected");
  if (opening && console_.getAttribute("data-terminal") === "true") {
    var items = rail.querySelectorAll(".rail-item[data-unit]");
    for (var i = 0; i < items.length; i += 1) {
      if (items[i].getAttribute("data-unit") === opening) {
        select(items[i]);
        break;
      }
    }
  }

  return {
    /* A frame's part of the rail, swapped in, with the mark and the filter
     * kept and the counts brought up to date. */
    update: function (frame) {
      if (!frame.outline) {
        return;
      }
      var part = document.getElementById(frame.outline.id);
      if (part) {
        part.replaceWith(fragmentFrom(frame.outline.html));
        var redrawn = document.getElementById(frame.outline.id);
        if (redrawn && folded[frame.outline.id]) {
          setFolded(redrawn, true);
        }
      }
      mark();
      applyFilter();
      recount();
    },
    setFilter: setFilter
  };
}

/* The bar's sidebar button hides the sidebar and gives the main area the
 * whole width, remembered for this reader. Hiding it also drops the rail's
 * filter: the filter's buttons live in the sidebar, and a log narrowed out
 * of sight would look like a log with output missing. By delegation,
 * because the stream's closing frame replaces the run bar; ``sync`` puts
 * the replacement button in step. */
function initSidebar() {
  var body = document.querySelector(".run-body");
  if (!body) {
    return null;
  }

  function stored() {
    try {
      return window.localStorage.getItem(SIDEBAR_STORAGE_KEY) !== "0";
    } catch (err) {
      return true;
    }
  }

  function sync() {
    var open = !body.classList.contains("no-side");
    var toggles = document.querySelectorAll("[data-side-toggle]");
    for (var i = 0; i < toggles.length; i += 1) {
      var label = open ? "Hide sidebar" : "Show sidebar";
      toggles[i].setAttribute("aria-expanded", open ? "true" : "false");
      toggles[i].setAttribute("aria-label", label);
      toggles[i].title = label;
    }
  }

  function set(open) {
    body.classList.toggle("no-side", !open);
    if (!open && RAIL) {
      RAIL.setFilter("all");
    }
    sync();
  }

  document.addEventListener("click", function (event) {
    if (!event.target.closest("[data-side-toggle]")) {
      return;
    }
    var open = body.classList.contains("no-side");
    set(open);
    try {
      window.localStorage.setItem(SIDEBAR_STORAGE_KEY, open ? "1" : "0");
    } catch (err) {
      /* Not being able to remember the choice is not a reason to refuse it. */
    }
  });

  set(stored());
  return { sync: sync };
}

/* The sidebar's panes -- Next run, Runs, Run -- fold by their headers, like
 * VS Code's sections. The server renders them open; the ones this reader
 * folded are remembered here, by name, and folded again on every page. */
function initPanes() {
  var panes = document.querySelectorAll("[data-pane]");
  if (!panes.length) {
    return;
  }
  var closed = [];
  try {
    closed = JSON.parse(window.localStorage.getItem(PANES_STORAGE_KEY) || "[]");
  } catch (err) {
    closed = [];
  }
  if (!Array.isArray(closed)) {
    closed = [];
  }

  function set(pane, close) {
    pane.classList.toggle("is-closed", close);
    var toggle = pane.querySelector("[data-pane-toggle]");
    if (toggle) {
      toggle.setAttribute("aria-expanded", close ? "false" : "true");
    }
  }

  for (var i = 0; i < panes.length; i += 1) {
    set(panes[i], closed.indexOf(panes[i].getAttribute("data-pane")) !== -1);
  }

  document.addEventListener("click", function (event) {
    var toggle = event.target.closest("[data-pane-toggle]");
    if (!toggle) {
      return;
    }
    var pane = toggle.closest("[data-pane]");
    var name = pane.getAttribute("data-pane");
    var close = !pane.classList.contains("is-closed");
    set(pane, close);
    closed = closed.filter(function (other) {
      return other !== name;
    });
    if (close) {
      closed.push(name);
    }
    try {
      window.localStorage.setItem(PANES_STORAGE_KEY, JSON.stringify(closed));
    } catch (err) {
      /* Not being able to remember the choice is not a reason to refuse it. */
    }
  });
}

/* The Runs pane's list (_run_list.html), fetched again whole when a run
 * starts or ends -- so a run started in another tab, or this page's run
 * finishing, shows up without a reload -- with the pane header's count. */
function initRunList() {
  var list = document.getElementById("run-list");
  if (!list) {
    return;
  }
  /* The page's own run, in view however far down the list it is. */
  var shown = list.querySelector('[aria-current="page"]');
  if (shown) {
    shown.scrollIntoView({ block: "nearest" });
  }
  if (typeof window.fetch !== "function") {
    return;
  }
  var url = list.getAttribute("data-url");

  function load() {
    window
      .fetch(url, { headers: { "HX-Request": "true" } })
      .then(function (response) {
        return response.ok ? response.text() : null;
      })
      .then(function (html) {
        if (html === null) {
          return;
        }
        var current = document.getElementById("run-list");
        var fresh = fragmentFrom(html).querySelector("#run-list");
        if (!current || !fresh) {
          return;
        }
        current.replaceWith(fresh);
        var count = fresh.closest("[data-pane]").querySelector("[data-pane-count]");
        if (count) {
          var n = fresh.getAttribute("data-count");
          count.textContent = n === "0" ? "" : n;
        }
      })
      .catch(function () {
        /* A server restarting under the page: keep the list it has. */
      });
  }

  document.addEventListener(RUNS_CHANGED, load);
}

/* Dropdown menus (``[data-menu]``): one open at a time, closed by Escape --
 * which hands focus back to its button -- by a click outside, and by
 * choosing one of its items. By delegation, so a run bar the stream
 * replaced keeps working. */
function initMenus() {
  function close(except) {
    var wraps = document.querySelectorAll("[data-menu]");
    for (var i = 0; i < wraps.length; i += 1) {
      if (wraps[i] === except) {
        continue;
      }
      var panel = wraps[i].querySelector(".menu");
      var toggle = wraps[i].querySelector("[data-menu-toggle]");
      if (panel) {
        panel.hidden = true;
      }
      if (toggle) {
        toggle.setAttribute("aria-expanded", "false");
      }
    }
  }

  document.addEventListener("click", function (event) {
    var toggle = event.target.closest("[data-menu-toggle]");
    if (toggle) {
      var wrap = toggle.closest("[data-menu]");
      var panel = wrap && wrap.querySelector(".menu");
      if (!panel) {
        return;
      }
      var open = panel.hidden;
      close(wrap);
      panel.hidden = !open;
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
      if (open) {
        var items = panel.querySelectorAll("input, .menu-item");
        for (var i = 0; i < items.length; i += 1) {
          if (items[i].offsetParent !== null && !items[i].disabled) {
            items[i].focus();
            break;
          }
        }
      }
      return;
    }
    if (event.target.closest(".menu-item") || !event.target.closest("[data-menu]")) {
      close(null);
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") {
      return;
    }
    var panels = document.querySelectorAll("[data-menu] .menu:not([hidden])");
    if (!panels.length) {
      return;
    }
    var toggle = panels[0].closest("[data-menu]").querySelector("[data-menu-toggle]");
    close(null);
    if (toggle) {
      toggle.focus();
    }
  });
}

/* "Copy link to run": the page's own address, without any fragment. The
 * clipboard API needs a secure context, which loopback is; a page reached
 * some other way falls back to a hidden field and the copy command. The
 * item says it worked for a moment. */
function initCopyLink() {
  function fallback(text) {
    var field = document.createElement("textarea");
    field.value = text;
    field.setAttribute("readonly", "");
    field.style.position = "fixed";
    field.style.opacity = "0";
    document.body.appendChild(field);
    field.select();
    var copied = false;
    try {
      copied = document.execCommand("copy");
    } catch (err) {
      copied = false;
    }
    field.remove();
    return copied ? Promise.resolve() : Promise.reject(new Error("copy refused"));
  }

  document.addEventListener("click", function (event) {
    var item = event.target.closest("[data-copy-link]");
    if (!item) {
      return;
    }
    var link = window.location.href.split("#")[0];
    var copying =
      navigator.clipboard && window.isSecureContext
        ? navigator.clipboard.writeText(link)
        : fallback(link);
    copying.then(
      function () {
        var label = item.querySelector("span");
        if (!label) {
          return;
        }
        label.textContent = "Link copied";
        window.setTimeout(function () {
          label.textContent = "Copy link to run";
        }, 1500);
      },
      function () {
        /* Refused: leave the address bar as the way to the link. */
      }
    );
  });
}

/* Light, dark, or the system's -- chosen in Settings, kept in this browser,
 * and applied to <html> as ``data-theme``. _theme.html applies the stored
 * choice before a page paints; this applies a new one at once. */
function initTheme() {
  var options = document.querySelectorAll("[data-theme-option]");
  var current = "system";
  try {
    var stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "light" || stored === "dark") {
      current = stored;
    }
  } catch (err) {
    /* Private browsing, or storage disabled. The system's choice stands. */
  }
  for (var i = 0; i < options.length; i += 1) {
    options[i].checked = options[i].value === current;
    options[i].addEventListener("change", function (event) {
      var choice = event.currentTarget.value;
      var root = document.documentElement;
      if (choice === "light" || choice === "dark") {
        root.setAttribute("data-theme", choice);
      } else {
        root.removeAttribute("data-theme");
      }
      try {
        if (choice === "light" || choice === "dark") {
          window.localStorage.setItem(THEME_STORAGE_KEY, choice);
        } else {
          window.localStorage.removeItem(THEME_STORAGE_KEY);
        }
      } catch (err) {
        /* Not being able to remember the choice is not a reason to refuse it. */
      }
    });
  }
}

/* The Next run pane's state card (_next_start.html): what the next run
 * starts from. Server-rendered and fetched whole -- once the page is up,
 * again when a run starts or ends, and as the answer to a delete --
 * so the card always says what kptn says, never what this file assumes.
 *
 * Its trash takes two clicks: the first arms it ("Delete?"), a second
 * within three seconds deletes, and it disarms when the time runs out or
 * focus moves away. The delete posts the form the server rendered, and the
 * card that comes back is the next starting point. */
var ARM_MS = 3000;

function initNextStart() {
  var slot = document.getElementById("next-start");
  if (!slot || typeof window.fetch !== "function") {
    return null;
  }
  var armTimer = null;

  function profile() {
    var select = document.getElementById("profile-select");
    return select ? select.value : "";
  }

  /* The pane's card, lit for a moment when a delete hands it on. */
  var lit = slot.closest(".next") || slot;

  function paint(html, swapped) {
    slot.replaceChildren(fragmentFrom(html));
    var card = slot.querySelector(".statecard");
    slot.hidden = !card;
    if (card && swapped) {
      lit.classList.remove("swap");
      void lit.offsetWidth;
      lit.classList.add("swap");
    }
  }

  function load() {
    var url = slot.getAttribute("data-url");
    var chosen = profile();
    window
      .fetch(chosen ? url + "?profile=" + encodeURIComponent(chosen) : url, {
        headers: { "HX-Request": "true" }
      })
      .then(function (response) {
        return response.ok ? response.text() : null;
      })
      .then(function (html) {
        if (html !== null) {
          paint(html, false);
        }
      })
      .catch(function () {
        /* A server restarting under the page: keep the card it has. */
      });
  }

  function label(button, armed) {
    var what = button.getAttribute("data-what");
    var text = armed ? "Click again to delete " + what : "Delete " + what;
    button.setAttribute("aria-label", text);
    button.title = armed ? "Click again to delete" : text;
  }

  function disarm() {
    window.clearTimeout(armTimer);
    var armed = slot.querySelectorAll("[data-st-delete].armed");
    for (var i = 0; i < armed.length; i += 1) {
      armed[i].classList.remove("armed");
      label(armed[i], false);
    }
  }

  function remove(button) {
    var form = button.closest("form");
    button.disabled = true;
    window
      .fetch(form.getAttribute("action"), {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "HX-Request": "true"
        },
        body: new URLSearchParams(new FormData(form)).toString()
      })
      .then(function (response) {
        /* Refused (409) still answers with the card as it is now. */
        return response.text();
      })
      .then(function (html) {
        paint(html, true);
        var next = slot.querySelector("[data-st-delete]");
        if (next) {
          next.focus();
        }
      })
      .catch(function () {
        button.disabled = false;
        load();
      });
  }

  slot.addEventListener("click", function (event) {
    var button = event.target.closest("[data-st-delete]");
    if (!button) {
      return;
    }
    event.preventDefault();
    if (!button.classList.contains("armed")) {
      button.classList.add("armed");
      label(button, true);
      window.clearTimeout(armTimer);
      armTimer = window.setTimeout(disarm, ARM_MS);
      return;
    }
    disarm();
    remove(button);
  });
  slot.addEventListener("focusout", disarm);
  lit.addEventListener("animationend", function (event) {
    event.target.classList.remove("swap");
  });

  /* A run may have made the database, or a checkpoint. */
  document.addEventListener(RUNS_CHANGED, load);

  load();
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
  if (!console_) {
    return;
  }

  var list = document.getElementById("console-events");
  var empty = document.getElementById("console-empty");
  var controls = initLogControls(console_);
  var rail = initRail(console_, list);
  RAIL = rail;
  if (console_.getAttribute("data-terminal") === "true" || typeof window.EventSource !== "function") {
    /* Already over. Its whole history is on the page, and its status came
     * from the run row -- there is nothing left to stream, and no ending
     * left to hear. */
    return;
  }
  unlockSoundOnFirstGesture();

  /* The log scrolls on its own, beside the rail: following the output means
   * keeping its newest line in view. */
  var autoScroll = startAutoScroll(console_);

  var source = null;
  var closed = false;

  /* Rows from the stream wait here and are added once per animation frame.
   * Following the output means scrolling to the bottom, and reading the
   * log's height forces a layout of the whole of it: per row, that was
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
    /* Each frame says where its row goes: the section to open first, the
     * element to append to, and a finished task's heading to update. All of
     * it was decided by the server; this only finds the ids. */
    var touched = [];
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
      if (frame.outcome) {
        var outcome = document.getElementById(frame.outcome.id);
        if (outcome) {
          outcome.replaceWith(fragmentFrom(frame.outcome.html));
        }
      }
      var section = parent.closest("[data-section]");
      if (section && touched.indexOf(section) === -1) {
        touched.push(section);
      }
      if (rail) {
        rail.update(frame);
      }
    }
    queued = [];
    queuedSequences = {};
    if (empty) {
      empty.remove();
      empty = null;
    }
    for (var t = 0; t < touched.length; t += 1) {
      refreshSection(touched[t]);
    }
    numberLines(list);
    controls.syncFoldAll();
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
      /* The new run bar's controls show this reader's choices. */
      controls.applyViewOptions();
      controls.syncFoldAll();
      if (SIDEBAR) {
        SIDEBAR.sync();
      }
    }
  }

  function applyStatus(event) {
    flushQueued();
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
      announceRunEnd(frame.status);
      document.dispatchEvent(new CustomEvent(RUNS_CHANGED));
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
  /* Choosing a profile in the app bar shows that profile: its newest run,
   * with its runs in the sidebar -- or, on a page about a profile (the
   * plan, the walkthrough), the same page for the new one.
   * ``data-profile-nav`` is where, rendered by the page. Blank is "(no
   * profile)", sent as such: the landing page reads no ``profile`` at all
   * as "the newest run of any".
   *
   * With JavaScript off the selector still chooses what Run and Plan
   * submit; this only adds the navigation. */
  var select = document.getElementById("profile-select");
  if (!select) {
    return;
  }

  select.addEventListener("change", function () {
    var target = select.getAttribute("data-profile-nav");
    if (target) {
      window.location.assign(target + "?profile=" + encodeURIComponent(select.value));
    }
  });

  /* Back to a page the browser kept: it shows the profile it was rendered
   * with, not the one chosen on the way out. */
  window.addEventListener("pageshow", function (event) {
    if (!event.persisted) {
      return;
    }
    for (var i = 0; i < select.options.length; i += 1) {
      if (select.options[i].defaultSelected) {
        select.value = select.options[i].value;
      }
    }
  });
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
  /* The run holding the lock when the page was rendered; another one --
   * or none -- means a run started or ended. */
  var seen = busy ? busy.getAttribute("data-active-run") || "" : "";

  function apply(state) {
    var holder = state.run_id || "";
    if (holder !== seen) {
      seen = holder;
      document.dispatchEvent(new CustomEvent(RUNS_CHANGED));
    }
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

initTheme();
initSettings();
initMenus();
initCopyLink();
SIDEBAR = initSidebar();
initPanes();
initRunList();
initNextStart();
initConsole();
initProfileNav();
initRunLock();
initProjectPicker();
