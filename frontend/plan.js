/* Plan view: album -> tok_name -> groups -> lessons (week). No framework. */

const $ = (sel) => document.querySelector(sel);

const START_HOUR = 7;
const END_HOUR = 21;

const apiBaseEl = $("#apiBase");
const apiStatusEl = $("#apiStatus");
const workPillEl = $("#workPill");
const workTextEl = $("#workText");
const toastEl = $("#toast");
const weekHintEl = $("#weekHint");

const albumEl = $("#albumNumber");
const majorsEl = $("#majorsCount");
const weekStartEl = $("#weekStart");
const rangeStartEl = $("#rangeStart");
const rangeEndEl = $("#rangeEnd");
const maxWorkersEl = $("#maxWorkers");

const btnPrevWeekEl = $("#btnPrevWeek");
const btnNextWeekEl = $("#btnNextWeek");
const btnLoadEl = $("#btnLoad");
const btnRefreshEl = $("#btnRefresh");
const statusTextEl = $("#statusText");

const groupListEl = $("#groupList");
const tooltipEl = $("#tooltip");

const hMon = $("#hMon");
const hTue = $("#hTue");
const hWed = $("#hWed");
const hThu = $("#hThu");
const hFri = $("#hFri");

const state = {
  ready: false,
  rawEvents: [],
  activeFilters: new Set(),
  seenFilterKeys: new Set(),
  filtersTouched: false,
  subjectToKeys: new Map(), // subjectBase -> Set<filterKey>
  formToKeys: new Map(), // `${subjectBase}||${formTitle}` -> Set<filterKey>
  working: false,
};

function setStatus(msg) {
  statusTextEl.textContent = msg || ".";
}

let toastTimer = null;
function toast(msg) {
  if (!toastEl) return;
  if (toastTimer) clearTimeout(toastTimer);
  toastEl.textContent = msg;
  toastEl.hidden = false;
  toastTimer = setTimeout(() => {
    toastEl.hidden = true;
  }, 1700);
}

function setApiStatus(ok) {
  if (!apiStatusEl) return;
  apiStatusEl.textContent = ok ? "API OK" : "API DOWN";
  apiStatusEl.style.borderColor = ok ? "rgba(22,123,63,0.25)" : "rgba(180,35,24,0.25)";
  apiStatusEl.style.background = ok ? "rgba(22,123,63,0.14)" : "rgba(180,35,24,0.14)";
  apiStatusEl.style.color = ok ? "rgba(22,123,63,1)" : "rgba(180,35,24,1)";
}

function setWorking(working, label = null) {
  state.working = !!working;
  if (workPillEl) workPillEl.dataset.working = working ? "1" : "0";
  if (workTextEl) workTextEl.textContent = label || (working ? "working" : "idle");
}

async function fetchJson(path, opts = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), opts.timeoutMs || 12000);
  try {
    const res = await fetch(path, {
      ...opts,
      headers: {
        "content-type": "application/json",
        ...(opts.headers || {}),
      },
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      let detail = "";
      if (ct.includes("application/json")) {
        const j = await res.json().catch(() => null);
        if (j && typeof j === "object") {
          if ("detail" in j) {
            const d = j.detail;
            detail = typeof d === "string" ? d : JSON.stringify(d);
          } else {
            detail = JSON.stringify(j);
          }
        }
      }
      if (!detail) {
        const text = await res.text().catch(() => "");
        detail = text || res.statusText;
      }
      throw new Error(`HTTP ${res.status}: ${detail}`);
    }
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

async function ensureHealth() {
  try {
    await fetchJson("/api/health", { timeoutMs: 5000 });
    setApiStatus(true);
  } catch (_) {
    setApiStatus(false);
  }
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatYMDLocal(d) {
  // Local date -> YYYY-MM-DD
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

function parseYMD(s) {
  const [y, m, d] = (s || "").split("-").map((x) => Number(x));
  if (!y || !m || !d) return null;
  return { y, m, d };
}

function ymdToUTCDate({ y, m, d }) {
  return new Date(Date.UTC(y, m - 1, d));
}

function utcDateToYMD(d) {
  return d.toISOString().slice(0, 10);
}

function endExclusiveLocalIsoToInclusiveYMD(endLocalIso) {
  const s = String(endLocalIso || "");
  const ymd = parseYMD(s.slice(0, 10));
  if (!ymd) return s.slice(0, 10);

  // If backend returns an exclusive boundary at midnight, display the previous day as inclusive end.
  if (s.endsWith("T00:00:00")) {
    const d = ymdToUTCDate(ymd);
    d.setUTCDate(d.getUTCDate() - 1);
    return utcDateToYMD(d);
  }
  return utcDateToYMD(ymdToUTCDate(ymd));
}

function mondayFromYMD(ymd) {
  const d = ymdToUTCDate(ymd);
  let dow = d.getUTCDay(); // 0=Sun..6=Sat
  if (dow === 0) dow = 7;
  d.setUTCDate(d.getUTCDate() - (dow - 1));
  return d;
}

function addDaysUTC(d, days) {
  const out = new Date(d.getTime());
  out.setUTCDate(out.getUTCDate() + days);
  return out;
}

function setWeekHeaders(mondayUTC) {
  const names = ["Poniedziałek", "Wtorek", "Środa", "Czwartek", "Piątek"];
  const els = [hMon, hTue, hWed, hThu, hFri];
  for (let i = 0; i < 5; i++) {
    const day = addDaysUTC(mondayUTC, i);
    els[i].textContent = `${names[i]} ${utcDateToYMD(day)}`;
  }
}

function stripLastParen(title) {
  const t = String(title || "").trim();
  if (!t) return "";
  return t.replace(/\s*\([^)]*\)\s*$/, "").trim();
}

function parseLocalIsoToParts(iso) {
  // expected: YYYY-MM-DDTHH:MM(:SS)
  const [datePart, timePartRaw] = String(iso || "").split("T");
  if (!datePart || !timePartRaw) return null;
  const [y, m, d] = datePart.split("-").map((x) => Number(x));
  const timePart = timePartRaw.trim();
  const [hh, mm, ss] = timePart.split(":").map((x) => Number(x));
  if (!y || !m || !d || Number.isNaN(hh) || Number.isNaN(mm)) return null;
  return { y, m, d, hh: hh || 0, mm: mm || 0, ss: ss || 0 };
}

function dayOfWeekFromYMD(y, m, d) {
  const utc = new Date(Date.UTC(y, m - 1, d));
  let dow = utc.getUTCDay();
  if (dow === 0) dow = 7;
  return dow; // 1..7
}

function formatTime(decimalTime) {
  const hrs = Math.floor(decimalTime);
  const mins = Math.round((decimalTime - hrs) * 60);
  return `${hrs}:${mins < 10 ? "0" + mins : mins}`;
}

function buildRawEventsFromLessons(lessons) {
  const out = [];
  for (const ev of lessons || []) {
    const title = String(ev.title || "Bez nazwy");
    const base = stripLastParen(title) || String(ev.subject || "").trim() || title;
    const formTitle = title;
    const group = String(ev.group_name || "").trim();
    const lecturer = String(ev.worker || ev.worker_title || "").trim() || "Brak prowadzącego";
    const room = String(ev.room || "").trim();

    const startParts = parseLocalIsoToParts(ev.start);
    const endParts = parseLocalIsoToParts(ev.end);
    if (!startParts || !endParts) continue;

    const day = dayOfWeekFromYMD(startParts.y, startParts.m, startParts.d);
    const startMetric = startParts.hh + startParts.mm / 60;
    const endMetric = endParts.hh + endParts.mm / 60;
    const durationMetric = endMetric - startMetric;

    const filterKey = `${base}|${formTitle}|${group}|${lecturer}`;

    out.push({
      base,
      formTitle,
      title,
      lecturer,
      room,
      type: String(ev.lesson_form || "").trim(),
      group,
      day,
      startMetric,
      endMetric,
      durationMetric,
      color: String(ev.color || "").trim() || null,
      filterKey,
      raw: ev,
    });
  }
  return out;
}

function rebuildSidebarData(rawEvents) {
  // subject -> form -> Set<optionLabel>
  const tree = new Map();
  const optionToKey = new Map(); // `${base}||${formTitle}||${optionLabel}` -> filterKey
  const allKeys = new Set();

  for (const e of rawEvents) {
    const base = e.base;
    const form = e.formTitle;
    const optionLabel = `${e.group} | ${e.lecturer}`;
    const formKey = `${base}||${form}`;
    const optKey = `${base}||${form}||${optionLabel}`;

    if (!tree.has(base)) tree.set(base, new Map());
    const fm = tree.get(base);
    if (!fm.has(form)) fm.set(form, new Set());
    fm.get(form).add(optionLabel);

    optionToKey.set(optKey, e.filterKey);
    allKeys.add(e.filterKey);
  }

  // Update selection:
  // - before user touches filters: default to "everything on" for currently visible keys
  // - after user touches filters: keep prior state, but auto-enable brand new keys so new subjects don't disappear
  for (const k of allKeys) {
    const isNew = !state.seenFilterKeys.has(k);
    if (!state.filtersTouched || isNew) state.activeFilters.add(k);
    state.seenFilterKeys.add(k);
  }

  // Build helper maps for subject/form toggles.
  state.subjectToKeys = new Map();
  state.formToKeys = new Map();
  for (const base of tree.keys()) {
    const subjectKeys = new Set();
    const fm = tree.get(base);
    for (const form of fm.keys()) {
      const fk = `${base}||${form}`;
      const formKeys = new Set();
      for (const optLabel of fm.get(form)) {
        const k = optionToKey.get(`${base}||${form}||${optLabel}`);
        if (k) {
          formKeys.add(k);
          subjectKeys.add(k);
        }
      }
      state.formToKeys.set(fk, formKeys);
    }
    state.subjectToKeys.set(base, subjectKeys);
  }

  return { tree, optionToKey, allKeys };
}

function isAllSelected(keys) {
  for (const k of keys) {
    if (!state.activeFilters.has(k)) return false;
  }
  return keys.size > 0;
}

function isAnySelected(keys) {
  for (const k of keys) {
    if (state.activeFilters.has(k)) return true;
  }
  return false;
}

function renderSidebar(tree, optionToKey) {
  groupListEl.innerHTML = "";
  const subjects = Array.from(tree.keys()).sort((a, b) => a.localeCompare(b, "pl"));
  if (subjects.length === 0) {
    const li = document.createElement("li");
    li.className = "muted";
    li.textContent = "Brak zajęć w tym tygodniu (albo nic nie jest zaznaczone w filtrze).";
    groupListEl.appendChild(li);
    return;
  }

  for (const base of subjects) {
    const subjectLi = document.createElement("li");

    const subjectHead = document.createElement("div");
    subjectHead.className = "subject-head";

    const titleSpan = document.createElement("span");
    titleSpan.textContent = base;
    titleSpan.style.flex = "1";

    const subjectCb = document.createElement("input");
    subjectCb.type = "checkbox";
    subjectCb.dataset.subject = base;
    const subjKeys = state.subjectToKeys.get(base) || new Set();
    subjectCb.checked = isAllSelected(subjKeys);
    subjectCb.indeterminate = !subjectCb.checked && isAnySelected(subjKeys);
    subjectCb.title = "Zaznacz/Odznacz wszystkie formy tego przedmiotu";
    subjectCb.addEventListener("change", () => {
      state.filtersTouched = true;
      const on = subjectCb.checked;
      for (const k of subjKeys) {
        if (on) state.activeFilters.add(k);
        else state.activeFilters.delete(k);
      }
      renderEverything();
    });

    subjectHead.appendChild(titleSpan);
    subjectHead.appendChild(subjectCb);
    subjectLi.appendChild(subjectHead);

    const formsMap = tree.get(base);
    const forms = Array.from(formsMap.keys()).sort((a, b) => a.localeCompare(b, "pl"));
    for (const formTitle of forms) {
      const formHead = document.createElement("div");
      formHead.className = "form-head";

      const formSpan = document.createElement("span");
      formSpan.textContent = formTitle;
      formSpan.style.flex = "1";

      const formCb = document.createElement("input");
      formCb.type = "checkbox";
      formCb.dataset.subject = base;
      formCb.dataset.form = formTitle;
      const formKeys = state.formToKeys.get(`${base}||${formTitle}`) || new Set();
      formCb.checked = isAllSelected(formKeys);
      formCb.indeterminate = !formCb.checked && isAnySelected(formKeys);
      formCb.title = "Zaznacz/Odznacz wszystkie grupy dla tej formy";
      formCb.addEventListener("change", () => {
        state.filtersTouched = true;
        const on = formCb.checked;
        for (const k of formKeys) {
          if (on) state.activeFilters.add(k);
          else state.activeFilters.delete(k);
        }
        renderEverything();
      });

      formHead.appendChild(formSpan);
      formHead.appendChild(formCb);
      subjectLi.appendChild(formHead);

      const ul = document.createElement("ul");
      ul.className = "opt-list";

      const options = Array.from(formsMap.get(formTitle)).sort((a, b) => a.localeCompare(b, "pl"));
      for (const optLabel of options) {
        const li = document.createElement("li");
        li.className = "opt-item";

        const filterKey = optionToKey.get(`${base}||${formTitle}||${optLabel}`);
        if (!filterKey) continue;

        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = filterKey;
        cb.checked = state.activeFilters.has(filterKey);
        cb.addEventListener("change", () => {
          state.filtersTouched = true;
          if (cb.checked) state.activeFilters.add(filterKey);
          else state.activeFilters.delete(filterKey);
          renderEverything();
        });

        const label = document.createElement("label");
        label.textContent = optLabel;
        label.style.cursor = "pointer";
        label.addEventListener("click", () => cb.click());

        li.appendChild(cb);
        li.appendChild(label);
        ul.appendChild(li);
      }
      subjectLi.appendChild(ul);
    }

    groupListEl.appendChild(subjectLi);
  }
}

function clearEvents() {
  document.querySelectorAll(".event-block").forEach((el) => el.remove());
}

function renderEventsOnGrid() {
  clearEvents();

  const visible = state.rawEvents.filter((e) => state.activeFilters.has(e.filterKey));

  // Merge duplicates (lecture shared across multiple groups etc.)
  const mergedMap = new Map();
  for (const row of visible) {
    const uniqueKey = `${row.day}-${row.startMetric}-${row.endMetric}-${row.room}-${row.title}-${row.lecturer}`;
    if (mergedMap.has(uniqueKey)) {
      const existing = mergedMap.get(uniqueKey);
      existing.groupsSet.add(row.group);
    } else {
      mergedMap.set(uniqueKey, { ...row, groupsSet: new Set([row.group]) });
    }
  }

  const mergedEvents = Array.from(mergedMap.values());

  for (let day = 1; day <= 5; day++) {
    const dayEvents = mergedEvents.filter((e) => e.day === day);
    if (dayEvents.length === 0) continue;

    dayEvents.sort((a, b) => a.startMetric - b.startMetric);

    const columns = [];
    for (const ev of dayEvents) {
      let placed = false;
      for (let i = 0; i < columns.length; i++) {
        const col = columns[i];
        const last = col[col.length - 1];
        if (ev.startMetric >= last.endMetric) {
          col.push(ev);
          ev.colIndex = i;
          placed = true;
          break;
        }
      }
      if (!placed) {
        columns.push([ev]);
        ev.colIndex = columns.length - 1;
      }
    }

    for (const ev of dayEvents) {
      const colliding = dayEvents.filter(
        (other) => other !== ev && !(other.endMetric <= ev.startMetric || other.startMetric >= ev.endMetric)
      );
      const cluster = [...colliding, ev];
      const maxColIndex = Math.max(...cluster.map((e) => e.colIndex));
      const totalCols = maxColIndex + 1;

      const widthPercent = 100 / totalCols;
      const leftPercent = ev.colIndex * widthPercent;
      drawEventElement(ev, widthPercent, leftPercent);
    }
  }
}

function drawEventElement(event, width, left) {
  const column = document.querySelector(`.day-column[data-day="${event.day}"]`);
  if (!column) return;

  const div = document.createElement("div");
  div.className = "event-block";

  const topPos = (event.startMetric - START_HOUR) * 60;
  const height = event.durationMetric * 60;

  div.style.top = `${topPos}px`;
  div.style.height = `${height}px`;
  div.style.width = `calc(${width}% - 4px)`;
  div.style.left = `${left}%`;
  div.style.backgroundColor = event.color ? event.color : "#4a90e2";

  const groupsArray = Array.from(event.groupsSet || new Set([event.group])).sort((a, b) => a.localeCompare(b, "pl"));
  const groupsString = groupsArray.join(", ");

  div.innerHTML = `
    <div class="event-title">${escapeHtml(event.title)}</div>
    <div class="event-details">${escapeHtml(event.room)}<br>${escapeHtml(event.lecturer)}</div>
    <div class="event-groups" title="${escapeHtml(groupsString)}">Gr: ${escapeHtml(groupsString)}</div>
  `;

  div.addEventListener("mouseenter", (e) => showTooltip(e, event, groupsString));
  div.addEventListener("mousemove", (e) => moveTooltip(e));
  div.addEventListener("mouseleave", hideTooltip);

  column.appendChild(div);
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function showTooltip(e, data, groupsString) {
  tooltipEl.style.display = "block";
  tooltipEl.innerHTML = `
    <strong>${escapeHtml(data.title)}</strong>
    Typ: ${escapeHtml(data.type)}<br>
    Prowadzący: ${escapeHtml(data.lecturer)}<br>
    Sala: ${escapeHtml(data.room)}<br>
    <strong>Grupy:</strong> ${escapeHtml(groupsString)}<br>
    Czas: ${escapeHtml(formatTime(data.startMetric))} - ${escapeHtml(formatTime(data.endMetric))}
  `;
  moveTooltip(e);
}

function moveTooltip(e) {
  const x = e.pageX + 15;
  const y = e.pageY + 15;
  if (x + 340 > window.innerWidth) {
    tooltipEl.style.left = x - 355 + "px";
  } else {
    tooltipEl.style.left = x + "px";
  }
  tooltipEl.style.top = y + "px";
}

function hideTooltip() {
  tooltipEl.style.display = "none";
}

function renderEverything() {
  const { tree, optionToKey } = rebuildSidebarData(state.rawEvents);
  renderSidebar(tree, optionToKey);
  renderEventsOnGrid();
}

function seedTimeColumn() {
  const timeCol = $("#timeColumn");
  timeCol.innerHTML = "";
  for (let h = START_HOUR; h <= END_HOUR; h++) {
    const div = document.createElement("div");
    div.className = "time-slot";
    div.textContent = `${h}:00`;
    timeCol.appendChild(div);
  }
}

function disableUi(disabled, label = null) {
  btnPrevWeekEl.disabled = disabled;
  btnNextWeekEl.disabled = disabled;
  btnLoadEl.disabled = disabled;
  btnRefreshEl.disabled = disabled;
  setWorking(disabled, label);
}

async function ensureStudent({ force }) {
  const album = (albumEl.value || "").trim();
  const majorsCount = Number(majorsEl.value || 1);
  const weekStart = weekStartEl.value || null;
  const rangeStart = rangeStartEl.value || null;
  const rangeEnd = rangeEndEl.value || null;
  const maxWorkers = Number(maxWorkersEl.value || 10);

  if (!album) throw new Error("Podaj numer albumu.");
  if (!Number.isFinite(majorsCount) || majorsCount < 1) throw new Error("Nieprawidłowa liczba kierunków.");

  const payload = {
    album_number: album,
    majors_count: majorsCount,
    week_start: weekStart,
    range_start: rangeStart,
    range_end: rangeEnd,
    force_refresh: !!force,
    max_workers: maxWorkers,
  };

  const j = await fetchJson("/api/student/ensure", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: 180000,
  });
  state.ready = true;
  if (j.week_start) weekStartEl.value = j.week_start;
  if (j.range_start) rangeStartEl.value = String(j.range_start).slice(0, 10);
  if (j.range_end) rangeEndEl.value = endExclusiveLocalIsoToInclusiveYMD(j.range_end);
  const ymdWs = parseYMD(j.week_start);
  if (ymdWs) setWeekHeaders(mondayFromYMD(ymdWs));

  const toks = (j.tok_names || []).length;
  const groupsTotal = Object.values(j.groups_by_tok || {}).reduce((acc, arr) => acc + (arr ? arr.length : 0), 0);
  const cached = j.cached ? "cache" : "fetch";
  const disc = j.group_discovery && j.group_discovery.performed ? `, discovery errors=${j.group_discovery.errors || 0}` : "";
  const rangeEndIncl = j.range_end ? endExclusiveLocalIsoToInclusiveYMD(j.range_end) : "-";
  setStatus(
    `ensure(${cached}): tok_name=${toks}, grupy=${groupsTotal}, range=${String(j.range_start).slice(0, 10)}..${rangeEndIncl}, tydz=${j.week_start} (${j.start} -> ${j.end})${disc}`
  );
}

async function loadWeek({ forceLessons }) {
  const album = (albumEl.value || "").trim();
  const weekStart = weekStartEl.value || null;
  const rangeStart = rangeStartEl.value || null;
  const rangeEnd = rangeEndEl.value || null;
  const maxWorkers = Number(maxWorkersEl.value || 10);
  if (!album) throw new Error("Podaj numer albumu.");

  const payload = {
    album_number: album,
    week_start: weekStart,
    range_start: rangeStart,
    range_end: rangeEnd,
    force_refresh: !!forceLessons,
    max_workers: maxWorkers,
  };

  const j = await fetchJson("/api/student/week", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: 180000,
  });

  state.rawEvents = buildRawEventsFromLessons(j.lessons || []);
  if (j.week_start) weekStartEl.value = j.week_start;
  if (j.range_start) rangeStartEl.value = String(j.range_start).slice(0, 10);
  if (j.range_end) rangeEndEl.value = endExclusiveLocalIsoToInclusiveYMD(j.range_end);
  const ymd = parseYMD(j.week_start);
  if (ymd) setWeekHeaders(mondayFromYMD(ymd));
  if (weekHintEl && j.start && j.end) {
    const s = String(j.start).slice(0, 10);
    const e = String(j.end).slice(0, 10);
    const rs = j.range_start ? String(j.range_start).slice(0, 10) : "-";
    const re = j.range_end ? endExclusiveLocalIsoToInclusiveYMD(j.range_end) : "-";
    weekHintEl.textContent = `${s}..${e} | range ${rs}..${re}`;
  }

  const meta = `week: groups=${j.groups_total} fetched=${j.groups_fetched} skipped=${j.groups_skipped} lessons=${
    (j.lessons || []).length
  } errors=${j.errors || 0}`;
  if (j.last_error) setStatus(`${meta} last_error="${j.last_error}"`);
  else setStatus(meta);

  renderEverything();
}

function bumpWeek(days) {
  const ymd = parseYMD(weekStartEl.value);
  if (!ymd) return;
  const mon = mondayFromYMD(ymd);
  const next = addDaysUTC(mon, days);
  weekStartEl.value = utcDateToYMD(next);
  setWeekHeaders(next);
}

btnPrevWeekEl.addEventListener("click", async () => {
  try {
    bumpWeek(-7);
    toast("Ładowanie tygodnia...");
    setStatus("loading week...");
    disableUi(true, "week");
    await loadWeek({ forceLessons: false });
    toast("Tydzień załadowany");
  } catch (e) {
    setStatus(String(e));
    toast(String(e));
  } finally {
    disableUi(false);
  }
});

btnNextWeekEl.addEventListener("click", async () => {
  try {
    bumpWeek(7);
    toast("Ładowanie tygodnia...");
    setStatus("loading week...");
    disableUi(true, "week");
    await loadWeek({ forceLessons: false });
    toast("Tydzień załadowany");
  } catch (e) {
    setStatus(String(e));
    toast(String(e));
  } finally {
    disableUi(false);
  }
});

btnLoadEl.addEventListener("click", async () => {
  try {
    toast("Ensure (cache)...");
    setStatus("1/2: ensure student (cache)...");
    disableUi(true, "ensure");
    await ensureStudent({ force: false });
    toast("Ładowanie zajęć...");
    setStatus("2/2: load week...");
    setWorking(true, "week");
    await loadWeek({ forceLessons: false });
    toast("Plan załadowany");
  } catch (e) {
    setStatus(String(e));
    toast(String(e));
  } finally {
    disableUi(false);
  }
});

btnRefreshEl.addEventListener("click", async () => {
  try {
    toast("Ensure (force)...");
    setStatus("1/2: ensure student (force)...");
    disableUi(true, "ensure");
    await ensureStudent({ force: true });
    toast("Pobieranie zajęć (force)...");
    setStatus("2/2: load week (force)...");
    setWorking(true, "week");
    await loadWeek({ forceLessons: true });
    toast("Odświeżono");
  } catch (e) {
    setStatus(String(e));
    toast(String(e));
  } finally {
    disableUi(false);
  }
});

$("#btnAllOn").addEventListener("click", () => {
  state.filtersTouched = true;
  const keys = new Set();
  for (const s of state.subjectToKeys.values()) {
    for (const k of s) keys.add(k);
  }
  state.activeFilters = keys;
  renderEverything();
});

$("#btnAllOff").addEventListener("click", () => {
  state.filtersTouched = true;
  state.activeFilters = new Set();
  renderEverything();
});

function boot() {
  seedTimeColumn();
  const today = new Date();
  const todayYmd = parseYMD(formatYMDLocal(today));
  const mon = mondayFromYMD(todayYmd);
  weekStartEl.value = utcDateToYMD(mon);
  rangeStartEl.value = weekStartEl.value;
  rangeEndEl.value = utcDateToYMD(addDaysUTC(mon, 6));
  setWeekHeaders(mon);
  if (apiBaseEl) apiBaseEl.textContent = window.location.origin;
  setWorking(false, "idle");
  ensureHealth().catch(() => {});
  setStatus("Wpisz numer albumu i kliknij Załaduj.");
}

boot();
