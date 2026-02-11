/* Minimal UI with polling. No framework. */

const $ = (sel) => document.querySelector(sel);

const apiBaseEl = $("#apiBase");
const apiStatusEl = $("#apiStatus");

const tokNameEl = $("#tokName");
const dateStartEl = $("#dateStart");
const dateEndEl = $("#dateEnd");
const useLast3El = $("#useLast3");
const maxWorkersEl = $("#maxWorkers");

const syncFormEl = $("#syncForm");
const syncNoteEl = $("#syncNote");
const btnStartEl = $("#btnStart");
const btnAttachEl = $("#btnAttach");

const runIdEl = $("#runId");
const runStatusEl = $("#runStatus");
const runRangeEl = $("#runRange");
const runRoomsEl = $("#runRooms");
const runGroupsEl = $("#runGroups");
const runErrorsEl = $("#runErrors");
const runLastErrorEl = $("#runLastError");
const barInEl = $("#barIn");

const groupFilterEl = $("#groupFilter");
const btnRefreshEl = $("#btnRefresh");
const btnCopyEl = $("#btnCopy");
const btnCsvEl = $("#btnCsv");
const groupsCountEl = $("#groupsCount");
const groupsNewEl = $("#groupsNew");
const groupsListEl = $("#groupsList");
const toastEl = $("#toast");

const state = {
  currentRunId: null,
  groups: [],
  newSet: new Set(),
  newCount: 0,
  polling: null,
};

function toast(msg) {
  toastEl.textContent = msg;
  toastEl.hidden = false;
  setTimeout(() => {
    toastEl.hidden = true;
  }, 1800);
}

function setNote(msg) {
  syncNoteEl.textContent = msg || ".";
}

function setApiStatus(ok) {
  apiStatusEl.textContent = ok ? "API OK" : "API DOWN";
  apiStatusEl.style.borderColor = ok ? "rgba(22,123,63,0.25)" : "rgba(180,35,24,0.25)";
  apiStatusEl.style.background = ok ? "rgba(22,123,63,0.14)" : "rgba(180,35,24,0.14)";
  apiStatusEl.style.color = ok ? "rgba(22,123,63,1)" : "rgba(180,35,24,1)";
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
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status}: ${text || res.statusText}`);
    }
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

function badgeFor(status) {
  if (status === "running") return `<span class="badge blue">running</span>`;
  if (status === "success") return `<span class="badge green">success</span>`;
  if (status === "failed") return `<span class="badge red">failed</span>`;
  if (status === "queued") return `<span class="badge gray">queued</span>`;
  return `<span class="badge gray">${status || "idle"}</span>`;
}

function setRunUI(run) {
  if (!run) {
    runIdEl.textContent = "-";
    runStatusEl.innerHTML = badgeFor("idle");
    runRangeEl.textContent = "-";
    runRoomsEl.textContent = "rooms: 0 / 0";
    runGroupsEl.textContent = "groups: 0";
    runErrorsEl.textContent = "0";
    runLastErrorEl.hidden = true;
    barInEl.style.width = "0%";
    return;
  }

  runIdEl.textContent = String(run.id);
  runStatusEl.innerHTML = badgeFor(run.status);
  runRangeEl.textContent = `${run.start_iso} -> ${run.end_iso}`;
  runRoomsEl.textContent = `rooms: ${run.rooms_processed} / ${run.rooms_total}`;
  runGroupsEl.textContent = `groups: ${run.groups_found}`;
  runErrorsEl.textContent = String(run.errors || 0);

  const pct = run.rooms_total > 0 ? Math.round((run.rooms_processed / run.rooms_total) * 100) : 0;
  barInEl.style.width = `${Math.max(0, Math.min(100, pct))}%`;

  if (run.last_error) {
    runLastErrorEl.hidden = false;
    runLastErrorEl.textContent = run.last_error;
  } else {
    runLastErrorEl.hidden = true;
    runLastErrorEl.textContent = "";
  }
}

function renderGroupsView() {
  const filter = (groupFilterEl.value || "").trim().toLowerCase();
  const groups = state.groups || [];
  const filtered = filter ? groups.filter((g) => g.toLowerCase().includes(filter)) : groups.slice();

  const prevTop = groupsListEl.scrollTop;
  groupsListEl.innerHTML = "";
  for (const g of filtered) {
    const li = document.createElement("li");
    li.textContent = g;
    if (state.newSet && state.newSet.has(g)) {
      li.classList.add("new");
    }
    groupsListEl.appendChild(li);
  }
  groupsListEl.scrollTop = prevTop;
}

function setGroups(groups, { markNew = false } = {}) {
  const old = new Set(state.groups || []);
  const newSet = markNew ? new Set(groups.filter((g) => !old.has(g))) : new Set();
  state.groups = groups.slice();
  state.newSet = newSet;
  state.newCount = newSet.size;

  groupsCountEl.textContent = String(groups.length);
  groupsNewEl.textContent = String(state.newCount);
  renderGroupsView();
}

function getPayloadFromForm() {
  const tokName = (tokNameEl.value || "").trim();
  const maxWorkers = Number(maxWorkersEl.value || 10);

  if (useLast3El.checked) {
    return { tok_name: tokName, max_workers: maxWorkers };
  }

  const start = dateStartEl.value ? dateStartEl.value : null;
  const end = dateEndEl.value ? dateEndEl.value : null;
  return { tok_name: tokName, start, end, max_workers: maxWorkers };
}

async function ensureHealth() {
  try {
    await fetchJson("/api/health", { timeoutMs: 5000 });
    setApiStatus(true);
  } catch (_) {
    setApiStatus(false);
  }
}

async function attachActiveRun() {
  setNote("Checking active run...");
  try {
    const j = await fetchJson("/api/runs/active", { timeoutMs: 8000 });
    if (j.run_id) {
      state.currentRunId = j.run_id;
      setNote(`Attached to run_id=${j.run_id}`);
      startPolling();
      return;
    }
    setNote("No active run.");
  } catch (e) {
    setNote(String(e));
  }
}

async function refreshGroups() {
  const tokName = (tokNameEl.value || "").trim();
  if (state.currentRunId) {
    const j = await fetchJson(`/api/groups?tok_name=${encodeURIComponent(tokName)}&run_id=${state.currentRunId}`);
    setGroups(j.groups || [], { markNew: true });
  } else {
    const j = await fetchJson(`/api/groups?tok_name=${encodeURIComponent(tokName)}`);
    setGroups(j.groups || [], { markNew: false });
    if (j.run_id) {
      // Helpful default: show which run generated current groups.
      runIdEl.textContent = String(j.run_id);
    }
  }
}

async function pollOnce() {
  if (!state.currentRunId) return;

  const run = await fetchJson(`/api/runs/${state.currentRunId}`, { timeoutMs: 12000 });
  setRunUI(run);

  // Update groups while running to show live collection
  await refreshGroups();

  if (run.status === "success" || run.status === "failed") {
    stopPolling();
    toast(`Run finished: ${run.status}`);
  }
}

function startPolling() {
  if (state.polling) return;
  state.polling = setInterval(() => {
    pollOnce().catch((e) => {
      setNote(String(e));
    });
  }, 1000);
  pollOnce().catch(() => {});
}

function stopPolling() {
  if (!state.polling) return;
  clearInterval(state.polling);
  state.polling = null;
}

function updateDateInputsState() {
  const disabled = useLast3El.checked;
  dateStartEl.disabled = disabled;
  dateEndEl.disabled = disabled;
}

function seedDates() {
  // UI hint only; backend can compute last 3 months automatically.
  const now = new Date();
  const end = now.toISOString().slice(0, 10);
  const start = new Date(now);
  start.setMonth(start.getMonth() - 3);
  const startStr = start.toISOString().slice(0, 10);
  dateStartEl.value = startStr;
  dateEndEl.value = end;
}

syncFormEl.addEventListener("submit", async (e) => {
  e.preventDefault();

  btnStartEl.disabled = true;
  setNote("Starting...");
  try {
    const payload = getPayloadFromForm();
    const res = await fetchJson("/api/sync", {
      method: "POST",
      body: JSON.stringify(payload),
      timeoutMs: 20000,
    });
    state.currentRunId = res.run_id;
    setNote(`Started run_id=${res.run_id}`);
    startPolling();
  } catch (err) {
    const msg = String(err || "");
    if (msg.includes("HTTP 409")) {
      setNote("Sync already running. Attaching...");
      await attachActiveRun();
    } else {
      setNote(msg);
    }
  } finally {
    btnStartEl.disabled = false;
  }
});

btnAttachEl.addEventListener("click", () => {
  attachActiveRun().catch(() => {});
});

btnRefreshEl.addEventListener("click", () => {
  refreshGroups().catch((e) => setNote(String(e)));
});

btnCopyEl.addEventListener("click", async () => {
  const text = (state.groups || []).join("\n");
  try {
    await navigator.clipboard.writeText(text);
    toast("Copied");
  } catch (_) {
    toast("Copy failed");
  }
});

btnCsvEl.addEventListener("click", () => {
  const rows = ["group_name", ...(state.groups || [])].map((x) => `"${String(x).replaceAll('"', '""')}"`);
  const blob = new Blob([rows.join("\n") + "\n"], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "groups.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
});

groupFilterEl.addEventListener("input", () => {
  renderGroupsView();
});

useLast3El.addEventListener("change", updateDateInputsState);

async function boot() {
  apiBaseEl.textContent = window.location.origin;
  updateDateInputsState();
  seedDates();

  await ensureHealth();
  await attachActiveRun();
  await refreshGroups();
}

boot().catch((e) => setNote(String(e)));
