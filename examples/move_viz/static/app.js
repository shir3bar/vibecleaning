const PALETTE = ["#60a5fa", "#2dd4bf", "#fde047", "#fb923c", "#f472b6", "#a78bfa", "#34d399", "#f87171"];
const MOVE_VIZ_PROTOCOL = 6;
const SQLITE_UPLOAD_TIMEOUT_MS = 120_000;
const SQLITE_READ_TIMEOUT_MS = 30_000;
const SERVER_CHECK_TIMEOUT_MS = 5_000;
const BASEMAPS = {
  Positron: rasterStyle("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", ["a", "b", "c", "d"], 20, "#e9edf2", "© OpenStreetMap contributors © CARTO"),
  "Dark Matter": rasterStyle("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", ["a", "b", "c", "d"], 20, "#08111b", "© OpenStreetMap contributors © CARTO"),
  "OSM Streets": rasterStyle("https://tile.openstreetmap.org/{z}/{x}/{y}.png", [], 19, "#f2efe9", "© OpenStreetMap contributors"),
  Satellite: rasterStyle("https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", [], 19, "#08111b", "Imagery © Esri, Maxar, Earthstar Geographics, and the GIS User Community"),
  Topographic: rasterStyle("https://tile.opentopomap.org/{z}/{x}/{y}.png", [], 17, "#e8e5dc", "© OpenStreetMap contributors · OpenTopoMap"),
};

function rasterStyle(tile, subdomains, maxzoom, background, attribution) {
  const tiles = subdomains.length ? subdomains.map(domain => tile.replace("{s}", domain).replace("{r}", "")) : [tile];
  return {
    version: 8,
    sources: { basemap: { type: "raster", tiles, tileSize: 256, maxzoom, attribution } },
    layers: [
      { id: "background", type: "background", paint: { "background-color": background } },
      { id: "basemap", type: "raster", source: "basemap" },
    ],
  };
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, character => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
  })[character]);
}

function truthy(value) {
  return ["true", "1", "yes", "y"].includes(String(value ?? "").trim().toLowerCase());
}

function roleReferenceKey(role) {
  return role.replace(/-([a-z])/g, (_, character) => character.toUpperCase());
}

function colorAt(ratio) {
  const stops = [[59, 130, 246], [45, 212, 191], [253, 224, 71], [249, 115, 22], [239, 68, 68]];
  const scaled = Math.max(0, Math.min(1, ratio)) * (stops.length - 1);
  const lower = Math.floor(scaled);
  const upper = Math.min(stops.length - 1, lower + 1);
  const amount = scaled - lower;
  const rgb = stops[lower].map((value, index) => Math.round(value + (stops[upper][index] - value) * amount));
  return `rgb(${rgb.join(",")})`;
}

async function ensureMapLibre() {
  if (window.maplibregl) return;
  const stylesheet = document.createElement("link");
  stylesheet.rel = "stylesheet";
  stylesheet.href = "/movement-assets/vendor/maplibre-gl/maplibre-gl.css";
  document.head.appendChild(stylesheet);
  await new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = "/movement-assets/vendor/maplibre-gl/maplibre-gl.js";
    script.onload = resolve;
    script.onerror = () => reject(new Error("Could not load the bundled map library"));
    document.head.appendChild(script);
  });
}

class MoveVizApp {
  constructor(root) {
    this.root = root;
    this.map = null;
    this.mapReady = false;
    this.styleChangeId = 0;
    this.session = null;
    this.data = null;
    this.selectedIndividuals = new Set();
    this.selectedFixes = new Set();
    this.rowByKey = new Map();
    this.rowsByIndividual = new Map();
    this.selectionMode = "fix";
    this.segmentAnchorKey = "";
    this.flags = new Map();
    this.valueColumnIndexes = new Map();
    this.colorBy = "";
    this.uploadRequest = null;
    this.detailController = null;
    this.detailRequestId = 0;
    this.reviewBusy = false;
    this.renderShell();
    this.bindEvents();
  }

  renderShell() {
    this.root.innerHTML = `
      <div class="app-shell">
        <header class="topbar">
          <div class="brand"><strong>move_viz</strong><span>SQLite movement viewer</span></div>
          <label class="file-button">Browse SQLite<input type="file" accept=".sqlite,.sqlite3,.db,application/vnd.sqlite3" data-role="file"></label>
          <button type="button" data-role="example">Load bundled example</button>
          <div class="file-meta" data-role="file-meta">No database selected · client protocol ${MOVE_VIZ_PROTOCOL}</div>
        </header>
        <div class="toolbar hidden" data-role="toolbar">
          <label data-role="table-wrap">Table <select data-role="table"></select></label>
          <label>Basemap <select data-role="basemap"></select></label>
          <label>Color by <select data-role="color"></select></label>
          <label><input type="checkbox" data-role="tracks" checked> Tracks</label>
          <label><input type="checkbox" data-role="points" checked> Points</label>
          <button type="button" data-role="reset">Reset view</button>
          <div class="spacer"></div>
          <span class="status" data-role="status"></span>
        </div>
        <main class="workspace">
          <section class="map-wrap">
            <div class="map" data-role="map"></div>
            <div class="empty-state" data-role="empty"><div><strong>Select a SQLite movement database</strong>Choose a local .sqlite or .db file. The source database is opened read-only.</div></div>
            <div class="legend hidden" data-role="legend"></div>
          </section>
          <aside class="side-panel">
            <div class="panel-head">
              <strong>Individuals</strong>
              <div class="individual-tools">
                <input type="search" placeholder="Filter IDs" aria-label="Filter individual IDs" data-role="search">
                <button type="button" data-role="all">All</button>
                <button type="button" data-role="none">None</button>
              </div>
              <button type="button" class="load-more" data-role="load-more" disabled>Load more fixes</button>
            </div>
            <div class="individual-list" data-role="individuals"><div class="status">No movement data loaded.</div></div>
            <div class="review-controls">
              <label class="review-mode">Select
                <select data-role="selection-mode" disabled>
                  <option value="fix">Single fixes</option>
                  <option value="segment">Track segment (2 clicks)</option>
                  <option value="individual">Entire individual</option>
                </select>
              </label>
              <input type="text" placeholder="Reviewer name" data-role="user">
              <input type="text" placeholder="Optional review note" data-role="comment">
              <div class="review-actions">
                <button type="button" class="flag" data-role="flag" disabled>Flag selected</button>
                <button type="button" data-role="unflag" disabled>Unflag selected</button>
                <button type="button" data-role="clear-selection" disabled>Clear selection</button>
                <button type="button" data-role="export" disabled>Export flags CSV</button>
                <button type="button" data-role="undo" disabled>Undo step</button>
              </div>
              <details class="review-history">
                <summary data-role="history-summary">Data graph · initial dataset</summary>
                <div class="history-list" data-role="history"></div>
              </details>
            </div>
            <div class="selection-detail" data-role="detail">Click map points to select fixes for review.</div>
          </aside>
        </main>
      </div>`;
    this.refs = Object.fromEntries(
      [...this.root.querySelectorAll("[data-role]")]
        .map(element => [roleReferenceKey(element.dataset.role), element]),
    );
    for (const name of Object.keys(BASEMAPS)) {
      this.refs.basemap.add(new Option(name, name));
    }
    this.refs.basemap.value = "Positron";
    this.refs.user.value = localStorage.getItem("vibecleaning_user_name") || "";
  }

  bindEvents() {
    this.refs.file.addEventListener("change", () => this.openSelectedFile());
    this.refs.example.addEventListener("click", () => this.openBundledExample());
    this.refs.table.addEventListener("change", () => this.loadTable(this.refs.table.value));
    this.refs.basemap.addEventListener("change", () => {
      void this.changeBasemap().catch(error => this.setStatus(`Could not switch basemap: ${error.message}`, true));
    });
    this.refs.color.addEventListener("change", () => { this.colorBy = this.refs.color.value; this.renderData(); });
    this.refs.tracks.addEventListener("change", () => this.renderData());
    this.refs.points.addEventListener("change", () => this.renderData());
    this.refs.reset.addEventListener("click", () => this.fitData());
    this.refs.search.addEventListener("input", () => this.renderIndividuals());
    this.refs.all.addEventListener("click", () => this.selectAllIndividuals());
    this.refs.none.addEventListener("click", () => this.selectNoIndividuals());
    this.refs.loadMore.addEventListener("click", () => void this.loadVisibleIndividuals({ append: true }));
    this.refs.selectionMode.addEventListener("change", () => {
      this.selectionMode = this.refs.selectionMode.value;
      this.clearFixSelection();
    });
    this.refs.user.addEventListener("change", () => {
      localStorage.setItem("vibecleaning_user_name", this.refs.user.value.trim());
    });
    this.refs.flag.addEventListener("click", () => void this.flagSelection());
    this.refs.unflag.addEventListener("click", () => void this.unflagSelection());
    this.refs.clearSelection.addEventListener("click", () => this.clearFixSelection());
    this.refs.export.addEventListener("click", () => void this.exportFlags());
    this.refs.undo.addEventListener("click", () => void this.undoReview());
    this.refs.history.addEventListener("click", event => {
      const button = event.target.closest("[data-dataset-id]");
      if (button) void this.navigateToDataset(button.dataset.datasetId);
    });
  }

  async initializeMap() {
    if (this.map) return;
    await ensureMapLibre();
    this.map = new window.maplibregl.Map({ container: this.refs.map, style: BASEMAPS[this.refs.basemap.value], center: [0, 20], zoom: 1.5 });
    this.map.addControl(new window.maplibregl.NavigationControl(), "top-right");
    await new Promise(resolve => this.map.once("load", resolve));
    this.mapReady = true;
    this.map.on("click", event => {
      if (!this.map.getLayer("move-viz-points")) return;
      this.handleMapClick({ features: this.map.queryRenderedFeatures(event.point, { layers: ["move-viz-points"] }) });
    });
    this.map.on("mousemove", event => {
      if (!this.map.getLayer("move-viz-points")) return;
      const features = this.map.queryRenderedFeatures(event.point, { layers: ["move-viz-points"] });
      this.map.getCanvas().style.cursor = features.length ? "pointer" : "";
    });
  }

  setStatus(message, error = false) {
    this.refs.status.textContent = message;
    this.refs.status.classList.toggle("error", error);
  }

  async checkServer() {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), SERVER_CHECK_TIMEOUT_MS);
    try {
      const response = await fetch(`/api/apps/move-viz/health?protocol=${MOVE_VIZ_PROTOCOL}`, {
        cache: "no-store",
        signal: controller.signal,
      });
      if (!response.ok) {
        throw new Error("The move_viz server is older than this page. Stop it, restart it, and refresh the browser.");
      }
      const payload = await response.json();
      if (payload.protocol !== MOVE_VIZ_PROTOCOL) {
        throw new Error(`Client/server protocol mismatch (${MOVE_VIZ_PROTOCOL}/${payload.protocol ?? "unknown"}). Restart the server and refresh the page.`);
      }
      this.refs.example.hidden = !payload.sample_available;
      return payload;
    } catch (error) {
      if (error.name === "AbortError") {
        throw new Error("The move_viz server did not answer its health check within five seconds.");
      }
      throw error;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  async readSqliteFile(file) {
    this.setStatus("Checking SQLite header…");
    let timeout;
    try {
      const headerBytes = await Promise.race([
        file.slice(0, 16).arrayBuffer(),
        new Promise((_, reject) => {
          timeout = window.setTimeout(
            () => reject(new Error("The browser could not read the SQLite header within 30 seconds.")),
            SQLITE_READ_TIMEOUT_MS,
          );
        }),
      ]);
      const header = new TextDecoder("ascii").decode(headerBytes);
      if (header !== "SQLite format 3\u0000") {
        throw new Error("The selected file is not a SQLite database.");
      }
      return file;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  uploadSqliteFile(file) {
    this.uploadRequest?.abort();
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest();
      this.uploadRequest = request;
      request.open("POST", `/api/apps/move-viz/sessions?filename=${encodeURIComponent(file.name)}`);
      request.setRequestHeader("Content-Type", "application/octet-stream");
      request.responseType = "json";
      request.timeout = SQLITE_UPLOAD_TIMEOUT_MS;
      this.setStatus("Uploading database… 0%");
      request.upload.addEventListener("progress", event => {
        if (!event.lengthComputable) {
          this.setStatus("Uploading database…");
          return;
        }
        const percent = Math.min(100, Math.round((event.loaded / event.total) * 100));
        this.setStatus(`Uploading database… ${percent}%`);
      });
      request.upload.addEventListener("load", () => this.setStatus("Inspecting SQLite tables…"));
      request.addEventListener("load", () => {
        this.uploadRequest = null;
        const payload = request.response || {};
        if (request.status >= 200 && request.status < 300) {
          resolve(payload);
          return;
        }
        reject(new Error(payload.error || `Could not open SQLite file (HTTP ${request.status})`));
      });
      request.addEventListener("error", () => {
        this.uploadRequest = null;
        reject(new Error("The SQLite upload failed. Check that the move_viz server is still running."));
      });
      request.addEventListener("timeout", () => {
        this.uploadRequest = null;
        reject(new Error("The SQLite upload timed out after two minutes."));
      });
      request.addEventListener("abort", () => reject(new Error("The previous SQLite upload was cancelled.")));
      request.send(file);
    });
  }

  async activateSession(payload) {
    this.session = payload;
    this.refs.fileMeta.textContent = `${payload.filename} · ${(payload.size / 1024 / 1024).toFixed(2)} MB`;
    this.populateTables();
    if (!payload.default_table) throw new Error("No table with recognizable longitude and latitude columns was found");
    await this.loadTable(payload.default_table);
  }

  async openSelectedFile() {
    const file = this.refs.file.files?.[0];
    if (!file) return;
    if (this.session?.session_id) {
      void fetch(`/api/apps/move-viz/sessions/${this.session.session_id}`, { method: "DELETE" });
    }
    this.refs.toolbar.classList.remove("hidden");
    this.refs.fileMeta.textContent = `${file.name} · ${(file.size / 1024 / 1024).toFixed(2)} MB`;
    try {
      this.setStatus(`Checking move_viz server · protocol ${MOVE_VIZ_PROTOCOL}…`);
      await this.checkServer();
      await this.readSqliteFile(file);
      const payload = await this.uploadSqliteFile(file);
      await this.activateSession(payload);
    } catch (error) {
      this.setStatus(error.message, true);
    }
  }

  async openBundledExample() {
    this.refs.toolbar.classList.remove("hidden");
    try {
      this.setStatus(`Checking move_viz server · protocol ${MOVE_VIZ_PROTOCOL}…`);
      await this.checkServer();
      this.setStatus("Opening bundled SQLite example…");
      const response = await fetch("/api/apps/move-viz/sessions/example", { method: "POST" });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not open the bundled SQLite example");
      await this.activateSession(payload);
    } catch (error) {
      this.setStatus(error.message, true);
    }
  }

  populateTables() {
    this.refs.table.innerHTML = "";
    for (const table of this.session.tables) {
      const option = new Option(`${table.name} (${table.row_count.toLocaleString()} rows)${table.compatible ? "" : " · mapping needed"}`, table.name);
      option.disabled = !table.compatible;
      this.refs.table.add(option);
    }
    this.refs.tableWrap.hidden = this.session.tables.filter(table => table.compatible).length <= 1;
  }

  async loadTable(table) {
    if (!this.session) return;
    this.detailController?.abort();
    this.detailController = null;
    this.detailRequestId += 1;
    this.refs.table.value = table;
    this.setStatus("Loading movement overview…");
    try {
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/load`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not load movement table");
      this.data = payload;
      this.valueColumnIndexes = new Map();
      this.rebuildRowIndexes();
      this.selectedIndividuals = new Set();
      this.selectedFixes.clear();
      this.segmentAnchorKey = "";
      this.refs.selectionMode.disabled = false;
      this.applyReviewState(payload);
      this.populateColorFields();
      this.renderIndividuals();
      await this.initializeMap();
      this.renderData();
      this.updateLoadMoreButton();
      this.showSelectionPrompt();
      this.setStatus(`${payload.row_count.toLocaleString()} fixes · ${payload.individuals.length.toLocaleString()} individuals · select individuals to load`);
    } catch (error) {
      this.setStatus(error.message, true);
    }
  }

  populateColorFields() {
    this.refs.color.innerHTML = "";
    for (const column of this.data.columns) this.refs.color.add(new Option(column.name, column.name));
    const preferred = this.data.mapping.individual || this.data.columns[0]?.name || "";
    this.colorBy = preferred;
    this.refs.color.value = preferred;
  }

  renderIndividuals() {
    if (!this.data) return;
    const counts = new Map(
      (this.data.individuals || []).map(item => [String(item.individual), Number(item.row_count) || 0]),
    );
    const query = this.refs.search.value.trim().toLowerCase();
    this.refs.individuals.innerHTML = [...counts.entries()]
      .filter(([individual]) => individual.toLowerCase().includes(query))
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([individual, count]) => `<label class="individual-row"><input type="checkbox" data-individual="${escapeHtml(individual)}" ${this.selectedIndividuals.has(individual) ? "checked" : ""}><span>${escapeHtml(individual)}</span><em>${count.toLocaleString()}</em></label>`)
      .join("") || '<div class="status">No matching individuals.</div>';
    for (const checkbox of this.refs.individuals.querySelectorAll("[data-individual]")) {
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) this.selectedIndividuals.add(checkbox.dataset.individual);
        else this.selectedIndividuals.delete(checkbox.dataset.individual);
        void this.loadVisibleIndividuals();
      });
    }
  }

  selectAllIndividuals() {
    if (!this.data) return;
    this.selectedIndividuals = new Set((this.data.individuals || []).map(item => String(item.individual)));
    this.renderIndividuals();
    void this.loadVisibleIndividuals();
  }

  selectNoIndividuals() {
    this.detailController?.abort();
    this.detailController = null;
    this.detailRequestId += 1;
    this.selectedIndividuals.clear();
    this.selectedFixes.clear();
    this.segmentAnchorKey = "";
    if (this.data) {
      this.data.rows = [];
      this.rebuildRowIndexes();
      this.data.loaded_count = 0;
      this.data.matching_row_count = 0;
      this.data.loaded_individuals = [];
      this.data.next_offset = 0;
      this.data.has_more = false;
      this.data.truncated = false;
    }
    this.renderIndividuals();
    this.renderData();
    this.updateLoadMoreButton();
    this.showSelectionPrompt();
    if (this.data) {
      this.setStatus(`${this.data.row_count.toLocaleString()} fixes · select individuals to load`);
    }
  }

  showSelectionPrompt() {
    this.refs.empty.innerHTML = "<div><strong>Select individuals to display</strong>No fixes are loaded into the map until you choose them from the list.</div>";
    this.refs.empty.classList.remove("hidden");
  }

  updateLoadMoreButton() {
    if (!this.data) return;
    const remaining = Math.max(0, (Number(this.data.matching_row_count) || 0) - (Number(this.data.next_offset) || 0));
    this.refs.loadMore.disabled = this.reviewBusy || Boolean(this.detailController) || !this.data.has_more;
    this.refs.loadMore.textContent = this.data.has_more
      ? `Load more fixes (${remaining.toLocaleString()} remaining)`
      : "Load more fixes";
  }

  async loadVisibleIndividuals({ append = false } = {}) {
    if (!this.session || !this.data) return;
    const individuals = [...this.selectedIndividuals].sort((left, right) => left.localeCompare(right));
    this.renderIndividuals();
    if (!individuals.length) {
      this.selectNoIndividuals();
      return;
    }
    const loadedIndividuals = [...(this.data.loaded_individuals || [])].sort((left, right) => left.localeCompare(right));
    const sameScope = individuals.length === loadedIndividuals.length
      && individuals.every((individual, index) => individual === loadedIndividuals[index]);
    if (append && (!sameScope || !this.data.has_more)) return;
    this.detailController?.abort();
    const controller = new AbortController();
    this.detailController = controller;
    const requestId = ++this.detailRequestId;
    if (!append) {
      this.selectedFixes.clear();
      this.segmentAnchorKey = "";
    }
    const offset = append ? Number(this.data.next_offset) || 0 : 0;
    this.updateLoadMoreButton();
    this.setStatus(`${append ? "Loading more fixes" : "Loading fixes"} for ${individuals.length.toLocaleString()} selected individuals…`);
    try {
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/fixes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table: this.data.table, individuals, offset }),
        signal: controller.signal,
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not load the selected individuals");
      if (requestId !== this.detailRequestId) return;
      const incomingRows = payload.rows || [];
      if (append) {
        const byKey = new Map(this.data.rows.map(row => [row.key, row]));
        for (const row of incomingRows) byKey.set(row.key, row);
        this.data.rows = [...byKey.values()];
      } else {
        this.data.rows = incomingRows;
      }
      this.data.loaded_count = this.data.rows.length;
      this.data.matching_row_count = Number(payload.matching_row_count) || 0;
      this.data.skipped_count = (append ? Number(this.data.skipped_count) || 0 : 0) + (Number(payload.skipped_count) || 0);
      this.data.loaded_individuals = payload.loaded_individuals || individuals;
      this.data.next_offset = Number(payload.next_offset) || 0;
      this.data.has_more = Boolean(payload.has_more);
      this.data.value_columns = payload.value_columns || [];
      this.valueColumnIndexes = new Map(this.data.value_columns.map((name, index) => [name, index]));
      this.rebuildRowIndexes();
      this.data.truncated = Boolean(payload.truncated);
      this.data.max_rows = Number(payload.max_rows) || this.data.max_rows;
      this.renderData();
      if (this.data.rows.length) {
        this.refs.empty.classList.add("hidden");
        this.fitData();
      } else {
        this.showSelectionPrompt();
      }
      const limitNote = this.data.has_more
        ? ` · ${this.data.matching_row_count.toLocaleString()} matching fixes total`
        : " · complete selection loaded";
      const skippedNote = this.data.skipped_count ? ` · skipped ${this.data.skipped_count.toLocaleString()} invalid coordinates` : "";
      this.setStatus(`${this.data.loaded_count.toLocaleString()} fixes loaded for ${individuals.length.toLocaleString()} individuals${limitNote}${skippedNote}`);
    } catch (error) {
      if (error.name === "AbortError" || requestId !== this.detailRequestId) return;
      this.setStatus(error.message, true);
    } finally {
      if (this.detailController === controller) this.detailController = null;
      this.updateLoadMoreButton();
    }
  }

  visibleRows() {
    if (!this.data) return [];
    return [...this.selectedIndividuals].flatMap(individual => this.rowsByIndividual.get(individual) || []);
  }

  rebuildRowIndexes() {
    this.rowByKey = new Map();
    this.rowsByIndividual = new Map();
    for (const row of this.data?.rows || []) {
      this.rowByKey.set(row.key, row);
      if (!this.rowsByIndividual.has(row.individual)) this.rowsByIndividual.set(row.individual, []);
      this.rowsByIndividual.get(row.individual).push(row);
    }
  }

  rowValue(row, column) {
    const index = this.valueColumnIndexes.get(column);
    return index === undefined ? undefined : row.values[index];
  }

  colorModel(rows) {
    const descriptor = this.data.columns.find(column => column.name === this.colorBy) || { kind: "categorical" };
    if (descriptor.kind === "numeric") {
      const values = rows.map(row => Number(this.rowValue(row, this.colorBy))).filter(Number.isFinite).sort((a, b) => a - b);
      const minimum = values[0] ?? 0;
      const maximum = values.at(-1) ?? minimum;
      return {
        color: row => {
          const value = Number(this.rowValue(row, this.colorBy));
          return Number.isFinite(value) ? colorAt(maximum === minimum ? 0.5 : (value - minimum) / (maximum - minimum)) : "#64748b";
        },
        legend: `<strong>${escapeHtml(this.colorBy)}</strong><div class="legend-gradient"></div><div>${escapeHtml(minimum)} <span style="float:right">${escapeHtml(maximum)}</span></div>`,
      };
    }
    const levels = [...new Set(rows.map(row => String(this.rowValue(row, this.colorBy) ?? "Missing")))];
    const colors = new Map(levels.map((level, index) => [level, PALETTE[index % PALETTE.length]]));
    const shown = levels.slice(0, 12);
    return {
      color: row => colors.get(String(this.rowValue(row, this.colorBy) ?? "Missing")),
      legend: `<strong>${escapeHtml(this.colorBy)}</strong><div class="legend-levels">${shown.map(level => `<span class="legend-level"><i class="legend-swatch" style="background:${colors.get(level)}"></i>${escapeHtml(level)}</span>`).join("")}${levels.length > shown.length ? `<span>+${levels.length - shown.length} more</span>` : ""}</div>`,
    };
  }

  renderData() {
    if (!this.mapReady || !this.data) return;
    const rows = this.visibleRows();
    const colors = this.colorModel(rows);
    const pointFeatures = rows.map(row => {
      const sourceManual = truthy(this.rowValue(row, "manually-marked-outlier"));
      const sourceAlgorithm = truthy(this.rowValue(row, "algorithm-marked-outlier"));
      const sourceFlagged = sourceManual || sourceAlgorithm;
      return {
        type: "Feature",
        geometry: { type: "Point", coordinates: [row.longitude, row.latitude] },
        properties: {
          key: row.key,
          displayColor: colors.color(row),
          borderColor: sourceFlagged ? "#fbbf24" : "rgba(0,0,0,0)",
          borderWidth: sourceFlagged ? 2 : 0,
        },
      };
    });
    const byIndividual = new Map();
    for (const row of rows) {
      if (!byIndividual.has(row.individual)) byIndividual.set(row.individual, []);
      byIndividual.get(row.individual).push([row.longitude, row.latitude]);
    }
    const lineFeatures = [...byIndividual.entries()].filter(([, coordinates]) => coordinates.length > 1).map(([individual, coordinates]) => ({
      type: "Feature", geometry: { type: "LineString", coordinates }, properties: { individual },
    }));
    this.setGeoJson("move-viz-tracks", lineFeatures, {
      type: "line", paint: { "line-color": "#8aa1b8", "line-opacity": 0.48, "line-width": 1.5 },
      layout: { visibility: this.refs.tracks.checked ? "visible" : "none" },
    });
    this.setGeoJson("move-viz-points", pointFeatures, {
      type: "circle",
      paint: {
        "circle-color": ["get", "displayColor"],
        "circle-radius": 4,
        "circle-opacity": 0.88,
        "circle-stroke-color": ["get", "borderColor"],
        "circle-stroke-width": ["get", "borderWidth"],
      },
      layout: { visibility: this.refs.points.checked ? "visible" : "none" },
    });
    this.renderReviewFlags();
    this.renderReviewSelection();
    this.refs.legend.innerHTML = colors.legend;
    this.refs.legend.classList.toggle("hidden", !rows.length);
  }

  renderReviewSelection() {
    if (!this.mapReady || !this.data) return;
    const selectedRows = this.selectionMode === "individual"
      ? []
      : [...this.selectedFixes].map(key => this.rowByKey.get(key)).filter(Boolean);
    const pointFeatures = selectedRows.map(row => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [row.longitude, row.latitude] },
      properties: { key: row.key },
    }));
    this.setGeoJson("move-viz-review-selection", pointFeatures, {
      type: "circle",
      paint: {
        "circle-color": "rgba(0,0,0,0)",
        "circle-radius": 7,
        "circle-stroke-color": "#7dd3fc",
        "circle-stroke-width": 3,
      },
      layout: { visibility: pointFeatures.length ? "visible" : "none" },
    });
    if (!this.map.getLayer("move-viz-selected-track")) {
      this.map.addLayer(
        {
          id: "move-viz-selected-track",
          type: "line",
          source: "move-viz-tracks",
          paint: { "line-color": "#7dd3fc", "line-opacity": 0.92, "line-width": 3 },
        },
        this.map.getLayer("move-viz-points") ? "move-viz-points" : undefined,
      );
    }
    const selectedRow = this.rowByKey.get(this.selectedFixes.values().next().value);
    const selectedTrack = this.selectionMode === "individual" ? selectedRow?.individual : "";
    this.map.setFilter(
      "move-viz-selected-track",
      selectedTrack
        ? ["==", ["get", "individual"], selectedTrack]
        : ["==", ["get", "individual"], "__move_viz_no_selected_track__"],
    );
    this.renderSelectionDetail();
  }

  renderReviewFlags() {
    if (!this.mapReady || !this.data) return;
    const flaggedIndividuals = new Set();
    const pointFeatures = [];
    for (const [key, flag] of this.flags) {
      const row = this.rowByKey.get(key);
      if (!row) continue;
      if (flag?.scope === "individual") {
        flaggedIndividuals.add(row.individual);
        continue;
      }
      pointFeatures.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [row.longitude, row.latitude] },
        properties: { key },
      });
    }
    this.setGeoJson("move-viz-review-flags", pointFeatures, {
      type: "circle",
      paint: {
        "circle-color": "rgba(0,0,0,0)",
        "circle-radius": 5.5,
        "circle-stroke-color": "#fb7185",
        "circle-stroke-width": 2.5,
      },
      layout: { visibility: pointFeatures.length ? "visible" : "none" },
    });
    if (!this.map.getLayer("move-viz-flagged-tracks")) {
      this.map.addLayer(
        {
          id: "move-viz-flagged-tracks",
          type: "line",
          source: "move-viz-tracks",
          paint: { "line-color": "#fb7185", "line-opacity": 0.88, "line-width": 2.5 },
        },
        this.map.getLayer("move-viz-points") ? "move-viz-points" : undefined,
      );
    }
    this.map.setFilter(
      "move-viz-flagged-tracks",
      flaggedIndividuals.size
        ? ["in", ["get", "individual"], ["literal", [...flaggedIndividuals]]]
        : ["==", ["get", "individual"], "__move_viz_no_flagged_track__"],
    );
  }

  setGeoJson(id, features, layerDefinition) {
    const data = { type: "FeatureCollection", features };
    const source = this.map.getSource(id);
    if (source) {
      source.setData(data);
      this.map.setLayoutProperty(id, "visibility", layerDefinition.layout.visibility);
      return;
    }
    this.map.addSource(id, { type: "geojson", data });
    this.map.addLayer({ id, source: id, ...layerDefinition });
  }

  async changeBasemap() {
    if (!this.map) return;
    const changeId = ++this.styleChangeId;
    this.mapReady = false;
    const styleLoaded = new Promise(resolve => this.map.once("style.load", resolve));
    const style = JSON.parse(JSON.stringify(BASEMAPS[this.refs.basemap.value]));
    this.map.setStyle(style, { diff: false });
    await styleLoaded;
    if (changeId !== this.styleChangeId) return;
    this.mapReady = true;
    this.renderData();
    if (this.data && (!this.map.getSource("move-viz-tracks") || !this.map.getSource("move-viz-points"))) {
      throw new Error("movement overlays could not be restored");
    }
    this.map.triggerRepaint();
  }

  fitData() {
    if (!this.map || !this.data) return;
    const rows = this.visibleRows();
    if (!rows.length) return;
    const bounds = rows.reduce((current, row) => current.extend([row.longitude, row.latitude]), new window.maplibregl.LngLatBounds());
    this.map.fitBounds(bounds, { padding: 45, maxZoom: 14, duration: 350 });
  }

  handleMapClick(event) {
    const key = event.features?.[0]?.properties?.key;
    if (!key) return;
    const row = this.rowByKey.get(key);
    if (!row) return;
    if (this.selectionMode === "individual") {
      const individualKeys = (this.rowsByIndividual.get(row.individual) || []).map(candidate => candidate.key);
      const remove = individualKeys.every(candidateKey => this.selectedFixes.has(candidateKey));
      this.selectedFixes = new Set(remove ? [] : individualKeys);
      this.segmentAnchorKey = "";
    } else if (this.selectionMode === "segment") {
      this.selectSegmentEndpoint(row);
    } else {
      this.segmentAnchorKey = "";
      if (this.selectedFixes.has(key)) this.selectedFixes.delete(key);
      else this.selectedFixes.add(key);
    }
    this.renderReviewSelection();
  }

  selectSegmentEndpoint(row) {
    const anchor = this.rowByKey.get(this.segmentAnchorKey);
    if (!anchor || anchor.individual !== row.individual) {
      this.segmentAnchorKey = row.key;
      this.selectedFixes = new Set([row.key]);
      return;
    }
    const track = this.rowsByIndividual.get(row.individual) || [];
    const anchorIndex = track.findIndex(candidate => candidate.key === anchor.key);
    const endpointIndex = track.findIndex(candidate => candidate.key === row.key);
    const start = Math.min(anchorIndex, endpointIndex);
    const end = Math.max(anchorIndex, endpointIndex);
    this.selectedFixes = new Set(track.slice(start, end + 1).map(candidate => candidate.key));
    this.segmentAnchorKey = "";
  }

  clearFixSelection() {
    this.selectedFixes.clear();
    this.segmentAnchorKey = "";
    this.renderReviewSelection();
  }

  renderSelectionDetail() {
    const selectedCount = this.selectedFixes.size;
    const row = this.rowByKey.get(this.selectedFixes.values().next().value);
    let selectedHasFlag = false;
    if (this.flags.size) {
      for (const key of this.selectedFixes) {
        if (this.flags.has(key)) {
          selectedHasFlag = true;
          break;
        }
      }
    }
    const segmentPending = this.selectionMode === "segment" && Boolean(this.segmentAnchorKey);
    const incompleteIndividualScope = this.selectionMode === "individual" && this.data.truncated;
    this.refs.flag.disabled = this.reviewBusy || !selectedCount || segmentPending || incompleteIndividualScope;
    this.refs.unflag.disabled = this.reviewBusy || incompleteIndividualScope || !selectedHasFlag;
    this.refs.clearSelection.disabled = this.reviewBusy || !selectedCount;
    this.refs.export.disabled = this.reviewBusy || !this.flags.size;
    if (!selectedCount || !row) {
      const instruction = this.selectionMode === "individual"
        ? "Click any fix to select its entire individual."
        : this.selectionMode === "segment"
          ? "Click two fixes from the same individual to select the intervening track segment."
          : "Click map points to select fixes for review.";
      this.refs.detail.textContent = `${this.flags.size.toLocaleString()} manually flagged fixes. ${instruction}`;
      return;
    }
    if (segmentPending) {
      this.refs.detail.textContent = `Segment start selected for ${row.individual}. Click a second fix from that individual.`;
      return;
    }
    if (incompleteIndividualScope) {
      this.refs.detail.textContent = `The selected scope is capped at ${this.data.max_rows.toLocaleString()} fixes, so an entire-individual flag would be incomplete. Select fewer fixes or raise MOVE_VIZ_MAX_ROWS before using this scope.`;
      return;
    }
    const entries = [
      ["Scope", this.selectionMode === "individual" ? "Entire individual" : this.selectionMode === "segment" ? "Track segment" : "Fix selection"],
      ["Selected", selectedCount.toLocaleString()], ["Individual", row.individual], ["Timestamp", row.timestamp ?? ""],
      ["Longitude", row.longitude], ["Latitude", row.latitude], ["Row key", row.key],
    ];
    if (truthy(this.rowValue(row, "manually-marked-outlier"))) entries.push(["Source flag", "manually-marked-outlier=true"]);
    if (truthy(this.rowValue(row, "algorithm-marked-outlier"))) entries.push(["Source flag", "algorithm-marked-outlier=true"]);
    this.refs.detail.innerHTML = `<dl>${entries.map(([name, value]) => `<dt>${escapeHtml(name)}</dt><dd>${escapeHtml(value)}</dd>`).join("")}</dl>`;
  }

  applyReviewState(payload) {
    if (!this.session) return;
    this.session.project_name = payload.project_name || this.session.project_name;
    this.session.dataset_id = payload.dataset_id || this.session.dataset_id;
    this.session.graph = payload.graph || this.session.graph;
    this.session.analyses = payload.analyses || this.session.analyses || [];
    this.flags = new Map(Object.entries(payload.flags || {}));
    this.renderHistory();
  }

  renderHistory() {
    const graph = this.session?.graph;
    if (!graph) return;
    const currentId = graph.current_dataset_id;
    const stepsByOutput = new Map((graph.steps || []).map(step => [step.output_dataset_id, step]));
    const analyses = this.session.analyses || [];
    const datasets = [...(graph.datasets || [])].sort((left, right) => {
      if (left.dataset_id === graph.root_dataset_id) return -1;
      if (right.dataset_id === graph.root_dataset_id) return 1;
      return String(left.created_at || "").localeCompare(String(right.created_at || ""));
    });
    this.refs.historySummary.textContent = `Data graph · ${datasets.length.toLocaleString()} dataset${datasets.length === 1 ? "" : "s"} · ${analyses.length.toLocaleString()} analysis${analyses.length === 1 ? "" : "es"}`;
    const datasetItems = datasets.map(dataset => {
      const step = stepsByOutput.get(dataset.dataset_id);
      const label = step?.title || (dataset.dataset_id === graph.root_dataset_id ? "Initial SQLite import" : dataset.note || "Dataset");
      const current = dataset.dataset_id === currentId;
      return `<button type="button" data-dataset-id="${escapeHtml(dataset.dataset_id)}" ${current ? "disabled" : ""}><strong>${escapeHtml(label)}</strong><span>${escapeHtml(dataset.created_at || "")}${current ? " · current" : ""}</span></button>`;
    }).join("");
    const analysisItems = analyses.map(analysis => `<div class="history-analysis"><strong>${escapeHtml(analysis.title || "Analysis")}</strong><span>${escapeHtml(analysis.created_at || "")} · ${escapeHtml(analysis.analysis_id || "")}</span></div>`).join("");
    this.refs.history.innerHTML = datasetItems + analysisItems;
    const currentDataset = datasets.find(dataset => dataset.dataset_id === currentId);
    this.refs.undo.disabled = this.reviewBusy || !currentDataset?.parent_dataset_id;
  }

  setReviewBusy(busy) {
    this.reviewBusy = busy;
    this.renderSelectionDetail();
    this.renderHistory();
    this.updateLoadMoreButton();
  }

  reviewUser() {
    const user = this.refs.user.value.trim();
    if (!user) {
      this.refs.user.focus();
      throw new Error("Enter a reviewer name before changing the review graph.");
    }
    localStorage.setItem("vibecleaning_user_name", user);
    return user;
  }

  async saveReviewOperation(operation) {
    if (!this.selectedFixes.size || !this.session || !this.data) return;
    try {
      const user = this.reviewUser();
      this.setReviewBusy(true);
      this.setStatus(`${operation === "flag" ? "Flagging" : "Unflagging"} ${this.selectedFixes.size.toLocaleString()} fixes in a new graph step…`);
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/review`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          operation,
          dataset_id: this.session.dataset_id,
          table: this.data.table,
          individuals: this.data.loaded_individuals || [...this.selectedIndividuals],
          row_keys: [...this.selectedFixes],
          scope: this.selectionMode,
          comment: this.refs.comment.value.trim(),
          user,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not save the review graph step");
      this.applyReviewState(payload);
      this.renderReviewFlags();
      this.renderReviewSelection();
      const stepId = payload.step_result?.step?.step_id || "saved step";
      this.setStatus(`Review graph updated · ${stepId}`);
    } catch (error) {
      this.setStatus(error.message, true);
    } finally {
      this.setReviewBusy(false);
    }
  }

  async flagSelection() {
    await this.saveReviewOperation("flag");
  }

  async unflagSelection() {
    await this.saveReviewOperation("unflag");
  }

  async undoReview() {
    if (!this.session || !this.data) return;
    try {
      this.setReviewBusy(true);
      this.setStatus("Moving to the parent dataset…");
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/undo`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table: this.data.table }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not undo the current review step");
      this.applyReviewState(payload);
      this.renderReviewFlags();
      this.renderReviewSelection();
      this.setStatus("Moved to the parent dataset.");
    } catch (error) {
      this.setStatus(error.message, true);
    } finally {
      this.setReviewBusy(false);
    }
  }

  async navigateToDataset(datasetId) {
    if (!this.session || !this.data || !datasetId) return;
    try {
      this.setReviewBusy(true);
      this.setStatus("Loading review history stage…");
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/head`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table: this.data.table, dataset_id: datasetId }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not load that history stage");
      this.applyReviewState(payload);
      this.renderReviewFlags();
      this.renderReviewSelection();
      this.setStatus("Review history stage loaded.");
    } catch (error) {
      this.setStatus(error.message, true);
    } finally {
      this.setReviewBusy(false);
    }
  }

  async exportFlags() {
    if (!this.flags.size || !this.session || !this.data) return;
    try {
      const user = this.reviewUser();
      this.setReviewBusy(true);
      this.setStatus("Creating a reproducible flag-export analysis…");
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/export`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset_id: this.session.dataset_id,
          table: this.data.table,
          user,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not export the current graph stage");
      this.applyReviewState(payload);
      const link = document.createElement("a");
      link.href = payload.download_url;
      link.download = payload.download_name || "move_viz_flags.csv";
      document.body.appendChild(link);
      link.click();
      link.remove();
      const analysisId = payload.analysis_result?.analysis?.analysis_id || "saved analysis";
      this.setStatus(`Flag export recorded · ${analysisId}`);
    } catch (error) {
      this.setStatus(error.message, true);
    } finally {
      this.setReviewBusy(false);
    }
  }
}

new MoveVizApp(document.getElementById("app"));
