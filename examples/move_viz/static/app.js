const PALETTE = ["#60a5fa", "#2dd4bf", "#fde047", "#fb923c", "#f472b6", "#a78bfa", "#34d399", "#f87171"];
const MOVE_VIZ_PROTOCOL = 3;
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
    this.selectionMode = "fix";
    this.segmentAnchorKey = "";
    this.flags = new Map();
    this.colorBy = "";
    this.uploadRequest = null;
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
    this.setStatus("Reading selected file… 0%");
    let timeout;
    try {
      const bytes = await Promise.race([
        file.arrayBuffer(),
        new Promise((_, reject) => {
          timeout = window.setTimeout(
            () => reject(new Error("The browser could not read the selected file within 30 seconds.")),
            SQLITE_READ_TIMEOUT_MS,
          );
        }),
      ]);
      const header = new TextDecoder("ascii").decode(bytes.slice(0, 16));
      if (header !== "SQLite format 3\u0000") {
        throw new Error("The selected file is not a SQLite database.");
      }
      this.setStatus("Reading selected file… 100%");
      return bytes;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  uploadSqliteFile(file, bytes) {
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
      request.send(bytes);
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
      const bytes = await this.readSqliteFile(file);
      const payload = await this.uploadSqliteFile(file, bytes);
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
    this.refs.table.value = table;
    this.setStatus("Loading movement rows…");
    try {
      const response = await fetch(`/api/apps/move-viz/sessions/${this.session.session_id}/load`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not load movement table");
      this.data = payload;
      this.selectedIndividuals = new Set(payload.rows.map(row => row.individual));
      this.selectedFixes.clear();
      this.segmentAnchorKey = "";
      this.refs.selectionMode.disabled = false;
      this.applyReviewState(payload);
      this.populateColorFields();
      this.renderIndividuals();
      await this.initializeMap();
      this.renderData();
      this.fitData();
      this.refs.empty.classList.add("hidden");
      const warning = payload.truncated ? ` · showing first ${payload.max_rows.toLocaleString()}` : "";
      this.setStatus(`${payload.loaded_count.toLocaleString()} fixes · ${this.selectedIndividuals.size.toLocaleString()} individuals${warning}`);
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
    const counts = new Map();
    for (const row of this.data.rows) counts.set(row.individual, (counts.get(row.individual) || 0) + 1);
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
        this.renderData();
      });
    }
  }

  selectAllIndividuals() {
    if (!this.data) return;
    this.selectedIndividuals = new Set(this.data.rows.map(row => row.individual));
    this.renderIndividuals();
    this.renderData();
  }

  selectNoIndividuals() {
    this.selectedIndividuals.clear();
    this.renderIndividuals();
    this.renderData();
  }

  visibleRows() {
    return this.data ? this.data.rows.filter(row => this.selectedIndividuals.has(row.individual)) : [];
  }

  colorModel(rows) {
    const descriptor = this.data.columns.find(column => column.name === this.colorBy) || { kind: "categorical" };
    if (descriptor.kind === "numeric") {
      const values = rows.map(row => Number(row.values[this.colorBy])).filter(Number.isFinite).sort((a, b) => a - b);
      const minimum = values[0] ?? 0;
      const maximum = values.at(-1) ?? minimum;
      return {
        color: row => {
          const value = Number(row.values[this.colorBy]);
          return Number.isFinite(value) ? colorAt(maximum === minimum ? 0.5 : (value - minimum) / (maximum - minimum)) : "#64748b";
        },
        legend: `<strong>${escapeHtml(this.colorBy)}</strong><div class="legend-gradient"></div><div>${escapeHtml(minimum)} <span style="float:right">${escapeHtml(maximum)}</span></div>`,
      };
    }
    const levels = [...new Set(rows.map(row => String(row.values[this.colorBy] ?? "Missing")))];
    const colors = new Map(levels.map((level, index) => [level, PALETTE[index % PALETTE.length]]));
    const shown = levels.slice(0, 12);
    return {
      color: row => colors.get(String(row.values[this.colorBy] ?? "Missing")),
      legend: `<strong>${escapeHtml(this.colorBy)}</strong><div class="legend-levels">${shown.map(level => `<span class="legend-level"><i class="legend-swatch" style="background:${colors.get(level)}"></i>${escapeHtml(level)}</span>`).join("")}${levels.length > shown.length ? `<span>+${levels.length - shown.length} more</span>` : ""}</div>`,
    };
  }

  renderData() {
    if (!this.mapReady || !this.data) return;
    const rows = this.visibleRows();
    const colors = this.colorModel(rows);
    const pointFeatures = rows.map(row => {
      const sourceManual = truthy(row.values["manually-marked-outlier"]);
      const sourceAlgorithm = truthy(row.values["algorithm-marked-outlier"]);
      const selected = this.selectedFixes.has(row.key);
      const flagged = this.flags.has(row.key);
      const sourceFlagged = sourceManual || sourceAlgorithm;
      return {
        type: "Feature",
        geometry: { type: "Point", coordinates: [row.longitude, row.latitude] },
        properties: {
          key: row.key,
          displayColor: colors.color(row),
          borderColor: selected ? "#7dd3fc" : flagged ? "#fb7185" : sourceFlagged ? "#fbbf24" : "rgba(0,0,0,0)",
          borderWidth: selected ? 3 : flagged ? 2.5 : sourceFlagged ? 2 : 0,
          selected: selected ? 1 : 0,
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
        "circle-radius": ["case", ["==", ["get", "selected"], 1], 7, 4],
        "circle-opacity": 0.88,
        "circle-stroke-color": ["get", "borderColor"],
        "circle-stroke-width": ["get", "borderWidth"],
      },
      layout: { visibility: this.refs.points.checked ? "visible" : "none" },
    });
    this.refs.legend.innerHTML = colors.legend;
    this.refs.legend.classList.toggle("hidden", !rows.length);
    this.renderSelectionDetail();
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
    const row = this.data?.rows.find(candidate => candidate.key === key);
    if (!row) return;
    if (this.selectionMode === "individual") {
      const individualKeys = this.data.rows
        .filter(candidate => candidate.individual === row.individual)
        .map(candidate => candidate.key);
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
    this.renderData();
  }

  selectSegmentEndpoint(row) {
    const anchor = this.data.rows.find(candidate => candidate.key === this.segmentAnchorKey);
    if (!anchor || anchor.individual !== row.individual) {
      this.segmentAnchorKey = row.key;
      this.selectedFixes = new Set([row.key]);
      return;
    }
    const track = this.data.rows.filter(candidate => candidate.individual === row.individual);
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
    this.renderData();
  }

  renderSelectionDetail() {
    const selected = this.data.rows.filter(row => this.selectedFixes.has(row.key));
    const segmentPending = this.selectionMode === "segment" && Boolean(this.segmentAnchorKey);
    this.refs.flag.disabled = this.reviewBusy || !selected.length || segmentPending;
    this.refs.unflag.disabled = this.reviewBusy || !selected.some(row => this.flags.has(row.key));
    this.refs.clearSelection.disabled = this.reviewBusy || !selected.length;
    this.refs.export.disabled = this.reviewBusy || !this.flags.size;
    if (!selected.length) {
      const instruction = this.selectionMode === "individual"
        ? "Click any fix to select its entire individual."
        : this.selectionMode === "segment"
          ? "Click two fixes from the same individual to select the intervening track segment."
          : "Click map points to select fixes for review.";
      this.refs.detail.textContent = `${this.flags.size.toLocaleString()} manually flagged fixes. ${instruction}`;
      return;
    }
    if (segmentPending) {
      this.refs.detail.textContent = `Segment start selected for ${selected[0].individual}. Click a second fix from that individual.`;
      return;
    }
    const row = selected[0];
    const entries = [
      ["Scope", this.selectionMode === "individual" ? "Entire individual" : this.selectionMode === "segment" ? "Track segment" : "Fix selection"],
      ["Selected", selected.length.toLocaleString()], ["Individual", row.individual], ["Timestamp", row.timestamp ?? ""],
      ["Longitude", row.longitude], ["Latitude", row.latitude], ["Row key", row.key],
    ];
    if (truthy(row.values["manually-marked-outlier"])) entries.push(["Source flag", "manually-marked-outlier=true"]);
    if (truthy(row.values["algorithm-marked-outlier"])) entries.push(["Source flag", "algorithm-marked-outlier=true"]);
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
          row_keys: [...this.selectedFixes],
          scope: this.selectionMode,
          comment: this.refs.comment.value.trim(),
          user,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || "Could not save the review graph step");
      this.applyReviewState(payload);
      this.renderData();
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
      this.renderData();
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
      this.renderData();
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
