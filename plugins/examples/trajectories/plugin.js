const BASEMAP_STYLES = {
  Positron: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  Voyager: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
  "Dark Matter": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
};

const MAX_POINTS_PER_SERIES = 2500;
const PATH_ALPHA = 180;
const POINT_ALPHA = 210;
const STORAGE_VERSION = 1;

let assetPromise = null;

export async function createPlugin(context) {
  const plugin = new TrajectoryExamplePlugin(context);
  await plugin.init();
  return plugin;
}

async function ensureAssetsLoaded() {
  if (assetPromise) {
    return assetPromise;
  }
  assetPromise = Promise.all([
    loadCss("/plugins/examples/trajectories/vendor/maplibre-gl/maplibre-gl.css"),
    loadScript("/plugins/examples/trajectories/vendor/maplibre-gl/maplibre-gl.js"),
    loadScript("/plugins/examples/trajectories/vendor/deckgl/deck.gl.min.js"),
    loadScript("/plugins/examples/trajectories/vendor/deckgl/deck.gl-mapbox.min.js"),
  ]);
  return assetPromise;
}

function loadCss(href) {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector(`link[data-vibecleaning-asset="${href}"]`);
    if (existing) {
      resolve();
      return;
    }
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = href;
    link.dataset.vibecleaningAsset = href;
    link.onload = () => resolve();
    link.onerror = () => reject(new Error(`Failed to load ${href}`));
    document.head.appendChild(link);
  });
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector(`script[data-vibecleaning-asset="${src}"]`);
    if (existing) {
      if (existing.dataset.loaded === "true") {
        resolve();
      } else {
        existing.addEventListener("load", () => resolve(), { once: true });
        existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
      }
      return;
    }
    const script = document.createElement("script");
    script.src = src;
    script.async = false;
    script.dataset.vibecleaningAsset = src;
    script.addEventListener("load", () => {
      script.dataset.loaded = "true";
      resolve();
    }, { once: true });
    script.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
    document.head.appendChild(script);
  });
}

class TrajectoryExamplePlugin {
  constructor({ mountEl, storageKey, fetchJSON, fetchText }) {
    this.mountEl = mountEl;
    this.storageKey = storageKey;
    this.fetchJSON = fetchJSON;
    this.fetchText = fetchText;
    this.uiState = this.loadUiState();
    this.projects = [];
    this.graph = null;
    this.datasets = [];
    this.currentProject = "";
    this.currentDatasetId = "";
    this.currentArtifact = "";
    this.currentDataset = null;
    this.currentArtifactEntry = null;
    this.data = null;
    this.currentTimeMs = 0;
    this.map = null;
    this.overlay = null;
    this.mapLoaded = false;
  }

  async init() {
    await ensureAssetsLoaded();
    this.renderShell();
    this.bindEvents();
    await this.loadProjects();
  }

  async destroy() {
    if (this.map) {
      this.map.remove();
      this.map = null;
      this.overlay = null;
      this.mapLoaded = false;
    }
  }

  loadUiState() {
    try {
      const raw = localStorage.getItem(this.storageKey);
      if (!raw) {
        throw new Error("missing");
      }
      const parsed = JSON.parse(raw);
      if (!parsed || parsed.version !== STORAGE_VERSION) {
        throw new Error("stale");
      }
      return parsed;
    } catch {
      return {
        version: STORAGE_VERSION,
        project: "",
        datasetId: "",
        artifact: "",
        basemap: "Positron",
        showTrain: true,
        showTest: true,
        showPoints: false,
      };
    }
  }

  saveUiState() {
    this.uiState = {
      version: STORAGE_VERSION,
      project: this.currentProject,
      datasetId: this.currentDatasetId,
      artifact: this.currentArtifact,
      basemap: this.refs.basemap.value,
      showTrain: this.refs.showTrain.checked,
      showTest: this.refs.showTest.checked,
      showPoints: this.refs.showPoints.checked,
    };
    localStorage.setItem(this.storageKey, JSON.stringify(this.uiState));
  }

  renderShell() {
    this.mountEl.innerHTML = `
      <style>
        .traj-root {
          display: grid;
          grid-template-rows: auto auto minmax(0, 1fr);
          height: 100%;
          min-height: 0;
          color: #e5edf7;
          font-family: "Segoe UI", sans-serif;
          background: linear-gradient(180deg, rgba(8, 17, 27, 0.96), rgba(5, 10, 18, 0.98));
        }
        .traj-toolbar {
          display: flex;
          flex-wrap: wrap;
          gap: 10px 14px;
          align-items: center;
          padding: 14px 16px 10px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
          background: rgba(8, 17, 27, 0.8);
        }
        .traj-toolbar label,
        .traj-toggle {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 12px;
          color: #92a4bd;
        }
        .traj-toolbar select,
        .traj-toolbar button {
          font: inherit;
        }
        .traj-toolbar select {
          min-width: 150px;
          padding: 8px 10px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
        }
        .traj-toolbar button {
          padding: 8px 12px;
          border: none;
          border-radius: 10px;
          background: rgba(255, 255, 255, 0.08);
          color: #e5edf7;
          cursor: pointer;
        }
        .traj-status {
          min-height: 24px;
          padding: 0 16px 10px;
          font-size: 12px;
          color: #92a4bd;
        }
        .traj-status.error {
          color: #ffb3c2;
        }
        .traj-main {
          display: grid;
          grid-template-columns: minmax(0, 1.2fr) minmax(300px, 0.8fr);
          gap: 12px;
          min-height: 0;
          padding: 0 16px 14px;
        }
        .traj-map-wrap,
        .traj-side {
          min-height: 0;
          border-radius: 16px;
          overflow: hidden;
          border: 1px solid rgba(255, 255, 255, 0.07);
          background: rgba(255, 255, 255, 0.03);
        }
        .traj-map-wrap {
          position: relative;
          background: #08111b;
        }
        .traj-map {
          width: 100%;
          height: 100%;
        }
        .traj-overlay {
          position: absolute;
          inset: 0;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 24px;
          text-align: center;
          color: #d6deea;
          background: linear-gradient(180deg, rgba(8, 17, 27, 0.18), rgba(8, 17, 27, 0.8));
        }
        .traj-overlay.hidden {
          display: none;
        }
        .traj-overlay-card {
          max-width: 520px;
          padding: 18px 20px;
          border-radius: 16px;
          background: rgba(7, 11, 22, 0.88);
          border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .traj-overlay-card h3 {
          font-size: 16px;
          margin-bottom: 8px;
        }
        .traj-overlay-card p {
          font-size: 13px;
          line-height: 1.5;
        }
        .traj-side {
          display: grid;
          grid-template-rows: auto auto minmax(0, 1fr) auto;
        }
        .traj-side-head,
        .traj-side-summary,
        .traj-slider-row {
          padding: 12px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }
        .traj-side-head {
          font-size: 12px;
          color: #92a4bd;
        }
        .traj-side-summary {
          display: grid;
          gap: 6px;
          font-size: 12px;
          color: #dbe5f0;
        }
        .traj-individuals {
          overflow-y: auto;
          padding: 10px 12px;
        }
        .traj-individual {
          display: grid;
          gap: 6px;
          padding: 10px 12px;
          margin-bottom: 10px;
          border-radius: 12px;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.05);
        }
        .traj-name {
          display: flex;
          justify-content: space-between;
          gap: 10px;
          font-size: 12px;
          color: #eef4fb;
        }
        .traj-species {
          font-size: 11px;
          color: #92a4bd;
        }
        .traj-stats {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          font-size: 11px;
          color: #92a4bd;
        }
        .traj-track {
          position: relative;
          height: 10px;
          width: 100%;
          border-radius: 999px;
          background: rgba(255, 255, 255, 0.06);
          overflow: hidden;
        }
        .traj-bar {
          position: absolute;
          top: 0;
          height: 100%;
          border-radius: 999px;
        }
        .traj-slider-row {
          display: grid;
          gap: 10px;
          border-bottom: none;
        }
        .traj-slider {
          width: 100%;
        }
        .traj-time {
          font-size: 12px;
          color: #dbe5f0;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        }
        @media (max-width: 980px) {
          .traj-main {
            grid-template-columns: 1fr;
            grid-template-rows: minmax(280px, 1fr) minmax(260px, 0.9fr);
          }
        }
      </style>
      <div class="traj-root">
        <div class="traj-toolbar">
          <label>Project <select data-role="project"></select></label>
          <label>Version <select data-role="dataset"></select></label>
          <label>Artifact <select data-role="artifact"></select></label>
          <label>Basemap <select data-role="basemap"></select></label>
          <label class="traj-toggle"><input type="checkbox" data-role="show-train"> Train</label>
          <label class="traj-toggle"><input type="checkbox" data-role="show-test"> Test</label>
          <label class="traj-toggle"><input type="checkbox" data-role="show-points"> Points</label>
          <button type="button" data-role="reset-view">Reset View</button>
        </div>
        <div class="traj-status" data-role="status"></div>
        <div class="traj-main">
          <div class="traj-map-wrap">
            <div class="traj-map" data-role="map"></div>
            <div class="traj-overlay" data-role="overlay">
              <div class="traj-overlay-card">
                <h3>Example: Trajectories</h3>
                <p>This read-only plugin loads one selected artifact at a time through the generic vibecleaning artifact APIs. It does not assume anything about the backend beyond projects, datasets, graph metadata, and artifact fetch endpoints.</p>
              </div>
            </div>
          </div>
          <div class="traj-side">
            <div class="traj-side-head">Individuals and coverage</div>
            <div class="traj-side-summary" data-role="summary"></div>
            <div class="traj-individuals" data-role="individuals"></div>
            <div class="traj-slider-row">
              <input class="traj-slider" data-role="slider" type="range" min="0" max="0" value="0" step="1">
              <div class="traj-time" data-role="time"></div>
            </div>
          </div>
        </div>
      </div>
    `;

    this.refs = {
      project: this.mountEl.querySelector('[data-role="project"]'),
      dataset: this.mountEl.querySelector('[data-role="dataset"]'),
      artifact: this.mountEl.querySelector('[data-role="artifact"]'),
      basemap: this.mountEl.querySelector('[data-role="basemap"]'),
      showTrain: this.mountEl.querySelector('[data-role="show-train"]'),
      showTest: this.mountEl.querySelector('[data-role="show-test"]'),
      showPoints: this.mountEl.querySelector('[data-role="show-points"]'),
      resetView: this.mountEl.querySelector('[data-role="reset-view"]'),
      status: this.mountEl.querySelector('[data-role="status"]'),
      summary: this.mountEl.querySelector('[data-role="summary"]'),
      individuals: this.mountEl.querySelector('[data-role="individuals"]'),
      slider: this.mountEl.querySelector('[data-role="slider"]'),
      time: this.mountEl.querySelector('[data-role="time"]'),
      map: this.mountEl.querySelector('[data-role="map"]'),
      overlay: this.mountEl.querySelector('[data-role="overlay"]'),
    };

    for (const name of Object.keys(BASEMAP_STYLES)) {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      this.refs.basemap.appendChild(option);
    }
    this.refs.basemap.value = this.uiState.basemap || "Positron";
    this.refs.showTrain.checked = this.uiState.showTrain !== false;
    this.refs.showTest.checked = this.uiState.showTest !== false;
    this.refs.showPoints.checked = this.uiState.showPoints === true;
  }

  bindEvents() {
    this.refs.project.addEventListener("change", async () => {
      this.currentProject = this.refs.project.value;
      await this.loadProject();
    });

    this.refs.dataset.addEventListener("change", async () => {
      this.currentDatasetId = this.refs.dataset.value;
      await this.loadDataset();
    });

    this.refs.artifact.addEventListener("change", async () => {
      this.currentArtifact = this.refs.artifact.value;
      await this.loadArtifact();
    });

    this.refs.basemap.addEventListener("change", async () => {
      this.saveUiState();
      await this.rebuildMap(true);
    });

    this.refs.showTrain.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
      this.renderIndividuals();
    });
    this.refs.showTest.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
      this.renderIndividuals();
    });
    this.refs.showPoints.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
    });
    this.refs.resetView.addEventListener("click", () => this.resetView());
    this.refs.slider.addEventListener("input", () => {
      this.currentTimeMs = Number(this.refs.slider.value) || 0;
      this.updateTimeLabel();
      this.renderLayers();
    });
  }

  async loadProjects() {
    this.setStatus("Loading projects…");
    let payload;
    try {
      payload = await this.fetchJSON("/api/projects");
    } catch (error) {
      this.setStatus(error.message, true);
      this.showOverlay("The project index could not be loaded.");
      return;
    }
    this.projects = Array.isArray(payload.projects) ? payload.projects : [];
    this.refs.project.innerHTML = "";
    for (const project of this.projects) {
      const option = document.createElement("option");
      option.value = project.name;
      option.textContent = project.name;
      this.refs.project.appendChild(option);
    }
    if (!this.projects.length) {
      this.showOverlay("No projects were found under data/{project}/.");
      this.setStatus("No projects found.", true);
      return;
    }
    const storedProject = this.uiState.project;
    this.currentProject = this.projects.some(project => project.name === storedProject)
      ? storedProject
      : this.projects[0].name;
    this.refs.project.value = this.currentProject;
    await this.loadProject();
  }

  async loadProject() {
    this.setStatus(`Loading graph for ${this.currentProject}…`);
    try {
      const [state, graph] = await Promise.all([
        this.fetchJSON(`/api/project/${encodeURIComponent(this.currentProject)}/state`),
        this.fetchJSON(`/api/project/${encodeURIComponent(this.currentProject)}/graph`),
      ]);
      this.graph = graph;
      this.datasets = [...(Array.isArray(graph.datasets) ? graph.datasets : [])]
        .sort((left, right) => String(right.created_at || "").localeCompare(String(left.created_at || "")));
      this.refs.dataset.innerHTML = "";
      for (const dataset of this.datasets) {
        const option = document.createElement("option");
        option.value = dataset.dataset_id;
        option.textContent = formatDatasetLabel(dataset, graph.current_dataset_id);
        this.refs.dataset.appendChild(option);
      }
      const storedDataset = this.uiState.project === this.currentProject ? this.uiState.datasetId : "";
      this.currentDatasetId = this.datasets.some(dataset => dataset.dataset_id === storedDataset)
        ? storedDataset
        : state.current_dataset.dataset_id;
      this.refs.dataset.value = this.currentDatasetId;
      await this.loadDataset();
    } catch (error) {
      this.setStatus(error.message, true);
      this.showOverlay(`Could not load ${this.currentProject}.`);
    }
  }

  async loadDataset() {
    this.data = null;
    this.currentDataset = null;
    this.currentArtifactEntry = null;
    this.refs.summary.innerHTML = "";
    this.refs.individuals.innerHTML = "";
    this.renderLayers();
    this.setStatus(`Loading dataset ${this.currentDatasetId}…`);
    try {
      this.currentDataset = await this.fetchJSON(
        `/api/project/${encodeURIComponent(this.currentProject)}/dataset/${encodeURIComponent(this.currentDatasetId)}`,
      );
    } catch (error) {
      this.setStatus(error.message, true);
      this.showOverlay(`Could not load dataset ${this.currentDatasetId}.`);
      return;
    }
    const artifacts = Array.isArray(this.currentDataset.artifacts) ? this.currentDataset.artifacts : [];
    this.refs.artifact.innerHTML = "";
    for (const artifact of artifacts) {
      const option = document.createElement("option");
      option.value = artifact.logical_name;
      option.textContent = `${artifact.logical_name} (${formatBytes(artifact.size)})`;
      this.refs.artifact.appendChild(option);
    }
    if (!artifacts.length) {
      this.currentArtifact = "";
      this.showOverlay(`Dataset ${this.currentDatasetId} does not contain artifacts.`);
      this.setStatus("Selected dataset has no artifacts.", true);
      return;
    }
    const storedArtifact =
      this.uiState.project === this.currentProject && this.uiState.datasetId === this.currentDatasetId
        ? this.uiState.artifact
        : "";
    this.currentArtifact = artifacts.some(artifact => artifact.logical_name === storedArtifact)
      ? storedArtifact
      : artifacts[0].logical_name;
    this.refs.artifact.value = this.currentArtifact;
    await this.loadArtifact();
  }

  async loadArtifact() {
    this.currentArtifactEntry = (this.currentDataset?.artifacts || []).find(
      artifact => artifact.logical_name === this.currentArtifact,
    ) || null;
    if (!this.currentArtifactEntry) {
      return;
    }
    this.saveUiState();
    const sizeLabel = formatBytes(this.currentArtifactEntry.size);
    this.setStatus(`Loading ${this.currentArtifact} from ${this.currentDatasetId} (${sizeLabel})…`);
    try {
      const text = await this.fetchText(
        `/api/project/${encodeURIComponent(this.currentProject)}/artifact/${encodeURIComponent(this.currentDatasetId)}/${encodeURIComponent(this.currentArtifact)}`,
      );
      this.data = summarizeTrajectoryCsv(text, this.currentArtifact);
      this.currentTimeMs = this.data.minTimeMs;
      this.refs.slider.min = String(this.data.minTimeMs);
      this.refs.slider.max = String(this.data.maxTimeMs);
      this.refs.slider.value = String(this.currentTimeMs);
      this.renderSummary();
      this.renderIndividuals();
      this.updateTimeLabel();
      this.hideOverlay();
      await this.rebuildMap(false);
      this.setStatus(
        `Loaded ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}.`,
      );
    } catch (error) {
      this.data = null;
      this.renderSummary();
      this.renderIndividuals();
      this.renderLayers();
      this.setStatus(error.message, true);
      this.showOverlay(`Could not render ${this.currentArtifact}.`);
    }
  }

  renderSummary() {
    if (!this.data) {
      this.refs.summary.innerHTML = "";
      return;
    }
    this.refs.summary.innerHTML = `
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Rows used:</strong> ${escapeHtml(formatCount(this.data.totalRows))}</div>
      <div><strong>Individuals:</strong> ${escapeHtml(formatCount(this.data.individuals.length))}</div>
      <div><strong>Time range:</strong> ${escapeHtml(formatTimestamp(this.data.minTimeMs))} to ${escapeHtml(formatTimestamp(this.data.maxTimeMs))}</div>
    `;
  }

  renderIndividuals() {
    this.refs.individuals.innerHTML = "";
    if (!this.data) {
      return;
    }
    for (const individual of this.data.individuals) {
      const stats = this.data.stats[individual];
      const coverage = this.data.coverageByIndividual[individual] || {};
      const card = document.createElement("div");
      card.className = "traj-individual";
      card.appendChild(htmlElement(`
        <div class="traj-name">
          <span>${escapeHtml(individual)}</span>
          <span>${escapeHtml(formatCount(stats.rowCount))}</span>
        </div>
      `));
      if (this.data.speciesByIndividual[individual]) {
        card.appendChild(htmlElement(`<div class="traj-species">${escapeHtml(this.data.speciesByIndividual[individual])}</div>`));
      }
      card.appendChild(htmlElement(`
        <div class="traj-stats">
          <span>median fix ${escapeHtml(formatMaybeNumber(stats.medianFixS, "s"))}</span>
          <span>median step ${escapeHtml(formatMaybeNumber(stats.medianStepM, "m"))}</span>
          <span>p95 step ${escapeHtml(formatMaybeNumber(stats.p95StepM, "m"))}</span>
        </div>
      `));
      const track = document.createElement("div");
      track.className = "traj-track";
      for (const setName of visibleSets(this.refs.showTrain.checked, this.refs.showTest.checked)) {
        const bar = coverage[setName];
        if (!bar) {
          continue;
        }
        const bounds = rangeToPercent(this.data.minTimeMs, this.data.maxTimeMs, bar.startMs, bar.endMs);
        const el = document.createElement("div");
        el.className = "traj-bar";
        el.style.left = `${bounds.left}%`;
        el.style.width = `${bounds.width}%`;
        el.style.background = colorCss(this.data.palette[individual], setName, 0.88);
        track.appendChild(el);
      }
      card.appendChild(track);
      this.refs.individuals.appendChild(card);
    }
  }

  async rebuildMap(forceStyleReload) {
    if (!this.data) {
      this.renderLayers();
      return;
    }
    const style = BASEMAP_STYLES[this.refs.basemap.value] || BASEMAP_STYLES.Positron;
    if (!this.map) {
      this.map = new maplibregl.Map({
        container: this.refs.map,
        style,
        center: [this.data.initialView.longitude, this.data.initialView.latitude],
        zoom: this.data.initialView.zoom,
        attributionControl: false,
      });
      this.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
      this.map.on("load", () => {
        this.mapLoaded = true;
        this.renderLayers();
      });
      this.overlay = new deck.MapboxOverlay({ interleaved: true, layers: [] });
      this.map.addControl(this.overlay);
      return;
    }
    if (forceStyleReload) {
      this.mapLoaded = false;
      this.map.setStyle(style);
      this.map.once("style.load", () => {
        this.mapLoaded = true;
        this.renderLayers();
      });
      return;
    }
    this.renderLayers();
  }

  renderLayers() {
    if (!this.data || !this.mapLoaded || !this.overlay) {
      if (this.overlay) {
        this.overlay.setProps({ layers: [] });
      }
      return;
    }
    const pathData = [];
    const pointData = [];
    for (const individual of this.data.individuals) {
      for (const setName of visibleSets(this.refs.showTrain.checked, this.refs.showTest.checked)) {
        const series = this.data.seriesByIndividual[individual]?.[setName];
        if (!series) {
          continue;
        }
        const filtered = filterSeriesByTime(series, this.currentTimeMs);
        if (filtered.positions.length < 2) {
          if (this.refs.showPoints.checked && filtered.positions.length === 1) {
            pointData.push({
              individual,
              setName,
              position: filtered.positions[0],
              color: splitColor(this.data.palette[individual], setName, POINT_ALPHA),
            });
          }
          continue;
        }
        pathData.push({
          individual,
          setName,
          path: filtered.positions,
          color: splitColor(this.data.palette[individual], setName, PATH_ALPHA),
        });
        if (this.refs.showPoints.checked) {
          pointData.push(...filtered.positions.map(position => ({
            individual,
            setName,
            position,
            color: splitColor(this.data.palette[individual], setName, POINT_ALPHA),
          })));
        }
      }
    }

    const layers = [
      new deck.PathLayer({
        id: "trajectory-paths",
        data: pathData,
        getPath: item => item.path,
        getColor: item => item.color,
        getWidth: 3,
        widthMinPixels: 2,
        pickable: false,
      }),
    ];

    if (this.refs.showPoints.checked) {
      layers.push(
        new deck.ScatterplotLayer({
          id: "trajectory-points",
          data: pointData,
          getPosition: item => item.position,
          getFillColor: item => item.color,
          getRadius: 36,
          radiusMinPixels: 2,
          radiusMaxPixels: 6,
          pickable: false,
        }),
      );
    }
    this.overlay.setProps({ layers });
  }

  resetView() {
    if (!this.data || !this.map) {
      return;
    }
    this.map.jumpTo({
      center: [this.data.initialView.longitude, this.data.initialView.latitude],
      zoom: this.data.initialView.zoom,
      bearing: 0,
      pitch: 0,
    });
  }

  updateTimeLabel() {
    if (!this.data) {
      this.refs.time.textContent = "No timestamps";
      return;
    }
    this.refs.time.textContent = formatTimestamp(this.currentTimeMs);
  }

  setStatus(message, isError = false) {
    this.refs.status.textContent = message;
    this.refs.status.classList.toggle("error", isError);
  }

  showOverlay(message) {
    this.refs.overlay.classList.remove("hidden");
    this.refs.overlay.querySelector("p").textContent = message;
  }

  hideOverlay() {
    this.refs.overlay.classList.add("hidden");
  }
}

function summarizeTrajectoryCsv(text, artifactName) {
  let header = null;
  let columns = null;
  let totalRows = 0;
  let minLon = Number.POSITIVE_INFINITY;
  let maxLon = Number.NEGATIVE_INFINITY;
  let minLat = Number.POSITIVE_INFINITY;
  let maxLat = Number.NEGATIVE_INFINITY;
  let minTimeMs = Number.POSITIVE_INFINITY;
  let maxTimeMs = Number.NEGATIVE_INFINITY;
  const speciesByIndividual = {};
  const rowCounts = {};
  const groups = new Map();
  const statsByIndividual = new Map();

  forEachCsvRow(text, row => {
    if (!header) {
      header = row;
      columns = detectColumns(header);
      if (!columns.individual || !columns.time || !columns.lon || !columns.lat) {
        throw new Error(`${artifactName} is missing required trajectory columns.`);
      }
      return;
    }
    if (!row.length || row.every(value => value === "")) {
      return;
    }

    const individual = getField(row, header, columns.individual).trim();
    if (!individual) {
      return;
    }
    const timeMs = parseTimestampMs(getField(row, header, columns.time));
    if (timeMs == null) {
      return;
    }
    const lon = Number.parseFloat(getField(row, header, columns.lon));
    const lat = Number.parseFloat(getField(row, header, columns.lat));
    if (!Number.isFinite(lon) || !Number.isFinite(lat) || lon < -180 || lon > 180 || lat < -90 || lat > 90) {
      return;
    }

    const setName = normalizeSet(getField(row, header, columns.set));
    const commonName = columns.commonName ? getField(row, header, columns.commonName).trim() : "";
    const scientificName = columns.scientificName ? getField(row, header, columns.scientificName).trim() : "";
    if (!speciesByIndividual[individual]) {
      speciesByIndividual[individual] = commonName || scientificName || "Unknown species";
    }

    totalRows += 1;
    rowCounts[individual] = (rowCounts[individual] || 0) + 1;
    minLon = Math.min(minLon, lon);
    maxLon = Math.max(maxLon, lon);
    minLat = Math.min(minLat, lat);
    maxLat = Math.max(maxLat, lat);
    minTimeMs = Math.min(minTimeMs, timeMs);
    maxTimeMs = Math.max(maxTimeMs, timeMs);

    const groupKey = `${individual}__${setName}`;
    const group = groups.get(groupKey) || {
      individual,
      setName,
      startMs: timeMs,
      endMs: timeMs,
      seen: 0,
      samples: [],
      prev: null,
    };
    group.seen += 1;
    group.startMs = Math.min(group.startMs, timeMs);
    group.endMs = Math.max(group.endMs, timeMs);
    reservoirAppend(group.samples, [timeMs, lon, lat], group.seen, MAX_POINTS_PER_SERIES);
    if (group.prev && timeMs > group.prev[0]) {
      const deltaS = (timeMs - group.prev[0]) / 1000;
      const stepM = haversineMeters(group.prev[1], group.prev[2], lon, lat);
      const stats = statsByIndividual.get(individual) || {
        seenFix: 0,
        seenStep: 0,
        fix: [],
        step: [],
      };
      stats.seenFix += 1;
      stats.seenStep += 1;
      reservoirAppend(stats.fix, deltaS, stats.seenFix, 5000);
      reservoirAppend(stats.step, stepM, stats.seenStep, 5000);
      statsByIndividual.set(individual, stats);
    }
    group.prev = [timeMs, lon, lat];
    groups.set(groupKey, group);
  });

  if (!Number.isFinite(minTimeMs) || !Number.isFinite(maxTimeMs) || totalRows === 0) {
    throw new Error(`${artifactName} did not contain usable trajectory rows.`);
  }

  const individuals = Object.keys(rowCounts).sort((left, right) => left.localeCompare(right));
  const seriesByIndividual = {};
  const coverageByIndividual = {};
  for (const group of groups.values()) {
    const sorted = [...group.samples].sort((left, right) => left[0] - right[0]);
    seriesByIndividual[group.individual] ||= {};
    seriesByIndividual[group.individual][group.setName] = {
      times: sorted.map(item => item[0]),
      positions: sorted.map(item => [item[1], item[2]]),
    };
    coverageByIndividual[group.individual] ||= {};
    coverageByIndividual[group.individual][group.setName] = {
      startMs: group.startMs,
      endMs: group.endMs,
    };
  }

  const stats = {};
  const palette = {};
  individuals.forEach((individual, index) => {
    const item = statsByIndividual.get(individual) || { fix: [], step: [] };
    stats[individual] = {
      rowCount: rowCounts[individual] || 0,
      medianFixS: median(item.fix),
      medianStepM: median(item.step),
      p95StepM: quantile(item.step, 0.95),
    };
    palette[individual] = hslToRgb((index * 137.508) % 360, 0.76, 0.54);
  });

  return {
    totalRows,
    individuals,
    speciesByIndividual,
    seriesByIndividual,
    coverageByIndividual,
    stats,
    palette,
    minTimeMs,
    maxTimeMs,
    initialView: {
      longitude: (minLon + maxLon) / 2,
      latitude: (minLat + maxLat) / 2,
      zoom: spanToZoom(Math.max(maxLon - minLon, maxLat - minLat)),
    },
  };
}

function forEachCsvRow(text, onRow) {
  let row = [];
  let field = "";
  let inQuotes = false;
  for (let index = 0; index < text.length; index += 1) {
    const ch = text[index];
    const next = text[index + 1];
    if (inQuotes) {
      if (ch === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        field += ch;
      }
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      field = "";
      onRow(row);
      row = [];
    } else if (ch === "\r") {
      continue;
    } else {
      field += ch;
    }
  }
  if (field.length || row.length) {
    row.push(field);
    onRow(row);
  }
}

function normalizeHeader(header) {
  return String(header || "").toLowerCase().replace(/[-_: ]/g, "");
}

function detectColumns(header) {
  const normalizedMap = new Map(header.map(name => [normalizeHeader(name), name]));
  return {
    individual: findColumn(normalizedMap, ["individual", "individualid", "individuallocalidentifier", "animalid", "trackid", "taglocalidentifier", "id"]),
    time: findColumn(normalizedMap, ["timestamp", "time", "datetime", "eventtime", "transmissiontimestamp"]),
    lon: findColumn(normalizedMap, ["longitude", "lon", "locationlong", "stependlocationlong", "x"]),
    lat: findColumn(normalizedMap, ["latitude", "lat", "locationlat", "stependlocationlat", "y"]),
    commonName: findColumn(normalizedMap, ["individualtaxoncommonname", "taxoncommonname", "commonname", "speciescommonname", "vernacularname", "animalcommonname"]),
    scientificName: findColumn(normalizedMap, ["individualtaxoncanonicalname", "taxoncanonicalname", "scientificname", "species", "taxon"]),
    set: findColumn(normalizedMap, ["set", "split", "partition"]),
  };
}

function findColumn(map, aliases) {
  for (const alias of aliases) {
    if (map.has(alias)) {
      return map.get(alias);
    }
  }
  return null;
}

function getField(row, header, columnName) {
  if (!columnName) {
    return "";
  }
  const index = header.indexOf(columnName);
  return index >= 0 ? String(row[index] || "") : "";
}

function normalizeSet(value) {
  return String(value || "").trim().toLowerCase() === "test" ? "test" : "train";
}

function parseTimestampMs(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }
  const direct = Date.parse(text);
  if (Number.isFinite(direct)) {
    return direct;
  }
  const fallback = Date.parse(text.replace(" ", "T"));
  return Number.isFinite(fallback) ? fallback : null;
}

function reservoirAppend(sample, item, seenCount, limit) {
  if (sample.length < limit) {
    sample.push(item);
    return;
  }
  const slot = Math.floor(Math.random() * seenCount);
  if (slot < limit) {
    sample[slot] = item;
  }
}

function median(values) {
  if (!values.length) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

function quantile(values, q) {
  if (!values.length) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const idx = (sorted.length - 1) * q;
  const lower = Math.floor(idx);
  const upper = Math.min(sorted.length - 1, lower + 1);
  if (lower === upper) {
    return sorted[lower];
  }
  const ratio = idx - lower;
  return sorted[lower] + (sorted[upper] - sorted[lower]) * ratio;
}

function haversineMeters(lon1, lat1, lon2, lat2) {
  const toRad = degrees => degrees * Math.PI / 180;
  const phi1 = toRad(lat1);
  const phi2 = toRad(lat2);
  const deltaPhi = toRad(lat2 - lat1);
  const deltaLambda = toRad(lon2 - lon1);
  const a = Math.sin(deltaPhi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(deltaLambda / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function spanToZoom(spanDegrees) {
  if (spanDegrees <= 0) return 2;
  if (spanDegrees > 120) return 1;
  if (spanDegrees > 60) return 2;
  if (spanDegrees > 30) return 3;
  if (spanDegrees > 15) return 4;
  if (spanDegrees > 8) return 5;
  if (spanDegrees > 4) return 6;
  if (spanDegrees > 2) return 7;
  if (spanDegrees > 1) return 8;
  if (spanDegrees > 0.5) return 9;
  if (spanDegrees > 0.25) return 10;
  return 11;
}

function filterSeriesByTime(series, currentTimeMs) {
  const positions = [];
  for (let index = 0; index < series.times.length; index += 1) {
    if (series.times[index] > currentTimeMs) {
      break;
    }
    positions.push(series.positions[index]);
  }
  return { positions };
}

function visibleSets(showTrain, showTest) {
  const sets = [];
  if (showTrain) sets.push("train");
  if (showTest) sets.push("test");
  return sets;
}

function splitColor(baseColor, setName, alpha) {
  const factor = setName === "test" ? 0.7 : 1.12;
  return [
    clampColor(baseColor[0] * factor),
    clampColor(baseColor[1] * factor),
    clampColor(baseColor[2] * factor),
    alpha,
  ];
}

function colorCss(baseColor, setName, alpha) {
  const color = splitColor(baseColor, setName, Math.round(alpha * 255));
  return `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${alpha})`;
}

function clampColor(value) {
  return Math.max(0, Math.min(255, Math.round(value)));
}

function hslToRgb(h, s, l) {
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const hp = h / 60;
  const x = c * (1 - Math.abs(hp % 2 - 1));
  let rgb = [0, 0, 0];
  if (hp >= 0 && hp < 1) rgb = [c, x, 0];
  else if (hp < 2) rgb = [x, c, 0];
  else if (hp < 3) rgb = [0, c, x];
  else if (hp < 4) rgb = [0, x, c];
  else if (hp < 5) rgb = [x, 0, c];
  else rgb = [c, 0, x];
  const m = l - c / 2;
  return rgb.map(value => Math.round((value + m) * 255));
}

function rangeToPercent(min, max, start, end) {
  const span = Math.max(1, max - min);
  const left = ((start - min) / span) * 100;
  const width = Math.max(0.8, ((end - start) / span) * 100);
  return { left, width };
}

function formatDatasetLabel(dataset, currentDatasetId) {
  const currentSuffix = dataset.dataset_id === currentDatasetId ? " (head)" : "";
  const note = dataset.note ? `${dataset.note} ` : "";
  return `${note}[${dataset.dataset_id}]${currentSuffix}`;
}

function formatBytes(value) {
  if (!Number.isFinite(value)) {
    return "unknown size";
  }
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatCount(value) {
  return new Intl.NumberFormat().format(value);
}

function formatTimestamp(value) {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toLocaleString() : "Unknown time";
}

function formatMaybeNumber(value, suffix) {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  return `${value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${suffix}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function htmlElement(html) {
  const template = document.createElement("template");
  template.innerHTML = html.trim();
  return template.content.firstElementChild;
}
