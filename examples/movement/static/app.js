const LOCAL_BLANK_STYLE = {
  version: 8,
  sources: {},
  layers: [
    {
      id: "background",
      type: "background",
      paint: {
        "background-color": "#08111b",
      },
    },
  ],
};

const BASEMAP_STYLES = {
  Blank: LOCAL_BLANK_STYLE,
  Positron: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  Voyager: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
  "Dark Matter": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
};

const PATH_ALPHA = 110;
const POINT_ALPHA = 215;
const STORAGE_VERSION = 3;
const MAX_SELECTED_FIXES_SHOWN = 150;
const DEFAULT_FAMILY = "movement_clean";
const NUMERIC_COLOR_MIN_QUANTILE = 0.01;
const NUMERIC_COLOR_MAX_QUANTILE = 0.99;

let assetPromise = null;

async function ensureAssetsLoaded() {
  if (assetPromise) {
    return assetPromise;
  }
  assetPromise = Promise.all([
    loadCss("/static/vendor/maplibre-gl/maplibre-gl.css"),
    loadScript("/static/vendor/maplibre-gl/maplibre-gl.js"),
    loadScript("/static/vendor/deckgl/deck.gl.min.js"),
    loadScript("/static/vendor/deckgl/deck.gl-mapbox.min.js"),
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

class MovementExampleApp {
  constructor({ mountEl }) {
    this.mountEl = mountEl;
    this.uiState = this.loadUiState();
    this.families = [];
    this.studies = [];
    this.graph = null;
    this.allDatasets = [];
    this.datasets = [];
    this.stepByOutputDatasetId = new Map();
    this.currentFamily = "";
    this.currentStudy = "";
    this.currentDatasetId = "";
    this.currentArtifact = "";
    this.currentDataset = null;
    this.currentArtifactEntry = null;
    this.data = null;
    this.currentTimeMs = 0;
    this.loadRequestId = 0;
    this.studyLoadId = 0;
    this.datasetLoadId = 0;
    this.requestControllers = {
      families: null,
      studies: null,
      study: null,
      dataset: null,
      overview: null,
      detail: null,
    };
    this.map = null;
    this.mapLoaded = false;
    this.overlay = null;
    this.pendingIssueStatus = "suspected";
    this.lastReportLinks = [];
    this.assetsLoaded = false;
    this.mapErrorMessage = "";
  }

  async init() {
    this.renderShell();
    this.bindEvents();
    this.showOverlay("Loading the movement review workspace...");
    this.setStatus("Loading movement review workspace...");
    try {
      await ensureAssetsLoaded();
      this.assetsLoaded = true;
      await this.rebuildMap(false);
    } catch (error) {
      this.assetsLoaded = false;
      this.setStatus(`Map assets could not be loaded: ${error.message}`, true);
      this.showOverlay("Map assets could not be loaded. You can still switch studies and inspect summaries.");
    }
    await this.loadFamilies();
  }

  loadUiState() {
    try {
      const raw = localStorage.getItem("vibecleaning_movement_example_state");
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
        family: familyPresetFromLocation() || DEFAULT_FAMILY,
        study: "",
        basemap: "Blank",
        showTrain: true,
        showTest: true,
        showPoints: true,
        colorBy: "step_length_m",
      };
    }
  }

  saveUiState() {
    this.uiState = {
      version: STORAGE_VERSION,
      family: this.currentFamily,
      study: this.currentStudy,
      basemap: this.refs.basemap.value,
      showTrain: this.refs.showTrain.checked,
      showTest: this.refs.showTest.checked,
      showPoints: this.refs.showPoints.checked,
      colorBy: this.refs.colorBy.value,
    };
    localStorage.setItem("vibecleaning_movement_example_state", JSON.stringify(this.uiState));
  }

  getUser() {
    return localStorage.getItem("vibecleaning_user_name") || "";
  }

  setUser(user) {
    localStorage.setItem("vibecleaning_user_name", user);
  }

  renderShell() {
    this.mountEl.innerHTML = `
      <style>
        .movement-root {
          display: grid;
          grid-template-rows: auto auto auto minmax(0, 1fr);
          min-height: 100%;
          height: 100%;
          color: #e8eef7;
          font-family: "Segoe UI", sans-serif;
          background:
            radial-gradient(circle at top left, rgba(114, 252, 196, 0.08), transparent 28%),
            radial-gradient(circle at bottom right, rgba(72, 187, 255, 0.12), transparent 24%),
            linear-gradient(180deg, rgba(6, 12, 20, 0.98), rgba(4, 8, 14, 0.98));
        }
        .movement-toolbar {
          display: flex;
          flex-wrap: wrap;
          gap: 10px 14px;
          align-items: center;
          padding: 14px 16px 10px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.06);
          background: rgba(8, 17, 27, 0.82);
        }
        .movement-toolbar label,
        .movement-toggle {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-toolbar select,
        .movement-toolbar button,
        .movement-modal input,
        .movement-modal textarea {
          font: inherit;
        }
        .movement-toolbar select,
        .movement-modal input,
        .movement-modal textarea {
          min-width: 150px;
          padding: 8px 10px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
        }
        .movement-toolbar button,
        .movement-modal button {
          padding: 8px 12px;
          border: none;
          border-radius: 10px;
          background: rgba(255, 255, 255, 0.08);
          color: #e5edf7;
          cursor: pointer;
          font-size: 14px;
          white-space: nowrap;
        }
        .movement-toolbar button.movement-emphasis,
        .movement-modal button.movement-emphasis {
          background: rgba(67, 206, 162, 0.22);
          color: #d8fff3;
        }
        .movement-toolbar button.movement-danger,
        .movement-modal button.movement-danger {
          background: rgba(220, 38, 38, 0.24);
          color: #ffe5e8;
        }
        .movement-toolbar button:disabled,
        .movement-modal button:disabled {
          cursor: not-allowed;
          opacity: 0.45;
        }
        .movement-status {
          min-height: 24px;
          padding: 0 16px 10px;
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-status.error,
        .movement-modal-status.error {
          color: #ffb3c2;
        }
        .movement-main {
          display: grid;
          grid-template-columns: minmax(0, 1.25fr) minmax(360px, 0.85fr);
          gap: 12px;
          min-height: 0;
          padding: 0 16px 14px;
        }
        .movement-map-wrap,
        .movement-side {
          min-height: 0;
          border-radius: 16px;
          overflow: hidden;
          border: 1px solid rgba(255, 255, 255, 0.07);
          background: rgba(255, 255, 255, 0.03);
        }
        .movement-map-wrap {
          position: relative;
          background: #08111b;
        }
        .movement-map {
          width: 100%;
          height: 100%;
        }
        .movement-legend {
          position: absolute;
          left: 16px;
          bottom: 16px;
          z-index: 4;
          width: min(260px, calc(100% - 32px));
          display: grid;
          gap: 10px;
          padding: 12px 13px;
          border-radius: 14px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(7, 11, 22, 0.88);
          box-shadow: 0 18px 44px rgba(0, 0, 0, 0.32);
          color: #e8eef7;
          pointer-events: none;
        }
        .movement-legend.hidden {
          display: none;
        }
        .movement-legend-head {
          display: grid;
          gap: 2px;
        }
        .movement-legend-title {
          font-size: 12px;
          font-weight: 600;
          color: #eef4fb;
        }
        .movement-legend-subtitle {
          font-size: 11px;
          color: #8fa5bc;
        }
        .movement-legend-scale {
          display: grid;
          gap: 6px;
        }
        .movement-legend-gradient {
          height: 12px;
          border-radius: 999px;
          border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .movement-legend-range {
          display: flex;
          justify-content: space-between;
          gap: 10px;
          font-size: 11px;
          color: #c8d5e4;
        }
        .movement-legend-note {
          font-size: 10px;
          color: #7e93aa;
        }
        .movement-legend-items {
          display: grid;
          gap: 6px;
        }
        .movement-legend-item {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 10px;
          font-size: 11px;
          color: #dbe5f0;
        }
        .movement-legend-item-label {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .movement-legend-swatch {
          flex: 0 0 auto;
          width: 16px;
          height: 16px;
          border-radius: 999px;
          border: 1px solid rgba(255, 255, 255, 0.16);
          box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.04);
        }
        .movement-overlay {
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
        .movement-overlay.hidden {
          display: none;
        }
        .movement-overlay-card {
          max-width: 540px;
          padding: 18px 20px;
          border-radius: 16px;
          background: rgba(7, 11, 22, 0.88);
          border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .movement-overlay-card h3 {
          font-size: 16px;
          margin-bottom: 8px;
        }
        .movement-overlay-card p {
          font-size: 13px;
          line-height: 1.5;
        }
        .movement-side {
          display: grid;
          grid-template-rows: auto minmax(0, 0.95fr) auto minmax(0, 1.05fr) auto;
        }
        .movement-side-head,
        .movement-slider-row {
          padding: 12px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-individuals,
        .movement-fixes {
          overflow-y: auto;
          padding: 10px 12px;
        }
        .movement-card {
          display: grid;
          gap: 6px;
          padding: 10px 12px;
          margin-bottom: 10px;
          border-radius: 12px;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.05);
        }
        .movement-card.interactive {
          cursor: pointer;
        }
        .movement-row {
          display: flex;
          justify-content: space-between;
          gap: 10px;
          align-items: center;
          min-width: 0;
        }
        .movement-row-left {
          display: flex;
          gap: 8px;
          align-items: center;
          min-width: 0;
        }
        .movement-title {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          color: #eef4fb;
          font-size: 12px;
        }
        .movement-subtle,
        .movement-stats,
        .movement-fix-meta,
        .movement-fix-note {
          color: #93a7bd;
          font-size: 11px;
        }
        .movement-stats {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }
        .movement-track {
          position: relative;
          height: 10px;
          width: 100%;
          border-radius: 999px;
          background: rgba(255, 255, 255, 0.06);
          overflow: hidden;
        }
        .movement-bar {
          position: absolute;
          top: 0;
          height: 100%;
          border-radius: 999px;
        }
        .movement-pill {
          padding: 2px 7px;
          border-radius: 999px;
          font-size: 10px;
          text-transform: uppercase;
          letter-spacing: 0.04em;
          border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .movement-pill.suspected {
          background: rgba(251, 191, 36, 0.16);
          color: #f7d48e;
        }
        .movement-pill.confirmed {
          background: rgba(248, 113, 113, 0.18);
          color: #ffced6;
        }
        .movement-pill.unreviewed {
          background: rgba(148, 163, 184, 0.12);
          color: #b8c5d3;
        }
        .movement-slider-row {
          display: grid;
          gap: 10px;
          border-bottom: none;
        }
        .movement-slider {
          width: 100%;
        }
        .movement-time {
          font-size: 12px;
          color: #dbe5f0;
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        }
        .movement-empty {
          padding: 18px 12px;
          color: #8ea1b7;
          font-size: 12px;
          line-height: 1.5;
        }
        .movement-fix-remove {
          background: transparent;
          border: 1px solid rgba(255, 255, 255, 0.08);
          color: #e8eef7;
          border-radius: 8px;
          padding: 4px 7px;
          font-size: 11px;
          cursor: pointer;
        }
        .movement-modal {
          position: fixed;
          inset: 0;
          z-index: 20;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 24px;
          background: rgba(2, 6, 12, 0.72);
          backdrop-filter: blur(8px);
        }
        .movement-modal.hidden {
          display: none;
        }
        .movement-modal-card {
          width: min(620px, 100%);
          border-radius: 18px;
          overflow: hidden;
          background: rgba(7, 11, 22, 0.96);
          border: 1px solid rgba(255, 255, 255, 0.08);
          box-shadow: 0 24px 60px rgba(0, 0, 0, 0.45);
        }
        .movement-modal-head,
        .movement-modal-foot {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 12px;
          padding: 14px 16px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }
        .movement-modal-foot {
          border-bottom: none;
          border-top: 1px solid rgba(255, 255, 255, 0.08);
        }
        .movement-modal-body {
          display: grid;
          gap: 14px;
          padding: 16px;
        }
        .movement-modal-body label {
          display: grid;
          gap: 6px;
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-modal textarea {
          min-height: 88px;
          resize: vertical;
        }
        .movement-selection-list {
          max-height: 150px;
          overflow: auto;
          padding: 10px 12px;
          border-radius: 12px;
          background: rgba(255, 255, 255, 0.04);
          border: 1px solid rgba(255, 255, 255, 0.06);
          font-size: 12px;
          color: #dbe4ef;
          line-height: 1.5;
        }
        .movement-links {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          font-size: 12px;
        }
        .movement-links a {
          color: #9df6dc;
        }
        @media (max-width: 1080px) {
          .movement-main {
            grid-template-columns: 1fr;
            grid-template-rows: minmax(320px, 1fr) minmax(320px, 1fr);
          }
          .movement-side {
            grid-template-rows: auto minmax(220px, 0.7fr) auto minmax(220px, 0.9fr) auto;
          }
        }
      </style>
      <div class="movement-root">
        <div class="movement-toolbar">
          <label>Family <select data-role="family"></select></label>
          <label>Study <select data-role="study"></select></label>
          <label>Version <select data-role="dataset"></select></label>
          <label>Artifact <select data-role="artifact"></select></label>
          <label>Basemap <select data-role="basemap"></select></label>
          <label>Color by <select data-role="color-by"></select></label>
          <label class="movement-toggle"><input type="checkbox" data-role="show-train"> Train</label>
          <label class="movement-toggle"><input type="checkbox" data-role="show-test"> Test</label>
          <label class="movement-toggle"><input type="checkbox" data-role="show-points"> Points</label>
          <button type="button" data-role="select-all">All individuals</button>
          <button type="button" data-role="select-none">No individuals</button>
          <button type="button" data-role="select-suspicious">Select all suspicious</button>
          <button type="button" data-role="clear-fixes">Clear checked fixes</button>
          <button type="button" data-role="reset-view">Reset view</button>
          <button type="button" class="movement-emphasis" data-role="mark-suspected">Mark suspected</button>
          <button type="button" class="movement-emphasis" data-role="mark-confirmed">Mark confirmed</button>
          <button type="button" data-role="generate-report">Generate report</button>
          <button type="button" class="movement-danger" data-role="remove-confirmed">Remove confirmed</button>
          <button type="button" data-role="undo">Undo</button>
        </div>
        <div class="movement-status" data-role="status"></div>
        <div class="movement-main">
          <div class="movement-map-wrap">
            <div class="movement-map" data-role="map"></div>
            <div class="movement-legend hidden" data-role="legend"></div>
            <div class="movement-overlay" data-role="overlay">
              <div class="movement-overlay-card">
                <h3>Movement Outlier Review</h3>
                <p>Review fixes on the map, color them by GPS-quality or movement-derived attributes, mark suspected or confirmed issues, and generate reports for the data owners.</p>
              </div>
            </div>
          </div>
          <div class="movement-side">
            <div class="movement-side-head" data-role="individual-head">Individuals and coverage</div>
            <div class="movement-individuals" data-role="individuals"></div>
            <div class="movement-side-head" data-role="fix-head">Checked fixes</div>
            <div class="movement-fixes" data-role="selected-fixes"></div>
            <div class="movement-slider-row">
              <input class="movement-slider" data-role="slider" type="range" min="0" max="0" value="0" step="1">
              <div class="movement-time" data-role="time"></div>
            </div>
          </div>
        </div>
      </div>

      <div class="movement-modal hidden" data-role="issue-modal">
        <div class="movement-modal-card">
          <div class="movement-modal-head">
            <h3 data-role="issue-title">Mark fixes</h3>
            <button type="button" data-role="issue-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div data-role="issue-meta"></div>
            <div class="movement-selection-list" data-role="issue-selection"></div>
            <label>User
              <input type="text" data-role="issue-user" placeholder="Name used for attribution">
            </label>
            <label>Issue type
              <input type="text" data-role="issue-type" placeholder="e.g. large displacement, odd motion, bad gps signal">
            </label>
            <label>Issue description
              <textarea data-role="issue-note" placeholder="Describe why these fixes look problematic."></textarea>
            </label>
            <label>Question for data owner
              <textarea data-role="issue-question" placeholder="What do you want the data owner to confirm or explain?"></textarea>
            </label>
            <div class="movement-modal-status" data-role="issue-status"></div>
          </div>
          <div class="movement-modal-foot">
            <span></span>
            <button type="button" class="movement-emphasis" data-role="issue-submit">Create step</button>
          </div>
        </div>
      </div>

      <div class="movement-modal hidden" data-role="report-modal">
        <div class="movement-modal-card">
          <div class="movement-modal-head">
            <h3>Generate owner report</h3>
            <button type="button" data-role="report-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div data-role="report-meta"></div>
            <div class="movement-selection-list" data-role="report-selection"></div>
            <label>User
              <input type="text" data-role="report-user" placeholder="Name used for attribution">
            </label>
            <label>Individual
              <select data-role="report-individual"></select>
            </label>
            <label>Screenshot mode
              <select data-role="report-screenshot-mode">
                <option value="manual">Manual placeholder</option>
                <option value="auto">Auto snapshot of current map</option>
              </select>
            </label>
            <div class="movement-links" data-role="report-links"></div>
            <div class="movement-modal-status" data-role="report-status"></div>
          </div>
          <div class="movement-modal-foot">
            <span></span>
            <button type="button" data-role="report-submit">Create analysis</button>
          </div>
        </div>
      </div>

      <div class="movement-modal hidden" data-role="remove-modal">
        <div class="movement-modal-card">
          <div class="movement-modal-head">
            <h3>Remove confirmed fixes</h3>
            <button type="button" data-role="remove-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div data-role="remove-meta"></div>
            <div class="movement-selection-list" data-role="remove-selection"></div>
            <label>User
              <input type="text" data-role="remove-user" placeholder="Name used for attribution">
            </label>
            <label>Reason
              <textarea data-role="remove-reason" placeholder="Why is it appropriate to remove these confirmed outliers now?"></textarea>
            </label>
            <div class="movement-modal-status" data-role="remove-status"></div>
          </div>
          <div class="movement-modal-foot">
            <span></span>
            <button type="button" class="movement-danger" data-role="remove-submit">Create step</button>
          </div>
        </div>
      </div>
    `;

    this.refs = {
      family: this.mountEl.querySelector('[data-role="family"]'),
      study: this.mountEl.querySelector('[data-role="study"]'),
      dataset: this.mountEl.querySelector('[data-role="dataset"]'),
      artifact: this.mountEl.querySelector('[data-role="artifact"]'),
      basemap: this.mountEl.querySelector('[data-role="basemap"]'),
      colorBy: this.mountEl.querySelector('[data-role="color-by"]'),
      showTrain: this.mountEl.querySelector('[data-role="show-train"]'),
      showTest: this.mountEl.querySelector('[data-role="show-test"]'),
      showPoints: this.mountEl.querySelector('[data-role="show-points"]'),
      selectAll: this.mountEl.querySelector('[data-role="select-all"]'),
      selectNone: this.mountEl.querySelector('[data-role="select-none"]'),
      selectSuspicious: this.mountEl.querySelector('[data-role="select-suspicious"]'),
      clearFixes: this.mountEl.querySelector('[data-role="clear-fixes"]'),
      resetView: this.mountEl.querySelector('[data-role="reset-view"]'),
      markSuspected: this.mountEl.querySelector('[data-role="mark-suspected"]'),
      markConfirmed: this.mountEl.querySelector('[data-role="mark-confirmed"]'),
      generateReport: this.mountEl.querySelector('[data-role="generate-report"]'),
      removeConfirmed: this.mountEl.querySelector('[data-role="remove-confirmed"]'),
      undo: this.mountEl.querySelector('[data-role="undo"]'),
      status: this.mountEl.querySelector('[data-role="status"]'),
      individuals: this.mountEl.querySelector('[data-role="individuals"]'),
      individualHead: this.mountEl.querySelector('[data-role="individual-head"]'),
      fixHead: this.mountEl.querySelector('[data-role="fix-head"]'),
      selectedFixes: this.mountEl.querySelector('[data-role="selected-fixes"]'),
      slider: this.mountEl.querySelector('[data-role="slider"]'),
      time: this.mountEl.querySelector('[data-role="time"]'),
      map: this.mountEl.querySelector('[data-role="map"]'),
      legend: this.mountEl.querySelector('[data-role="legend"]'),
      overlay: this.mountEl.querySelector('[data-role="overlay"]'),
      issueModal: this.mountEl.querySelector('[data-role="issue-modal"]'),
      issueTitle: this.mountEl.querySelector('[data-role="issue-title"]'),
      issueMeta: this.mountEl.querySelector('[data-role="issue-meta"]'),
      issueSelection: this.mountEl.querySelector('[data-role="issue-selection"]'),
      issueUser: this.mountEl.querySelector('[data-role="issue-user"]'),
      issueType: this.mountEl.querySelector('[data-role="issue-type"]'),
      issueNote: this.mountEl.querySelector('[data-role="issue-note"]'),
      issueQuestion: this.mountEl.querySelector('[data-role="issue-question"]'),
      issueStatus: this.mountEl.querySelector('[data-role="issue-status"]'),
      issueClose: this.mountEl.querySelector('[data-role="issue-close"]'),
      issueSubmit: this.mountEl.querySelector('[data-role="issue-submit"]'),
      reportModal: this.mountEl.querySelector('[data-role="report-modal"]'),
      reportMeta: this.mountEl.querySelector('[data-role="report-meta"]'),
      reportSelection: this.mountEl.querySelector('[data-role="report-selection"]'),
      reportUser: this.mountEl.querySelector('[data-role="report-user"]'),
      reportIndividual: this.mountEl.querySelector('[data-role="report-individual"]'),
      reportScreenshotMode: this.mountEl.querySelector('[data-role="report-screenshot-mode"]'),
      reportLinks: this.mountEl.querySelector('[data-role="report-links"]'),
      reportStatus: this.mountEl.querySelector('[data-role="report-status"]'),
      reportClose: this.mountEl.querySelector('[data-role="report-close"]'),
      reportSubmit: this.mountEl.querySelector('[data-role="report-submit"]'),
      removeModal: this.mountEl.querySelector('[data-role="remove-modal"]'),
      removeMeta: this.mountEl.querySelector('[data-role="remove-meta"]'),
      removeSelection: this.mountEl.querySelector('[data-role="remove-selection"]'),
      removeUser: this.mountEl.querySelector('[data-role="remove-user"]'),
      removeReason: this.mountEl.querySelector('[data-role="remove-reason"]'),
      removeStatus: this.mountEl.querySelector('[data-role="remove-status"]'),
      removeClose: this.mountEl.querySelector('[data-role="remove-close"]'),
      removeSubmit: this.mountEl.querySelector('[data-role="remove-submit"]'),
    };

    for (const name of Object.keys(BASEMAP_STYLES)) {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      this.refs.basemap.appendChild(option);
    }
    this.refs.basemap.value = BASEMAP_STYLES[this.uiState.basemap] ? this.uiState.basemap : "Blank";
    this.refs.showTrain.checked = this.uiState.showTrain !== false;
    this.refs.showTest.checked = this.uiState.showTest !== false;
    this.refs.showPoints.checked = this.uiState.showPoints !== false;
    this.updateActionButtons();
  }

  bindEvents() {
    this.refs.family.addEventListener("change", async () => {
      try {
        await this.switchFamily(this.refs.family.value);
      } catch (error) {
        console.error("Failed to switch movement family", error);
        this.setStatus(`Could not switch family: ${error.message}`, true);
        this.showOverlay("The family changed, but the study list could not be reloaded.");
      }
    });
    this.refs.study.addEventListener("change", async () => {
      try {
        this.currentStudy = this.refs.study.value;
        this.currentDatasetId = "";
        this.currentArtifact = "";
        this.currentDataset = null;
        this.saveUiState();
        await this.loadStudy();
      } catch (error) {
        console.error("Failed to switch movement study", error);
        this.setStatus(`Could not switch study: ${error.message}`, true);
        this.showOverlay("The study changed, but its lineage could not be loaded.");
      }
    });
    this.refs.dataset.addEventListener("change", async () => {
      this.currentDatasetId = this.refs.dataset.value;
      await this.loadDataset();
    });
    this.refs.artifact.addEventListener("change", async () => {
      this.currentArtifact = this.refs.artifact.value;
      this.saveUiState();
      if (!this.currentArtifact) {
        this.clearLoadedStudyState();
        this.showOverlay("Select a study to load movement tracks.");
        this.setStatus("Select a study to load the map.");
        return;
      }
      await this.loadArtifact();
    });
    this.refs.basemap.addEventListener("change", async () => {
      this.saveUiState();
      await this.rebuildMap(true);
    });
    this.refs.colorBy.addEventListener("change", () => {
      this.saveUiState();
      this.renderLegend();
      this.renderLayers();
      this.renderSelectedFixes();
    });
    this.refs.showTrain.addEventListener("change", () => this.handleVisibilityChange());
    this.refs.showTest.addEventListener("change", () => this.handleVisibilityChange());
    this.refs.showPoints.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
    });
    this.refs.selectAll.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedIndividuals = new Set(this.data.individuals);
      this.saveUiState();
      this.renderIndividuals();
      this.renderLayers();
      this.updateActionButtons();
      void this.loadDetailForCurrentSelection();
    });
    this.refs.selectNone.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedIndividuals = new Set();
      this.data.selectedFixKeys = new Set();
      this.saveUiState();
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
      void this.loadDetailForCurrentSelection();
    });
    this.refs.selectSuspicious.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedFixKeys = new Set(this.getSuspiciousFixes().map(fix => fix.fixKey));
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
    });
    this.refs.clearFixes.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedFixKeys = new Set();
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
    });
    this.refs.resetView.addEventListener("click", () => this.resetView());
    this.refs.markSuspected.addEventListener("click", () => this.openIssueModal("suspected"));
    this.refs.markConfirmed.addEventListener("click", () => this.openIssueModal("confirmed"));
    this.refs.generateReport.addEventListener("click", () => this.openReportModal());
    this.refs.removeConfirmed.addEventListener("click", () => this.openRemoveModal());
    this.refs.undo.addEventListener("click", async () => {
      await this.undoCurrentHead();
    });
    this.refs.slider.addEventListener("input", () => {
      this.currentTimeMs = Number(this.refs.slider.value) || 0;
      this.updateTimeLabel();
      this.renderLayers();
    });

    this.refs.issueClose.addEventListener("click", () => this.closeModal(this.refs.issueModal, this.refs.issueSubmit));
    this.refs.issueSubmit.addEventListener("click", async () => this.submitIssueAction());
    this.refs.reportClose.addEventListener("click", () => this.closeModal(this.refs.reportModal, this.refs.reportSubmit));
    this.refs.reportIndividual.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportSubmit.addEventListener("click", async () => this.submitGenerateReport());
    this.refs.removeClose.addEventListener("click", () => this.closeModal(this.refs.removeModal, this.refs.removeSubmit));
    this.refs.removeSubmit.addEventListener("click", async () => this.submitRemoveConfirmed());

    for (const modal of [this.refs.issueModal, this.refs.reportModal, this.refs.removeModal]) {
      modal.addEventListener("click", (event) => {
        if (event.target === modal) {
          const submitButton = modal === this.refs.issueModal
            ? this.refs.issueSubmit
            : modal === this.refs.reportModal
              ? this.refs.reportSubmit
              : this.refs.removeSubmit;
          this.closeModal(modal, submitButton);
        }
      });
    }
  }

  async fetchJSON(url, options) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || `${response.status} ${response.statusText}`);
    }
    return payload;
  }

  async requestJSON(url, options) {
    return this.fetchJSON(url, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...(options?.headers || {}),
      },
    });
  }

  isAbortError(error) {
    return error?.name === "AbortError";
  }

  beginRequest(name) {
    const existing = this.requestControllers[name];
    if (existing) {
      existing.abort();
    }
    const controller = new AbortController();
    this.requestControllers[name] = controller;
    return controller;
  }

  cancelRequest(name) {
    const existing = this.requestControllers[name];
    if (existing) {
      existing.abort();
      this.requestControllers[name] = null;
    }
  }

  cancelSelectionRequests(level = "family") {
    if (level === "family") {
      this.cancelRequest("studies");
      this.cancelRequest("study");
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("detail");
      return;
    }
    if (level === "study") {
      this.cancelRequest("study");
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("detail");
      return;
    }
    if (level === "dataset") {
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("detail");
      return;
    }
    if (level === "artifact") {
      this.cancelRequest("overview");
      this.cancelRequest("detail");
    }
  }

  handleVisibilityChange() {
    this.saveUiState();
    this.renderIndividuals();
    this.renderLayers();
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

  clearLoadedStudyState() {
    this.data = null;
    this.currentArtifactEntry = null;
    this.currentTimeMs = 0;
    this.lastReportLinks = [];
    this.refs.individuals.innerHTML = "";
    this.refs.selectedFixes.innerHTML = "";
    this.refs.individualHead.textContent = "Individuals and coverage";
    this.refs.fixHead.textContent = "Checked fixes";
    this.refs.slider.min = "0";
    this.refs.slider.max = "0";
    this.refs.slider.value = "0";
    this.updateTimeLabel();
    this.renderLayers();
    this.renderLegend();
    this.updateActionButtons();
  }

  async switchFamily(familyName) {
    if (!familyName) {
      return;
    }
    this.cancelSelectionRequests("family");
    this.currentFamily = familyName;
    this.currentStudy = "";
    this.currentDatasetId = "";
    this.currentArtifact = "";
    this.currentDataset = null;
    this.refs.family.value = familyName;
    this.resetSelect(this.refs.study, "Select a study");
    this.resetSelect(this.refs.dataset, "No versions");
    this.resetSelect(this.refs.artifact, "No artifacts");
    this.saveUiState();
    setFamilyPresetInUrl(familyName);
    await this.loadStudies();
  }

  resetSelect(select, placeholderText) {
    select.innerHTML = "";
    const option = document.createElement("option");
    option.value = "";
    option.textContent = placeholderText;
    select.appendChild(option);
    select.value = "";
    select.disabled = true;
  }

  async loadFamilies() {
    this.setStatus("Loading movement families...");
    try {
      const controller = this.beginRequest("families");
      const payload = await this.fetchJSON("/api/apps/movement/families", { signal: controller.signal });
      if (this.requestControllers.families !== controller) {
        return;
      }
      this.families = Array.isArray(payload.families) ? payload.families.filter(family => family.study_count > 0) : [];
      this.refs.family.innerHTML = "";
      for (const family of this.families) {
        const option = document.createElement("option");
        option.value = family.name;
        option.textContent = family.label || family.name;
        this.refs.family.appendChild(option);
      }
      if (!this.families.length) {
        this.showOverlay("No movement studies were found. Run the movement sample-data migration to create family/study roots.");
        this.setStatus("No movement studies were found.", true);
        return;
      }
      this.currentFamily = chooseStartupFamily(this.families, this.uiState.family);
      this.refs.family.value = this.currentFamily;
      this.refs.family.disabled = false;
      setFamilyPresetInUrl(this.currentFamily);
      await this.loadStudies();
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      console.error("Failed to load movement families", error);
      this.setStatus(error.message, true);
      this.showOverlay("The movement family index could not be loaded.");
    }
  }

  async loadStudies() {
    this.studyLoadId += 1;
    this.datasetLoadId += 1;
    this.loadRequestId += 1;
    const familyName = this.currentFamily;
    this.currentStudy = "";
    this.currentDataset = null;
    this.currentArtifactEntry = null;
    this.resetSelect(this.refs.study, "Loading studies...");
    this.resetSelect(this.refs.dataset, "No versions");
    this.resetSelect(this.refs.artifact, "No artifacts");
    this.setStatus(`Loading studies for ${this.currentFamily}...`);
    try {
      const controller = this.beginRequest("studies");
      const payload = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/studies`,
        { signal: controller.signal },
      );
      if (this.requestControllers.studies !== controller || familyName !== this.currentFamily) {
        return;
      }
      this.studies = Array.isArray(payload.studies) ? payload.studies : [];
      this.refs.study.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Select a study";
      this.refs.study.appendChild(placeholder);
      for (const study of this.studies) {
        const option = document.createElement("option");
        option.value = study.name;
        option.textContent = study.name;
        this.refs.study.appendChild(option);
      }
      if (!this.studies.length) {
        this.resetSelect(this.refs.study, "No studies");
        this.clearLoadedStudyState();
        this.setStatus(`No studies were found in ${familyName}.`, true);
        this.showOverlay(`No studies were found in ${familyName}.`);
        return;
      }
      this.refs.study.disabled = false;
      this.currentStudy = "";
      this.refs.study.value = "";
      this.clearLoadedStudyState();
      this.saveUiState();
      this.setStatus(`Loaded ${formatCount(this.studies.length)} studies in ${familyName}. Select a study to continue.`);
      this.showOverlay(`Select a study in ${familyName} to load its lineage and tracks.`);
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      console.error("Failed to load movement studies", error);
      this.setStatus(error.message, true);
      this.showOverlay(`Could not load studies for ${familyName}.`);
    }
  }

  async loadStudy() {
    this.cancelRequest("study");
    this.cancelRequest("dataset");
    this.cancelRequest("overview");
    this.cancelRequest("detail");
    this.datasetLoadId += 1;
    this.loadRequestId += 1;
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const studyLoadId = ++this.studyLoadId;
    this.currentDataset = null;
    this.currentArtifactEntry = null;
    this.resetSelect(this.refs.dataset, "Loading versions...");
    this.resetSelect(this.refs.artifact, "No artifacts");
    if (!studyName) {
      this.clearLoadedStudyState();
      this.saveUiState();
      this.setStatus("Select a study to load its lineage.", true);
      this.showOverlay(`Select a study in ${familyName} to load its lineage and tracks.`);
      return;
    }
    this.setStatus(`Loading ${studyName} in ${familyName}...`);
    try {
      const controller = this.beginRequest("study");
      this.setStatus(`Loading study data for ${studyName}...`);
      const payload = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/load`,
        { signal: controller.signal },
      );
      if (
        this.requestControllers.study !== controller
        || studyLoadId !== this.studyLoadId
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
      ) {
        return;
      }
      const state = payload.state || {};
      const graph = payload.graph || {};
      const dataset = payload.dataset || null;
      const datasetId = String(payload.dataset_id || state?.current_dataset?.dataset_id || "");
      const artifactName = String(payload.logical_name || "");
      const summary = payload.overview || null;
      this.graph = graph;
      this.allDatasets = [...(Array.isArray(graph.datasets) ? graph.datasets : [])]
        .sort((left, right) => String(right.created_at || "").localeCompare(String(left.created_at || "")));
      this.stepByOutputDatasetId = new Map(
        (Array.isArray(graph.steps) ? graph.steps : []).map(step => [step.output_dataset_id, step]),
      );
      const preferredDatasetId = datasetId || this.currentDatasetId || state?.current_dataset?.dataset_id || "";
      this.refreshDatasetOptions(preferredDatasetId);
      this.currentDataset = dataset;
      this.currentDatasetId = preferredDatasetId;
      this.currentArtifact = artifactName;
      if (this.currentDataset && Array.isArray(this.currentDataset.artifacts)) {
        this.refs.artifact.innerHTML = "";
        for (const artifact of this.currentDataset.artifacts) {
          const option = document.createElement("option");
          option.value = artifact.logical_name;
          option.textContent = artifact.logical_name;
          this.refs.artifact.appendChild(option);
        }
        this.refs.artifact.disabled = !this.currentDataset.artifacts.length;
        if (artifactName) {
          this.refs.artifact.value = artifactName;
        }
      }
      this.saveUiState();
      if (!this.currentDataset || !summary || !artifactName) {
        await this.loadDataset();
        return;
      }
      this.currentArtifactEntry = (this.currentDataset.artifacts || []).find(
        artifact => artifact.logical_name === artifactName,
      ) || null;
      this.clearLoadedStudyState();
      this.data = buildDatasetFromSummary(summary, this.uiState.colorBy);
      this.currentTimeMs = this.data.minTimeMs;
      this.refs.slider.min = String(this.data.minTimeMs);
      this.refs.slider.max = String(this.data.maxTimeMs);
      this.refs.slider.value = String(this.currentTimeMs);
      this.data.selectedIndividuals = new Set(this.data.individuals);
      this.data.selectedFixKeys = new Set();
      this.populateColorByOptions();
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderLegend();
      this.updateTimeLabel();
      this.hideOverlay();
      await this.rebuildMap(false);
      this.resetView();
      this.updateActionButtons();
      this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Select exactly one individual to load detailed fixes.`);
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      this.clearLoadedStudyState();
      console.error("Failed to load movement study", error);
      this.setStatus(error.message, true);
      this.showOverlay(`Could not load ${studyName}.`);
    }
  }

  refreshDatasetOptions(preferredDatasetId = this.currentDatasetId) {
    this.datasets = [...this.allDatasets];
    this.refs.dataset.innerHTML = "";
    for (const dataset of this.datasets) {
      const option = document.createElement("option");
      option.value = dataset.dataset_id;
      option.textContent = formatDatasetLabel(dataset, this.graph?.current_dataset_id || "");
      this.refs.dataset.appendChild(option);
    }
    if (!this.datasets.length) {
      this.currentDatasetId = "";
      this.refs.dataset.disabled = true;
      return;
    }
    const nextDatasetId = this.datasets.some(dataset => dataset.dataset_id === preferredDatasetId)
      ? preferredDatasetId
      : this.datasets[0].dataset_id;
    this.currentDatasetId = nextDatasetId;
    this.refs.dataset.value = nextDatasetId;
    this.refs.dataset.disabled = false;
  }

  async loadDataset() {
    this.cancelSelectionRequests("dataset");
    this.loadRequestId += 1;
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const datasetLoadId = ++this.datasetLoadId;
    const preservedFixKeys = this.data ? new Set(this.data.selectedFixKeys) : new Set();
    this.clearLoadedStudyState();
    this.currentDataset = null;
    this.currentArtifactEntry = null;
    this.resetSelect(this.refs.artifact, "No artifacts");
    if (!familyName || !studyName || !datasetId) {
      this.saveUiState();
      this.setStatus("Select a version to load its artifacts.");
      this.showOverlay("Select a version to load movement tracks.");
      return;
    }
    this.setStatus(`Loading dataset ${this.currentDatasetId}...`);
    try {
      const controller = this.beginRequest("dataset");
      this.currentDataset = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/dataset/${encodeURIComponent(datasetId)}`,
        { signal: controller.signal },
      );
      if (
        this.requestControllers.dataset !== controller
        || datasetLoadId !== this.datasetLoadId
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
      ) {
        return;
      }
      this.updateActionButtons();
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      this.setStatus(error.message, true);
      this.showOverlay(`Could not load dataset ${this.currentDatasetId}.`);
      return;
    }
    const artifacts = Array.isArray(this.currentDataset.artifacts) ? this.currentDataset.artifacts : [];
    this.refs.artifact.innerHTML = "";
    for (const artifact of artifacts) {
      const option = document.createElement("option");
      option.value = artifact.logical_name;
      option.textContent = artifact.logical_name;
      this.refs.artifact.appendChild(option);
    }
    if (!artifacts.length) {
      this.currentArtifact = "";
      this.updateActionButtons();
      this.showOverlay(`Dataset ${this.currentDatasetId} does not contain artifacts.`);
      this.setStatus("Selected dataset has no artifacts.", true);
      return;
    }
    this.currentArtifact = artifacts.some(artifact => artifact.logical_name === this.currentArtifact)
      ? this.currentArtifact
      : artifacts[0].logical_name;
    this.refs.artifact.value = this.currentArtifact;
    this.refs.artifact.disabled = false;
    this.saveUiState();
      await this.loadArtifact(preservedFixKeys);
    }

  async loadArtifact(preservedFixKeys = new Set()) {
    this.cancelSelectionRequests("artifact");
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    const requestId = ++this.loadRequestId;
    if (!this.currentDataset || this.currentDataset.dataset_id !== datasetId) {
      return;
    }
    if (!this.allDatasets.some(dataset => dataset.dataset_id === datasetId)) {
      return;
    }
    this.currentArtifactEntry = (this.currentDataset?.artifacts || []).find(
      artifact => artifact.logical_name === this.currentArtifact,
    ) || null;
    if (!this.currentArtifactEntry) {
      return;
    }
    this.saveUiState();
    this.setStatus(`Loading overview for ${this.currentArtifact} from ${this.currentDatasetId}...`);
    try {
      const controller = this.beginRequest("overview");
      const summary = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/dataset/${encodeURIComponent(datasetId)}/overview?${new URLSearchParams({ logical_name: artifactName }).toString()}`,
        { signal: controller.signal },
      );
      if (
        requestId !== this.loadRequestId
        || this.requestControllers.overview !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
      ) {
        return;
      }
      this.data = buildDatasetFromSummary(summary, this.uiState.colorBy);
      this.currentTimeMs = this.data.minTimeMs;
      this.refs.slider.min = String(this.data.minTimeMs);
      this.refs.slider.max = String(this.data.maxTimeMs);
      this.refs.slider.value = String(this.currentTimeMs);
      this.data.selectedIndividuals = new Set(this.data.individuals);
      this.data.selectedFixKeys = new Set(
        [...preservedFixKeys].filter(key => this.data.fixByKey.has(key)),
      );
      this.populateColorByOptions();
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderLegend();
      this.updateTimeLabel();
      this.hideOverlay();
      await this.rebuildMap(false);
      this.resetView();
      this.updateActionButtons();
      this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Select exactly one individual to load detailed fixes.`);
      void this.loadDetailForCurrentSelection({ preservedFixKeys });
    } catch (error) {
      if (this.isAbortError(error) || requestId !== this.loadRequestId) {
        return;
      }
      this.clearLoadedStudyState();
      this.setStatus(error.message, true);
      this.showOverlay(`Could not render ${this.currentArtifact}.`);
    }
  }

  populateColorByOptions() {
    this.refs.colorBy.innerHTML = "";
    if (!this.data) {
      return;
    }
    for (const field of this.data.colorFields) {
      const option = document.createElement("option");
      option.value = field.key;
      option.textContent = `${field.label} (${field.source})`;
      this.refs.colorBy.appendChild(option);
    }
    const preferred = this.data.colorFields.some(field => field.key === this.uiState.colorBy)
      ? this.uiState.colorBy
      : this.data.colorFields[0]?.key || "step_length_m";
    this.refs.colorBy.value = preferred;
    this.saveUiState();
  }

  renderIndividuals() {
    this.refs.individuals.innerHTML = "";
    if (!this.data) {
      return;
    }
    this.refs.individualHead.textContent = `Individuals and coverage (${formatCount(this.data.individuals.length)})`;
    for (const individual of this.data.individuals) {
      const stats = this.data.stats[individual];
      const coverage = this.data.coverageByIndividual[individual] || {};
      const isSelected = this.data.selectedIndividuals.has(individual);
      const card = document.createElement("div");
      card.className = "movement-card interactive";
      card.style.opacity = isSelected ? "1" : "0.34";

      const header = document.createElement("div");
      header.className = "movement-row";
      const left = document.createElement("div");
      left.className = "movement-row-left";
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = isSelected;
      checkbox.addEventListener("click", event => event.stopPropagation());
      checkbox.addEventListener("change", () => {
        this.toggleIndividual(individual, checkbox.checked);
      });
      const label = document.createElement("div");
      label.className = "movement-title";
      label.textContent = individual;
      left.append(checkbox, label);
      if (this.data.speciesByIndividual[individual]) {
        const species = document.createElement("span");
        species.className = "movement-subtle";
        species.textContent = `• ${this.data.speciesByIndividual[individual]}`;
        left.append(species);
      }
      const total = document.createElement("span");
      total.className = "movement-subtle";
      total.textContent = formatCount(stats.rowCount);
      header.append(left, total);
      card.appendChild(header);

      const statsRow = document.createElement("div");
      statsRow.className = "movement-stats";
      statsRow.append(
        statChip(`median step ${formatMaybeNumber(stats.medianStepM, "m")}`),
        statChip(`median speed ${formatMaybeNumber(stats.medianSpeedMps, "m/s")}`),
        statChip(`suspected ${formatCount(stats.suspectedCount)}`),
        statChip(`confirmed ${formatCount(stats.confirmedCount)}`),
      );
      card.appendChild(statsRow);

      const track = document.createElement("div");
      track.className = "movement-track";
      for (const setName of visibleSets(this.refs.showTrain.checked, this.refs.showTest.checked)) {
        const bar = coverage[setName];
        if (!bar) {
          continue;
        }
        const bounds = rangeToPercent(this.data.minTimeMs, this.data.maxTimeMs, bar.startMs, bar.endMs);
        const barEl = document.createElement("div");
        barEl.className = "movement-bar";
        barEl.style.left = `${bounds.left}%`;
        barEl.style.width = `${bounds.width}%`;
        barEl.style.background = colorCss(this.data.individualPalette[individual], setName, isSelected ? 0.86 : 0.26);
        track.appendChild(barEl);
      }
      card.appendChild(track);

      card.addEventListener("click", () => {
        this.toggleIndividual(individual, !isSelected);
      });
      this.refs.individuals.appendChild(card);
    }
  }

  toggleIndividual(individual, shouldSelect) {
    if (!this.data) {
      return;
    }
    if (shouldSelect) {
      this.data.selectedIndividuals.add(individual);
    } else {
      this.data.selectedIndividuals.delete(individual);
    }
    this.saveUiState();
    this.renderIndividuals();
    this.renderSelectedFixes();
    this.renderLayers();
    this.updateActionButtons();
    void this.loadDetailForCurrentSelection();
  }

  renderSelectedFixes() {
    this.refs.selectedFixes.innerHTML = "";
    if (!this.data) {
      this.refs.fixHead.textContent = "Checked fixes";
      return;
    }
    const selectedFixes = this.getSelectedFixes();
    this.refs.fixHead.textContent = `Checked fixes (${formatCount(selectedFixes.length)})`;
    if (!selectedFixes.length) {
      const empty = document.createElement("div");
      empty.className = "movement-empty";
      if (this.data.detailState === "loading") {
        empty.textContent = `Loading detailed fixes for ${this.data.detailIndividual}...`;
      } else if (!this.hasLoadedDetailSelection()) {
        empty.textContent = "Select exactly one individual to load detailed fixes, then click map points to add fixes to the checked review list.";
      } else if (this.data.detailState === "error") {
        empty.textContent = "Detailed fixes could not be loaded for the current selection. Try selecting the individual again.";
      } else {
        empty.textContent = "Click map points to add fixes to the checked review list.";
      }
      this.refs.selectedFixes.appendChild(empty);
      return;
    }
    const fixesToShow = selectedFixes.slice(0, MAX_SELECTED_FIXES_SHOWN);
    for (const fix of fixesToShow) {
      const card = document.createElement("div");
      card.className = "movement-card";

      const header = document.createElement("div");
      header.className = "movement-row";
      const left = document.createElement("div");
      left.className = "movement-row-left";
      const title = document.createElement("div");
      title.className = "movement-title";
      title.textContent = `${fix.individual} • ${formatTimestamp(fix.timeMs)}`;
      left.appendChild(title);
      const pill = statusPill(fix.review.status || "unreviewed");
      left.appendChild(pill);
      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.className = "movement-fix-remove";
      removeButton.textContent = "Remove";
      removeButton.addEventListener("click", () => {
        this.toggleFixSelection(fix.fixKey);
      });
      header.append(left, removeButton);
      card.appendChild(header);

      const meta = document.createElement("div");
      meta.className = "movement-fix-meta";
      const colorField = this.data.colorFieldByKey.get(this.refs.colorBy.value);
      const colorValue = formatColorValue(fix.attributes[this.refs.colorBy.value], colorField?.kind || "numeric");
      meta.textContent = [
        `fix ${fix.fixKey}`,
        `color ${this.refs.colorBy.value}: ${colorValue}`,
        `step ${formatMaybeNumber(fix.attributes.step_length_m, "m")}`,
        `speed ${formatMaybeNumber(fix.attributes.speed_mps, "m/s")}`,
      ].join(" • ");
      card.appendChild(meta);

      if (fix.review.issueType || fix.review.issueNote) {
        const note = document.createElement("div");
        note.className = "movement-fix-note";
        note.textContent = `${fix.review.issueType || "Issue"}: ${fix.review.issueNote || fix.review.ownerQuestion || ""}`;
        card.appendChild(note);
      }

      this.refs.selectedFixes.appendChild(card);
    }
    if (selectedFixes.length > fixesToShow.length) {
      const remainder = document.createElement("div");
      remainder.className = "movement-empty";
      remainder.textContent = `Showing the first ${formatCount(fixesToShow.length)} checked fixes.`;
      this.refs.selectedFixes.appendChild(remainder);
    }
  }

  renderLegend() {
    const legendEl = this.refs.legend;
    if (!legendEl) {
      return;
    }
    if (!this.data) {
      legendEl.innerHTML = "";
      legendEl.classList.add("hidden");
      return;
    }

    const field = this.data.colorFieldByKey.get(this.refs.colorBy.value) || this.data.colorFields[0];
    const style = field ? this.data.colorStyles.get(field.key) : null;
    if (!field || !style) {
      legendEl.innerHTML = "";
      legendEl.classList.add("hidden");
      return;
    }

    const header = `
      <div class="movement-legend-head">
        <div class="movement-legend-title">${escapeHtml(field.label)}</div>
        <div class="movement-legend-subtitle">${escapeHtml(field.source)} | ${escapeHtml(field.kind)}</div>
      </div>
    `;

    let body = "";
    if (style.kind === "numeric") {
      const lowerLabel = style.range.observedMin < style.range.min
        ? `<= ${formatColorValue(style.range.min, "numeric")}`
        : formatColorValue(style.range.min, "numeric");
      const upperLabel = style.range.observedMax > style.range.max
        ? `>= ${formatColorValue(style.range.max, "numeric")}`
        : formatColorValue(style.range.max, "numeric");
      const observedSummary = style.range.observedMin === style.range.min && style.range.observedMax === style.range.max
        ? "Scale uses the full observed numeric range."
        : `Scale is clipped to the ${formatPercent(NUMERIC_COLOR_MIN_QUANTILE)}-${formatPercent(NUMERIC_COLOR_MAX_QUANTILE)} percentile range; observed range ${formatColorValue(style.range.observedMin, "numeric")} to ${formatColorValue(style.range.observedMax, "numeric")}.`;
      body = `
        <div class="movement-legend-scale">
          <div
            class="movement-legend-gradient"
            style="background: linear-gradient(90deg, ${numericLegendGradient()});"
          ></div>
          <div class="movement-legend-range">
            <span>${escapeHtml(lowerLabel)}</span>
            <span>${escapeHtml(upperLabel)}</span>
          </div>
          <div class="movement-legend-note">${escapeHtml(observedSummary)}</div>
        </div>
      `;
    } else if (style.kind === "boolean") {
      body = `
        <div class="movement-legend-items">
          ${legendItem("False", [96, 201, 170, POINT_ALPHA])}
          ${legendItem("True", [246, 92, 110, POINT_ALPHA])}
          ${legendItem("Missing", [120, 136, 153, 120])}
        </div>
      `;
    } else {
      const items = Array.from(style.categories.entries())
        .sort((left, right) => left[0].localeCompare(right[0], undefined, { sensitivity: "base" }))
        .map(([label, color]) => legendItem(label, color))
        .join("");
      body = `<div class="movement-legend-items">${items}</div>`;
    }

    legendEl.innerHTML = `${header}${body}`;
    legendEl.classList.remove("hidden");
  }

  async rebuildMap(forceStyleReload) {
    if (!this.assetsLoaded || !window.maplibregl || !window.deck) {
      return;
    }
    const style = BASEMAP_STYLES[this.refs.basemap.value] || BASEMAP_STYLES.Blank;
    if (!this.map) {
      this.map = new maplibregl.Map({
        container: this.refs.map,
        style,
        center: this.data
          ? [this.data.initialView.longitude, this.data.initialView.latitude]
          : [0, 20],
        zoom: this.data ? this.data.initialView.zoom : 1.3,
        attributionControl: false,
      });
      this.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
      this.map.on("load", () => {
        this.mapLoaded = true;
        this.renderLayers();
      });
      this.map.on("error", (event) => {
        const message = event?.error?.message || "Map request failed";
        if (message === this.mapErrorMessage) {
          return;
        }
        this.mapErrorMessage = message;
        if (this.refs.basemap.value !== "Blank") {
          this.refs.basemap.value = "Blank";
          this.saveUiState();
          this.setStatus(`Map warning: ${message}. Falling back to Blank basemap.`, true);
          void this.rebuildMap(true);
          return;
        }
        this.setStatus(`Map warning: ${message}`, true);
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
    if (!this.data || !this.overlay || !this.mapLoaded) {
      if (this.overlay) {
        try {
          this.overlay.setProps({ layers: [] });
        } catch {}
      }
      return;
    }

    const visibleIndividuals = new Set(this.data.selectedIndividuals);
    const visibleSetNames = new Set(visibleSets(this.refs.showTrain.checked, this.refs.showTest.checked));
    const pathData = [];
    const pointData = [];
    const selectedPointData = [];
    const cursorData = [];

    for (const individual of this.data.individuals) {
      if (!visibleIndividuals.has(individual)) {
        continue;
      }
      for (const setName of visibleSetNames) {
        const series = this.data.seriesByIndividual[individual]?.[setName];
        if (!series) {
          continue;
        }
        if (series.positions.length >= 2) {
          pathData.push({
            individual,
            setName,
            path: series.positions,
            color: splitColor(this.data.individualPalette[individual], setName, PATH_ALPHA),
          });
        }
        const cursorPosition = interpolateSeriesPosition(series, this.currentTimeMs);
        if (cursorPosition) {
          cursorData.push({ position: cursorPosition });
        }
      }
    }

    for (const fix of this.data.fixes) {
      if (!visibleIndividuals.has(fix.individual) || !visibleSetNames.has(fix.setName)) {
        continue;
      }
      const point = {
        fixKey: fix.fixKey,
        individual: fix.individual,
        setName: fix.setName,
        position: fix.position,
        color: this.colorForFix(fix),
      };
      if (this.data.selectedFixKeys.has(fix.fixKey)) {
        selectedPointData.push({ ...point, status: fix.review.status || "unreviewed" });
      } else {
        pointData.push(point);
      }
    }

    const layers = [
      new deck.PathLayer({
        id: "movement-paths",
        data: pathData,
        getPath: item => item.path,
        getColor: item => item.color,
        getWidth: 2.5,
        widthMinPixels: 2,
        pickable: false,
      }),
    ];

    if (this.refs.showPoints.checked) {
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-points",
          data: pointData,
          getPosition: item => item.position,
          getFillColor: item => item.color,
          getRadius: 80,
          radiusMinPixels: 4,
          radiusMaxPixels: 10,
          pickable: true,
          onClick: info => {
            if (info.object) {
              this.toggleFixSelection(info.object.fixKey);
            }
          },
        }),
      );
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-selected-points",
          data: selectedPointData,
          getPosition: item => item.position,
          getFillColor: item => item.color,
          getLineColor: [255, 255, 255, 255],
          stroked: true,
          lineWidthMinPixels: 1.5,
          getRadius: 130,
          radiusMinPixels: 7,
          radiusMaxPixels: 14,
          pickable: true,
          onClick: info => {
            if (info.object) {
              this.toggleFixSelection(info.object.fixKey);
            }
          },
        }),
      );
    }

    if (cursorData.length) {
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-cursor",
          data: cursorData,
          getPosition: item => item.position,
          getFillColor: [10, 10, 10, 255],
          getLineColor: [255, 255, 255, 235],
          stroked: true,
          filled: true,
          getRadius: 68,
          radiusMinPixels: 4,
          radiusMaxPixels: 8,
          lineWidthMinPixels: 1.5,
          pickable: false,
        }),
      );
    }
    try {
      this.overlay.setProps({ layers });
    } catch (error) {
      this.setStatus(`Map warning: ${error.message}`, true);
    }
  }

  colorForFix(fix) {
    const field = this.data.colorFieldByKey.get(this.refs.colorBy.value) || this.data.colorFields[0];
    if (!field) {
      return [124, 210, 255, POINT_ALPHA];
    }
    const style = this.data.colorStyles.get(field.key);
    const value = fix.attributes[field.key];
    if (!style) {
      return [124, 210, 255, POINT_ALPHA];
    }
    if (style.kind === "numeric") {
      return interpolateNumericColor(value, style.range, POINT_ALPHA);
    }
    if (style.kind === "boolean") {
      if (value === true) return [246, 92, 110, POINT_ALPHA];
      if (value === false) return [96, 201, 170, POINT_ALPHA];
      return [120, 136, 153, 120];
    }
    return style.categories.get(String(value ?? "Missing")) || [120, 136, 153, 150];
  }

  toggleFixSelection(fixKey) {
    if (!this.data || !this.data.fixByKey.has(fixKey)) {
      return;
    }
    if (this.data.selectedFixKeys.has(fixKey)) {
      this.data.selectedFixKeys.delete(fixKey);
    } else {
      this.data.selectedFixKeys.add(fixKey);
    }
    this.renderSelectedFixes();
    this.renderLayers();
    this.updateActionButtons();
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

  getSelectedFixes() {
    if (!this.data) {
      return [];
    }
    return Array.from(this.data.selectedFixKeys)
      .map(key => this.data.fixByKey.get(key))
      .filter(Boolean)
      .sort((left, right) => left.timeMs - right.timeMs || left.individual.localeCompare(right.individual));
  }

  getSuspiciousFixes(individual = "") {
    if (!this.data) {
      return [];
    }
    return this.data.fixes
      .filter(fix => fix.review.status === "suspected" && (!individual || fix.individual === individual))
      .sort((left, right) => left.individual.localeCompare(right.individual) || left.timeMs - right.timeMs);
  }

  hasLoadedDetailSelection() {
    if (!this.data || this.data.detailState !== "loaded" || !this.data.detailIndividual) {
      return false;
    }
    return this.data.selectedIndividuals.size === 1 && this.data.selectedIndividuals.has(this.data.detailIndividual);
  }

  async loadDetailForCurrentSelection({ preservedFixKeys } = {}) {
    if (!this.data || !this.currentArtifact) {
      return;
    }
    const selectedIndividuals = [...this.data.selectedIndividuals].sort((left, right) => left.localeCompare(right));
    const preserved = preservedFixKeys instanceof Set ? preservedFixKeys : new Set(this.data.selectedFixKeys);
    if (selectedIndividuals.length !== 1) {
      this.cancelRequest("detail");
      this.data.detailState = "idle";
      this.data.detailIndividual = "";
      this.data.detailFixes = [];
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set();
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
      return;
    }

    const individual = selectedIndividuals[0];
    if (this.data.detailState === "loaded" && this.data.detailIndividual === individual) {
      this.updateActionButtons();
      return;
    }

    this.cancelRequest("detail");
    this.data.detailState = "loading";
    this.data.detailIndividual = individual;
    this.data.selectedFixKeys = new Set([...preserved].filter(key => this.data.fixByKey.has(key)));
    this.renderSelectedFixes();
    this.updateActionButtons();
    this.setStatus(`Loaded overview for ${this.currentArtifact}. Loading detailed fixes for ${individual}...`);

    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    const requestId = ++this.loadRequestId;
    try {
      const controller = this.beginRequest("detail");
      const payload = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/dataset/${encodeURIComponent(datasetId)}/fixes?${new URLSearchParams({ logical_name: artifactName, individual }).toString()}`,
        { signal: controller.signal },
      );
      if (
        requestId !== this.loadRequestId
        || this.requestControllers.detail !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
        || !this.data
      ) {
        return;
      }
      this.data.detailState = "loaded";
      this.data.detailIndividual = individual;
      this.data.detailFixes = parseMovementFixes(payload.fixes || []);
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set([...preserved].filter(key => this.data.fixByKey.has(key)));
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
      this.setStatus(`Loaded detailed fixes for ${individual} (${formatCount(payload.returned_fix_count || this.data.detailFixes.length)} fixes).`);
    } catch (error) {
      if (this.isAbortError(error) || requestId !== this.loadRequestId || !this.data) {
        return;
      }
      this.data.detailState = "error";
      this.data.detailFixes = [];
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set();
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
      this.setStatus(`Overview loaded, but detailed fixes for ${individual} failed: ${error.message}`, true);
    }
  }

  populateReportIndividualOptions() {
    const suspiciousFixes = this.getSuspiciousFixes();
    const individuals = uniqueStrings(suspiciousFixes.map(fix => fix.individual))
      .sort((left, right) => left.localeCompare(right));
    const currentValue = this.refs.reportIndividual.value;
    this.refs.reportIndividual.innerHTML = "";

    const allOption = document.createElement("option");
    allOption.value = "";
    allOption.textContent = `All individuals (${formatCount(suspiciousFixes.length)} fixes)`;
    this.refs.reportIndividual.appendChild(allOption);

    for (const individual of individuals) {
      const option = document.createElement("option");
      option.value = individual;
      option.textContent = `${individual} (${formatCount(this.getSuspiciousFixes(individual).length)} fixes)`;
      this.refs.reportIndividual.appendChild(option);
    }

    this.refs.reportIndividual.value = individuals.includes(currentValue) ? currentValue : "";
  }

  getReportFixes() {
    return this.getSuspiciousFixes(this.refs.reportIndividual.value);
  }

  getReportSnapshotWindows() {
    return this.buildReportSnapshotWindows(this.getReportFixes());
  }

  serializeFixForReport(fix) {
    return {
      fix_key: fix.fixKey,
      individual: fix.individual,
      set_name: fix.setName,
      time_ms: fix.timeMs,
      time_text: formatTimestamp(fix.timeMs),
      lon: fix.position[0],
      lat: fix.position[1],
      step_length_m: fix.attributes.step_length_m ?? null,
      speed_mps: fix.attributes.speed_mps ?? null,
      time_delta_s: fix.attributes.time_delta_s ?? null,
      attributes: { ...(fix.attributes || {}) },
      review: {
        status: fix.review.status || "",
        issue_id: fix.review.issueId || "",
        issue_type: fix.review.issueType || "",
        issue_note: fix.review.issueNote || "",
        owner_question: fix.review.ownerQuestion || "",
        review_user: fix.review.reviewUser || "",
        reviewed_at: fix.review.reviewedAt || "",
      },
    };
  }

  serializeSnapshotWindowForReport(window) {
    return {
      snapshot_key: window.snapshotKey,
      caption: window.caption,
      individual: window.individual,
      set_name: window.setName,
      issue_type: window.issueType,
      issue_types: [...window.issueTypes],
      anchor_fix_keys: [...window.anchorFixKeys],
      report_fix_keys: [...window.reportFixKeys],
      start_fix_key: window.startFixKey,
      end_fix_key: window.endFixKey,
      start_time_ms: window.startTimeMs,
      end_time_ms: window.endTimeMs,
      start_time_text: window.startTimeText,
      end_time_text: window.endTimeText,
      window_fix_count: window.windowFixCount,
    };
  }

  buildReportSnapshotWindows(reportFixes) {
    if (!this.data || !reportFixes.length) {
      return [];
    }
    const trackFixesByKey = new Map();
    const indexByFixKey = new Map();
    for (const fix of this.data.fixes) {
      const trackKey = reportTrackKey(fix.individual, fix.setName);
      const group = trackFixesByKey.get(trackKey) || [];
      group.push(fix);
      trackFixesByKey.set(trackKey, group);
    }
    for (const [trackKey, group] of trackFixesByKey.entries()) {
      group.sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
      group.forEach((fix, index) => {
        indexByFixKey.set(fix.fixKey, { trackKey, index });
      });
    }

    const reportFixMap = new Map(reportFixes.map(fix => [fix.fixKey, fix]));
    const candidates = [];
    for (const fix of reportFixes) {
      const located = indexByFixKey.get(fix.fixKey);
      if (!located) {
        continue;
      }
      const trackFixes = trackFixesByKey.get(located.trackKey) || [];
      candidates.push({
        trackKey: located.trackKey,
        individual: fix.individual,
        setName: fix.setName,
        startIndex: Math.max(0, located.index - 25),
        endIndex: Math.min(trackFixes.length - 1, located.index + 25),
        anchorFixes: [fix],
      });
    }
    candidates.sort((left, right) => (
      left.trackKey.localeCompare(right.trackKey)
      || left.startIndex - right.startIndex
      || left.endIndex - right.endIndex
    ));

    const merged = [];
    for (const candidate of candidates) {
      const previous = merged[merged.length - 1];
      if (
        previous
        && previous.trackKey === candidate.trackKey
        && candidate.startIndex <= previous.endIndex
      ) {
        previous.endIndex = Math.max(previous.endIndex, candidate.endIndex);
        previous.anchorFixes.push(...candidate.anchorFixes);
        continue;
      }
      merged.push({ ...candidate });
    }

    return merged.map((window, index) => {
      const trackFixes = trackFixesByKey.get(window.trackKey) || [];
      const windowFixes = trackFixes.slice(window.startIndex, window.endIndex + 1);
      const reportFixKeys = windowFixes
        .filter(fix => reportFixMap.has(fix.fixKey))
        .map(fix => fix.fixKey);
      const reportWindowFixes = reportFixKeys
        .map(fixKey => reportFixMap.get(fixKey))
        .filter(Boolean)
        .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
      const anchorFixKeys = uniqueNonEmpty(window.anchorFixes.map(fix => fix.fixKey)).sort((left, right) => {
        const leftFix = reportFixMap.get(left);
        const rightFix = reportFixMap.get(right);
        return (leftFix?.timeMs || 0) - (rightFix?.timeMs || 0) || left.localeCompare(right);
      });
      const issueTypes = uniqueNonEmpty(reportWindowFixes.map(fix => reportIssueType(fix))).sort((left, right) => left.localeCompare(right));
      const firstWindowFix = windowFixes[0];
      const lastWindowFix = windowFixes[windowFixes.length - 1];
      return {
        snapshotKey: `snapshot_${String(index + 1).padStart(2, "0")}`,
        caption: `${issueTypes.join(", ")} | ${window.individual} | ${formatTimestamp(firstWindowFix?.timeMs)} to ${formatTimestamp(lastWindowFix?.timeMs)}`,
        individual: window.individual,
        setName: window.setName,
        issueType: issueTypes[0] || "Unspecified issue",
        issueTypes,
        anchorFixKeys,
        reportFixKeys,
        startFixKey: firstWindowFix?.fixKey || "",
        endFixKey: lastWindowFix?.fixKey || "",
        startTimeMs: firstWindowFix?.timeMs || 0,
        endTimeMs: lastWindowFix?.timeMs || 0,
        startTimeText: formatTimestamp(firstWindowFix?.timeMs || 0),
        endTimeText: formatTimestamp(lastWindowFix?.timeMs || 0),
        windowFixCount: windowFixes.length,
        windowFixes,
        reportWindowFixes,
      };
    });
  }

  renderReportSelection() {
    const reportFixes = this.getReportFixes();
    const snapshotWindows = this.getReportSnapshotWindows();
    const issueTypes = uniqueNonEmpty(reportFixes.map(fix => reportIssueType(fix)));
    const individualLabel = this.refs.reportIndividual.value || "All individuals";
    this.refs.reportMeta.innerHTML = `
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Individual:</strong> ${escapeHtml(individualLabel)}</div>
      <div><strong>Suspected fixes in report:</strong> ${escapeHtml(formatCount(reportFixes.length))}</div>
      <div><strong>Snapshot windows:</strong> ${escapeHtml(formatCount(snapshotWindows.length))}</div>
      <div><strong>Issue types in report:</strong> ${escapeHtml(issueTypes.length ? issueTypes.join(", ") : "Unspecified issue")}</div>
    `;
    this.refs.reportSelection.textContent = snapshotWindows.length
      ? snapshotWindows
        .slice(0, 25)
        .map(window => `${window.individual} • ${window.startTimeText} to ${window.endTimeText} • ${window.windowFixCount} fixes • ${window.issueTypes.join(", ")}`)
        .join("\n")
      : "No suspected fixes match this selection.";
    this.refs.reportSubmit.disabled = reportFixes.length === 0;
  }

  updateActionButtons() {
    const hasData = Boolean(this.data);
    const hasDetail = this.hasLoadedDetailSelection();
    const selectedCount = this.getSelectedFixes().length;
    const suspiciousCount = this.getSuspiciousFixes().length;
    for (const button of [
      this.refs.selectSuspicious,
      this.refs.clearFixes,
      this.refs.markSuspected,
      this.refs.markConfirmed,
      this.refs.generateReport,
      this.refs.removeConfirmed,
    ]) {
      button.hidden = !hasDetail;
    }
    this.refs.markSuspected.disabled = !hasData || !hasDetail || selectedCount === 0;
    this.refs.markConfirmed.disabled = !hasData || !hasDetail || selectedCount === 0;
    this.refs.generateReport.disabled = !hasData || !hasDetail || suspiciousCount === 0;
    this.refs.removeConfirmed.disabled = !hasData || !hasDetail || selectedCount === 0;
    this.refs.selectSuspicious.disabled = !hasData || !hasDetail || suspiciousCount === 0;
    this.updateUndoButton();
  }

  updateUndoButton() {
    const currentHeadDatasetId = this.graph?.current_dataset_id || "";
    const selectedIsCurrentHead = Boolean(currentHeadDatasetId) && this.currentDatasetId === currentHeadDatasetId;
    const canUndo = selectedIsCurrentHead && Boolean(this.currentDataset?.parent_dataset_id);
    this.refs.undo.disabled = !canUndo;
  }

  openIssueModal(status) {
    const selectedFixes = this.getSelectedFixes();
    if (!selectedFixes.length || !this.currentArtifact) {
      return;
    }
    this.pendingIssueStatus = status;
    this.refs.issueTitle.textContent = `Mark fixes as ${status}`;
    this.refs.issueMeta.innerHTML = `
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Checked fixes:</strong> ${escapeHtml(formatCount(selectedFixes.length))}</div>
    `;
    this.refs.issueSelection.textContent = selectedFixes
      .slice(0, 25)
      .map(fix => `${fix.individual} • ${formatTimestamp(fix.timeMs)} • ${fix.fixKey}`)
      .join("\n");
    this.refs.issueUser.value = this.getUser();
    this.refs.issueType.value = "";
    this.refs.issueNote.value = "";
    this.refs.issueQuestion.value = "Could you confirm whether these fixes should be treated as outliers and explain the likely error source?";
    this.refs.issueStatus.textContent = "";
    this.refs.issueStatus.classList.remove("error");
    this.refs.issueSubmit.disabled = false;
    this.refs.issueClose.disabled = false;
    this.refs.issueModal.classList.remove("hidden");
    this.refs.issueType.focus();
  }

  openReportModal() {
    if (!this.currentArtifact) {
      return;
    }
    const suspiciousFixes = this.getSuspiciousFixes();
    if (!suspiciousFixes.length) {
      return;
    }
    this.refs.reportUser.value = this.getUser();
    this.populateReportIndividualOptions();
    this.refs.reportScreenshotMode.value = "manual";
    this.refs.reportLinks.innerHTML = this.lastReportLinks.map(link => (
      `<a href="${link.href}" target="_blank" rel="noreferrer">${escapeHtml(link.label)}</a>`
    )).join("");
    this.refs.reportStatus.textContent = "";
    this.refs.reportStatus.classList.remove("error");
    this.refs.reportSubmit.disabled = false;
    this.refs.reportClose.disabled = false;
    this.renderReportSelection();
    this.refs.reportModal.classList.remove("hidden");
  }

  openRemoveModal() {
    const selectedFixes = this.getSelectedFixes();
    if (!selectedFixes.length || !this.currentArtifact) {
      return;
    }
    this.refs.removeMeta.innerHTML = `
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Checked fixes:</strong> ${escapeHtml(formatCount(selectedFixes.length))}</div>
    `;
    this.refs.removeSelection.textContent = selectedFixes
      .slice(0, 25)
      .map(fix => `${fix.individual} • ${formatTimestamp(fix.timeMs)} • ${fix.review.status || "unreviewed"}`)
      .join("\n");
    this.refs.removeUser.value = this.getUser();
    this.refs.removeReason.value = "";
    this.refs.removeStatus.textContent = "";
    this.refs.removeStatus.classList.remove("error");
    this.refs.removeSubmit.disabled = false;
    this.refs.removeClose.disabled = false;
    this.refs.removeModal.classList.remove("hidden");
    this.refs.removeReason.focus();
  }

  closeModal(modal, submitButton) {
    if (submitButton.disabled) {
      return;
    }
    modal.classList.add("hidden");
  }

  async submitIssueAction() {
    const selectedFixes = this.getSelectedFixes();
    const user = this.refs.issueUser.value.trim();
    const issueType = this.refs.issueType.value.trim();
    const issueNote = this.refs.issueNote.value.trim();
    const ownerQuestion = this.refs.issueQuestion.value.trim();
    if (!selectedFixes.length) {
      return;
    }
    if (!user || !issueType || !issueNote || !ownerQuestion) {
      this.refs.issueStatus.textContent = "User, issue type, description, and owner question are required.";
      this.refs.issueStatus.classList.add("error");
      return;
    }

    this.refs.issueSubmit.disabled = true;
    this.refs.issueClose.disabled = true;
    this.refs.issueStatus.textContent = `Marking ${formatCount(selectedFixes.length)} fixes as ${this.pendingIssueStatus}...`;
    this.refs.issueStatus.classList.remove("error");
    this.setStatus(this.refs.issueStatus.textContent);

    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/annotate-fixes`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            fix_keys: selectedFixes.map(fix => fix.fixKey),
            status: this.pendingIssueStatus,
            issue_type: issueType,
            issue_note: issueNote,
            owner_question: ownerQuestion,
            user,
          }),
        },
      );
      this.setUser(user);
      this.refs.issueModal.classList.add("hidden");
      await this.loadStudyAtDataset(result.dataset.dataset_id);
      this.setStatus(`Created ${result.step.title} in ${result.dataset.dataset_id}.`);
    } catch (error) {
      this.refs.issueStatus.textContent = error.message;
      this.refs.issueStatus.classList.add("error");
      this.refs.issueSubmit.disabled = false;
      this.refs.issueClose.disabled = false;
      this.setStatus(error.message, true);
    }
  }

  async submitGenerateReport() {
    const selectedFixes = this.getReportFixes();
    const snapshotWindows = this.getReportSnapshotWindows();
    const user = this.refs.reportUser.value.trim();
    if (!selectedFixes.length) {
      return;
    }
    if (!user) {
      this.refs.reportStatus.textContent = "User is required.";
      this.refs.reportStatus.classList.add("error");
      return;
    }

    this.refs.reportSubmit.disabled = true;
    this.refs.reportClose.disabled = true;
    this.refs.reportStatus.textContent = `Generating a report for ${formatCount(selectedFixes.length)} suspected fixes...`;
    this.refs.reportStatus.classList.remove("error");
    this.setStatus(this.refs.reportStatus.textContent);

    try {
      const screenshotMode = this.refs.reportScreenshotMode.value;
      const snapshots = screenshotMode === "auto" ? await this.captureSnapshotsForSelection(snapshotWindows) : [];
      const issueIds = uniqueNonEmpty(selectedFixes.map(fix => fix.review.issueId));
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/generate-report`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            fix_keys: selectedFixes.map(fix => fix.fixKey),
            issue_ids: issueIds,
            report_fixes: selectedFixes.map(fix => this.serializeFixForReport(fix)),
            snapshot_windows: snapshotWindows.map(window => this.serializeSnapshotWindowForReport(window)),
            screenshot_mode: screenshotMode,
            snapshots,
            user,
          }),
        },
      );
      this.setUser(user);
      const analysisId = result.analysis.analysis_id;
      this.lastReportLinks = [
        {
          label: "Markdown Report",
          href: `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis/${encodeURIComponent(analysisId)}/artifact/movement_outlier_report.md`,
        },
        {
          label: "HTML Report",
          href: `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis/${encodeURIComponent(analysisId)}/artifact/movement_outlier_report.html`,
        },
        {
          label: "Appendix CSV",
          href: `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis/${encodeURIComponent(analysisId)}/artifact/movement_outlier_fixes.csv`,
        },
      ];
      this.refs.reportLinks.innerHTML = this.lastReportLinks.map(link => (
        `<a href="${link.href}" target="_blank" rel="noreferrer">${escapeHtml(link.label)}</a>`
      )).join("");
      this.refs.reportStatus.textContent = `Created analysis ${analysisId}.`;
      this.refs.reportSubmit.disabled = false;
      this.refs.reportClose.disabled = false;
      this.setStatus(`Created report analysis ${analysisId}.`);
    } catch (error) {
      this.refs.reportStatus.textContent = error.message;
      this.refs.reportStatus.classList.add("error");
      this.refs.reportSubmit.disabled = false;
      this.refs.reportClose.disabled = false;
      this.setStatus(error.message, true);
    }
  }

  async submitRemoveConfirmed() {
    const selectedFixes = this.getSelectedFixes();
    const user = this.refs.removeUser.value.trim();
    const reason = this.refs.removeReason.value.trim();
    if (!selectedFixes.length) {
      return;
    }
    if (!user || !reason) {
      this.refs.removeStatus.textContent = "User and reason are required.";
      this.refs.removeStatus.classList.add("error");
      return;
    }

    this.refs.removeSubmit.disabled = true;
    this.refs.removeClose.disabled = true;
    this.refs.removeStatus.textContent = `Removing ${formatCount(selectedFixes.length)} checked fixes...`;
    this.refs.removeStatus.classList.remove("error");
    this.setStatus(this.refs.removeStatus.textContent);

    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/remove-confirmed-fixes`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            fix_keys: selectedFixes.map(fix => fix.fixKey),
            reason,
            user,
          }),
        },
      );
      this.setUser(user);
      this.refs.removeModal.classList.add("hidden");
      await this.loadStudyAtDataset(result.dataset.dataset_id);
      this.setStatus(`Created ${result.step.title} in ${result.dataset.dataset_id}.`);
    } catch (error) {
      this.refs.removeStatus.textContent = error.message;
      this.refs.removeStatus.classList.add("error");
      this.refs.removeSubmit.disabled = false;
      this.refs.removeClose.disabled = false;
      this.setStatus(error.message, true);
    }
  }

  async captureSnapshotsForSelection(snapshotWindows) {
    if (!snapshotWindows.length) {
      return [];
    }
    const renderer = await createReportSnapshotRenderer({
      style: BASEMAP_STYLES[this.refs.basemap.value] || BASEMAP_STYLES.Positron,
    });
    try {
      const snapshots = [];
      for (let index = 0; index < snapshotWindows.length; index += 1) {
        const window = snapshotWindows[index];
        this.refs.reportStatus.textContent = `Rendering snapshot ${index + 1} of ${snapshotWindows.length}...`;
        this.setStatus(this.refs.reportStatus.textContent);
        const dataUrl = await renderer.capture(window);
        if (!dataUrl) {
          continue;
        }
        snapshots.push({
          snapshot_key: window.snapshotKey,
          caption: window.caption,
          data_url: dataUrl,
        });
      }
      return snapshots;
    } finally {
      renderer.destroy();
    }
  }

  async loadStudyAtDataset(datasetId) {
    this.currentDatasetId = datasetId;
    await this.loadStudy();
  }

  async undoCurrentHead() {
    if (this.refs.undo.disabled) {
      return;
    }
    this.refs.undo.disabled = true;
    this.setStatus(`Undoing ${this.currentDatasetId}...`);
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/undo`,
        { method: "POST" },
      );
      await this.loadStudyAtDataset(result.dataset.dataset_id);
      this.setStatus(`Undid to ${result.dataset.dataset_id}.`);
    } catch (error) {
      this.setStatus(error.message, true);
      this.updateUndoButton();
    }
  }
}

function buildDatasetFromSummary(summary, preferredColorBy) {
  const individuals = Array.isArray(summary.individuals)
    ? [...summary.individuals].sort((left, right) => left.localeCompare(right))
    : [];
  if (!individuals.length) {
    throw new Error("Dataset summary did not contain any individuals.");
  }

  const seriesByIndividual = {};
  const coverageByIndividual = {};
  const stats = {};
  const individualPalette = {};

  Object.entries(summary.series_by_individual || {}).forEach(([individual, sets]) => {
    seriesByIndividual[individual] = {};
    Object.entries(sets || {}).forEach(([setName, series]) => {
      seriesByIndividual[individual][setName] = {
        times: Array.isArray(series?.times) ? series.times.map(value => Number(value) || 0) : [],
        positions: Array.isArray(series?.positions)
          ? series.positions.map(position => [Number(position?.[0]) || 0, Number(position?.[1]) || 0])
          : [],
      };
    });
  });

  Object.entries(summary.coverage_by_individual || {}).forEach(([individual, sets]) => {
    coverageByIndividual[individual] = {};
    Object.entries(sets || {}).forEach(([setName, coverage]) => {
      coverageByIndividual[individual][setName] = {
        startMs: Number(coverage?.start_ms) || 0,
        endMs: Number(coverage?.end_ms) || 0,
      };
    });
  });

  Object.entries(summary.stats || {}).forEach(([individual, item]) => {
    stats[individual] = {
      rowCount: Number(item?.row_count) || 0,
      medianFixS: finiteOrNull(item?.median_fix_s),
      medianStepM: finiteOrNull(item?.median_step_m),
      medianSpeedMps: finiteOrNull(item?.median_speed_mps),
      p95StepM: finiteOrNull(item?.p95_step_m),
      p95SpeedMps: finiteOrNull(item?.p95_speed_mps),
      suspectedCount: Number(item?.suspected_count) || 0,
      confirmedCount: Number(item?.confirmed_count) || 0,
    };
  });

  const colorFields = Array.isArray(summary.color_fields) ? summary.color_fields.map(field => ({
    key: String(field?.key || ""),
    label: String(field?.label || field?.key || ""),
    kind: String(field?.kind || "categorical"),
    source: String(field?.source || "raw"),
  })).filter(field => field.key) : [];
  const colorFieldByKey = new Map(colorFields.map(field => [field.key, field]));

  individuals.forEach((individual, index) => {
    individualPalette[individual] = hslToRgb((index * 137.508) % 360, 0.76, 0.54);
    if (!stats[individual]) {
      stats[individual] = {
        rowCount: 0,
        medianFixS: null,
        medianStepM: null,
        medianSpeedMps: null,
        p95StepM: null,
        p95SpeedMps: null,
        suspectedCount: 0,
        confirmedCount: 0,
      };
    }
  });

  const defaultColorBy = colorFields.some(field => field.key === preferredColorBy)
    ? preferredColorBy
    : colorFields[0]?.key || "step_length_m";

  const data = {
    totalRows: Number(summary.total_rows) || 0,
    individuals,
    speciesByIndividual: summary.species_by_individual || {},
    seriesByIndividual,
    coverageByIndividual,
    stats,
    fixes: [],
    fixByKey: new Map(),
    overviewFixes: parseMovementFixes(summary.fixes || []),
    detailFixes: [],
    detailState: "idle",
    detailIndividual: "",
    selectedFixKeys: new Set(),
    selectedIndividuals: new Set(individuals),
    colorFields,
    colorFieldByKey,
    colorStyles: new Map(),
    defaultColorBy,
    individualPalette,
    minTimeMs: Number(summary.min_time_ms) || 0,
    maxTimeMs: Number(summary.max_time_ms) || 0,
    initialView: {
      longitude: Number(summary.initial_view?.longitude) || 0,
      latitude: Number(summary.initial_view?.latitude) || 0,
      zoom: Number(summary.initial_view?.zoom) || 1,
    },
  };
  refreshMovementFixCollections(data);
  return data;
}

function parseMovementFixes(items) {
  return Array.isArray(items) ? items.map(item => ({
    fixKey: String(item?.fix_key || ""),
    individual: String(item?.individual || ""),
    setName: String(item?.set || "train") || "train",
    timeMs: Number(item?.time_ms) || 0,
    position: [Number(item?.lon) || 0, Number(item?.lat) || 0],
    attributes: { ...(item?.attributes || {}) },
    review: {
      status: String(item?.review?.status || ""),
      issueId: String(item?.review?.issue_id || ""),
      issueType: String(item?.review?.issue_type || ""),
      issueNote: String(item?.review?.issue_note || ""),
      ownerQuestion: String(item?.review?.owner_question || ""),
      reviewUser: String(item?.review?.review_user || ""),
      reviewedAt: String(item?.review?.reviewed_at || ""),
    },
  })).filter(item => item.fixKey) : [];
}

function refreshMovementFixCollections(data) {
  if (!data) {
    return;
  }
  const merged = new Map();
  for (const fix of [...(data.overviewFixes || []), ...(data.detailFixes || [])]) {
    merged.set(fix.fixKey, fix);
  }
  data.fixes = Array.from(merged.values())
    .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
  data.fixByKey = new Map(data.fixes.map(fix => [fix.fixKey, fix]));
  data.colorStyles = computeMovementColorStyles(data.colorFields, data.fixes);
}

function computeMovementColorStyles(colorFields, fixes) {
  const colorStyles = new Map();
  for (const field of colorFields) {
    if (field.kind === "numeric") {
      const values = fixes
        .map(fix => finiteOrNull(fix.attributes[field.key]))
        .filter(value => typeof value === "number");
      colorStyles.set(field.key, {
        kind: "numeric",
        range: computeNumericRange(values),
      });
      continue;
    }
    if (field.kind === "boolean") {
      colorStyles.set(field.key, { kind: "boolean" });
      continue;
    }
    const categories = uniqueStrings(fixes.map(fix => {
      const raw = fix.attributes[field.key];
      return raw === null || raw === undefined || raw === "" ? "Missing" : String(raw);
    }));
    const palette = new Map();
    categories.forEach((category, index) => {
      if (category === "Missing") {
        palette.set(category, [120, 136, 153, 150]);
      } else if (category === "suspected") {
        palette.set(category, [245, 181, 54, POINT_ALPHA]);
      } else if (category === "confirmed") {
        palette.set(category, [241, 106, 124, POINT_ALPHA]);
      } else {
        palette.set(category, [...hslToRgb((index * 137.508) % 360, 0.72, 0.56), POINT_ALPHA]);
      }
    });
    colorStyles.set(field.key, {
      kind: "categorical",
      categories: palette,
    });
  }
  return colorStyles;
}

function computeNumericRange(values) {
  if (!values.length) {
    return { min: 0, max: 1, observedMin: 0, observedMax: 1 };
  }
  const sorted = [...values].sort((left, right) => left - right);
  const observedMin = sorted[0];
  const observedMax = sorted[sorted.length - 1];
  const lowerQuantile = quantile(sorted, NUMERIC_COLOR_MIN_QUANTILE);
  const upperQuantile = quantile(sorted, NUMERIC_COLOR_MAX_QUANTILE);
  if (lowerQuantile === upperQuantile) {
    return {
      min: observedMin,
      max: observedMax || observedMin + 1,
      observedMin,
      observedMax,
    };
  }
  return {
    min: lowerQuantile,
    max: upperQuantile,
    observedMin,
    observedMax,
  };
}

function interpolateNumericColor(value, range, alpha) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return [120, 136, 153, 120];
  }
  const span = (range.max - range.min) || 1;
  const t = Math.max(0, Math.min(1, (value - range.min) / span));
  const start = [76, 196, 255];
  const mid = [255, 214, 92];
  const end = [242, 80, 103];
  const [from, to, localT] = t < 0.5
    ? [start, mid, t / 0.5]
    : [mid, end, (t - 0.5) / 0.5];
  return [
    Math.round(from[0] + (to[0] - from[0]) * localT),
    Math.round(from[1] + (to[1] - from[1]) * localT),
    Math.round(from[2] + (to[2] - from[2]) * localT),
    alpha,
  ];
}

function numericLegendGradient() {
  const stops = [0, 0.25, 0.5, 0.75, 1]
    .map(stop => {
      const color = interpolateNumericColor(stop, { min: 0, max: 1 }, 255);
      return `${rgbaCss(color)} ${Math.round(stop * 100)}%`;
    });
  return stops.join(", ");
}

function quantile(sortedValues, q) {
  if (!sortedValues.length) {
    return 0;
  }
  const idx = (sortedValues.length - 1) * q;
  const lower = Math.floor(idx);
  const upper = Math.min(sortedValues.length - 1, lower + 1);
  if (lower === upper) {
    return sortedValues[lower];
  }
  const ratio = idx - lower;
  return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * ratio;
}

function finiteOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function formatCount(value) {
  return new Intl.NumberFormat().format(Number(value) || 0);
}

function formatMaybeNumber(value, suffix) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "n/a";
  }
  const formatted = Math.abs(value) >= 100 ? value.toFixed(0) : value.toFixed(value >= 10 ? 1 : 2);
  return `${formatted}${suffix ? ` ${suffix}` : ""}`;
}

function formatPercent(value) {
  const percent = Number(value) * 100;
  if (!Number.isFinite(percent)) {
    return "n/a";
  }
  return `${Number.isInteger(percent) ? percent.toFixed(0) : percent.toFixed(1)}th`;
}

function formatTimestamp(timeMs) {
  const date = new Date(Number(timeMs) || 0);
  if (Number.isNaN(date.getTime())) {
    return "Invalid time";
  }
  return date.toISOString().replace("T", " ").replace(".000Z", "Z");
}

function formatDatasetLabel(dataset, currentHeadDatasetId) {
  const parts = [shortId(dataset.dataset_id), formatDateTime(dataset.created_at)];
  if (dataset.dataset_id === currentHeadDatasetId) {
    parts.unshift("head");
  }
  return parts.join(" | ");
}

function shortId(value) {
  if (!value) {
    return "none";
  }
  const parts = String(value).split("_");
  return parts[parts.length - 1];
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value || "unknown");
  }
  return date.toLocaleString();
}

function uniqueStrings(values) {
  return Array.from(new Set(values.map(value => String(value))));
}

function uniqueNonEmpty(values) {
  return Array.from(new Set(values.filter(Boolean).map(value => String(value))));
}

function reportIssueType(fix) {
  const issueType = String(fix?.review?.issueType || "").trim();
  return issueType || "Unspecified issue";
}

function reportTrackKey(individual, setName) {
  return `${String(individual)}\u0000${String(setName || "train")}`;
}

async function createReportSnapshotRenderer({ style }) {
  const container = document.createElement("div");
  Object.assign(container.style, {
    position: "fixed",
    left: "-20000px",
    top: "0",
    width: "1100px",
    height: "700px",
    opacity: "1",
    pointerEvents: "none",
    zIndex: "-1",
  });
  document.body.appendChild(container);

  const map = new maplibregl.Map({
    container,
    style,
    center: [0, 20],
    zoom: 2,
    attributionControl: false,
    interactive: false,
    preserveDrawingBuffer: true,
    fadeDuration: 0,
  });
  const overlay = new deck.MapboxOverlay({ interleaved: true, layers: [] });
  map.addControl(overlay);
  await waitForMapReady(map);

  return {
    async capture(snapshotWindow) {
      if (!snapshotWindow?.windowFixes?.length) {
        return null;
      }
      const bounds = buildWindowBounds(snapshotWindow.windowFixes);
      overlay.setProps({ layers: buildReportSnapshotLayers(snapshotWindow) });
      if (bounds) {
        map.fitBounds(bounds, { padding: 70, duration: 0, maxZoom: 15 });
      }
      map.triggerRepaint();
      await waitForMapReady(map);
      await waitForAnimationFrames(2);
      try {
        return map.getCanvas().toDataURL("image/png");
      } catch {
        return null;
      }
    },
    destroy() {
      try {
        overlay.setProps({ layers: [] });
      } catch {}
      try {
        map.remove();
      } catch {}
      container.remove();
    },
  };
}

function buildReportSnapshotLayers(snapshotWindow) {
  const anchorSet = new Set(snapshotWindow.anchorFixKeys || []);
  const contextPoints = [];
  const suspiciousPoints = [];
  for (const fix of snapshotWindow.windowFixes || []) {
    const point = {
      position: fix.position,
      fixKey: fix.fixKey,
    };
    if (anchorSet.has(fix.fixKey)) {
      suspiciousPoints.push(point);
    } else {
      contextPoints.push(point);
    }
  }
  return [
    new deck.PathLayer({
      id: `report-snapshot-path-${snapshotWindow.snapshotKey}`,
      data: [{ path: (snapshotWindow.windowFixes || []).map(fix => fix.position) }],
      getPath: item => item.path,
      getColor: [26, 52, 74, 190],
      getWidth: 3,
      widthMinPixels: 2,
      pickable: false,
    }),
    new deck.ScatterplotLayer({
      id: `report-snapshot-context-${snapshotWindow.snapshotKey}`,
      data: contextPoints,
      getPosition: item => item.position,
      getFillColor: [78, 144, 184, 170],
      getRadius: 75,
      radiusMinPixels: 4,
      radiusMaxPixels: 9,
      pickable: false,
    }),
    new deck.ScatterplotLayer({
      id: `report-snapshot-anchors-${snapshotWindow.snapshotKey}`,
      data: suspiciousPoints,
      getPosition: item => item.position,
      getFillColor: [242, 80, 103, 235],
      getLineColor: [255, 255, 255, 255],
      stroked: true,
      lineWidthMinPixels: 2,
      getRadius: 150,
      radiusMinPixels: 8,
      radiusMaxPixels: 16,
      pickable: false,
    }),
  ];
}

function buildWindowBounds(windowFixes) {
  if (!windowFixes.length) {
    return null;
  }
  let minLon = windowFixes[0].position[0];
  let maxLon = windowFixes[0].position[0];
  let minLat = windowFixes[0].position[1];
  let maxLat = windowFixes[0].position[1];
  for (const fix of windowFixes) {
    minLon = Math.min(minLon, fix.position[0]);
    maxLon = Math.max(maxLon, fix.position[0]);
    minLat = Math.min(minLat, fix.position[1]);
    maxLat = Math.max(maxLat, fix.position[1]);
  }
  const lonPad = Math.max((maxLon - minLon) * 0.15, 0.002);
  const latPad = Math.max((maxLat - minLat) * 0.15, 0.002);
  return new maplibregl.LngLatBounds(
    [minLon - lonPad, minLat - latPad],
    [maxLon + lonPad, maxLat + latPad],
  );
}

async function waitForMapReady(map) {
  if (!map) {
    return;
  }
  if (map.loaded()) {
    await new Promise(resolve => map.once("idle", resolve));
    return;
  }
  await new Promise(resolve => map.once("idle", resolve));
}

async function waitForAnimationFrames(count) {
  for (let index = 0; index < count; index += 1) {
    await new Promise(resolve => requestAnimationFrame(() => resolve()));
  }
}

function statusPill(status) {
  const pill = document.createElement("span");
  const normalized = status === "suspected" || status === "confirmed" ? status : "unreviewed";
  pill.className = `movement-pill ${normalized}`;
  pill.textContent = normalized;
  return pill;
}

function statChip(text) {
  const element = document.createElement("span");
  element.textContent = text;
  return element;
}

function rangeToPercent(min, max, start, end) {
  if (max <= min) {
    return { left: 0, width: 100 };
  }
  const left = ((start - min) / (max - min)) * 100;
  const width = ((end - start) / (max - min)) * 100;
  return {
    left: Math.max(0, Math.min(100, left)),
    width: Math.max(0.25, Math.min(100, width)),
  };
}

function colorCss(rgb, setName, alpha) {
  const [r, g, b] = splitColor(rgb, setName, Math.round(alpha * 255));
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function rgbaCss(color) {
  const [r, g, b, a = 255] = color;
  return `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(3)})`;
}

function splitColor(rgb, setName, alpha) {
  const base = [...rgb];
  if (setName === "test") {
    base[0] = Math.min(255, base[0] + 24);
    base[1] = Math.min(255, base[1] + 14);
  }
  return [base[0], base[1], base[2], alpha];
}

function hslToRgb(h, s, l) {
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs((h / 60) % 2 - 1));
  const m = l - c / 2;
  let rgb = [0, 0, 0];
  if (h < 60) rgb = [c, x, 0];
  else if (h < 120) rgb = [x, c, 0];
  else if (h < 180) rgb = [0, c, x];
  else if (h < 240) rgb = [0, x, c];
  else if (h < 300) rgb = [x, 0, c];
  else rgb = [c, 0, x];
  return rgb.map(value => Math.round((value + m) * 255));
}

function visibleSets(showTrain, showTest) {
  const sets = [];
  if (showTrain) sets.push("train");
  if (showTest) sets.push("test");
  return sets.length ? sets : ["train"];
}

function interpolateSeriesPosition(series, currentTimeMs) {
  if (!series || !Array.isArray(series.times) || !Array.isArray(series.positions) || !series.times.length) {
    return null;
  }
  if (currentTimeMs <= series.times[0]) {
    return series.positions[0];
  }
  const lastIndex = series.times.length - 1;
  if (currentTimeMs >= series.times[lastIndex]) {
    return series.positions[lastIndex];
  }
  for (let index = 1; index < series.times.length; index += 1) {
    const leftTime = series.times[index - 1];
    const rightTime = series.times[index];
    if (currentTimeMs > rightTime) {
      continue;
    }
    const leftPos = series.positions[index - 1];
    const rightPos = series.positions[index];
    const span = rightTime - leftTime;
    if (span <= 0) {
      return rightPos;
    }
    const ratio = (currentTimeMs - leftTime) / span;
    return [
      leftPos[0] + (rightPos[0] - leftPos[0]) * ratio,
      leftPos[1] + (rightPos[1] - leftPos[1]) * ratio,
    ];
  }
  return series.positions[lastIndex];
}

function stepTouchesArtifact(step, logicalName) {
  return (step?.input_artifacts || []).includes(logicalName)
    || (step?.output_artifacts || []).includes(logicalName)
    || (step?.removed_artifacts || []).includes(logicalName);
}

function formatColorValue(value, kind) {
  if (value === null || value === undefined || value === "") {
    return "missing";
  }
  if (kind === "numeric") {
    return formatMaybeNumber(Number(value), "");
  }
  return String(value);
}

function familyStartupPriority(name) {
  if (name === DEFAULT_FAMILY) return 0;
  if (name === "movement_hightemporalres") return 1;
  if (name === "movement_raw") return 2;
  return 3;
}

function familyPresetFromLocation() {
  try {
    const value = new URLSearchParams(window.location.search).get("family") || "";
    return ["movement_raw", "movement_clean", "movement_hightemporalres"].includes(value) ? value : "";
  } catch {
    return "";
  }
}

function setFamilyPresetInUrl(familyName) {
  try {
    const url = new URL(window.location.href);
    if (familyName) {
      url.searchParams.set("family", familyName);
    } else {
      url.searchParams.delete("family");
    }
    window.history.replaceState({}, "", url);
  } catch {}
}

function chooseStartupFamily(families, storedFamily) {
  const names = families.map(family => family.name);
  const preferredFamily = familyPresetFromLocation();
  if (preferredFamily && names.includes(preferredFamily)) {
    return preferredFamily;
  }
  if (storedFamily && names.includes(storedFamily)) {
    return storedFamily;
  }
  const sorted = [...families].sort((left, right) => (
    familyStartupPriority(left.name) - familyStartupPriority(right.name)
      || left.name.localeCompare(right.name)
  ));
  return sorted[0]?.name || "";
}

function legendItem(label, color) {
  return `
    <div class="movement-legend-item">
      <span class="movement-legend-item-label">${escapeHtml(label)}</span>
      <span class="movement-legend-swatch" style="background:${rgbaCss(color)};"></span>
    </div>
  `;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

const app = new MovementExampleApp({
  mountEl: document.getElementById("app"),
});

app.init().catch(error => {
  console.error(error);
});
