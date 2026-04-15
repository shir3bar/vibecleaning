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
const REPORT_SNAPSHOT_IDLE_TIMEOUT_MS = 12000;

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
      reportDetail: null,
    };
    this.map = null;
    this.mapLoaded = false;
    this.overlay = null;
    this.pendingIssueStatus = "suspected";
    this.lastReportLinks = [];
    this.assetsLoaded = false;
    this.mapErrorMessage = "";
    this.reportDetailLoadId = 0;
    this.thresholdState = {
      fieldKey: "",
      value: null,
      reverse: false,
      selectedLevels: [],
      histogramMode: "full",
      histogramMin: null,
      histogramMax: null,
    };
    this.thresholdInputPendingBlur = false;
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
        .movement-threshold {
          position: absolute;
          right: 16px;
          bottom: 16px;
          z-index: 4;
          width: min(320px, calc(100% - 32px));
          display: grid;
          gap: 10px;
          padding: 12px 13px;
          border-radius: 14px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(7, 11, 22, 0.9);
          box-shadow: 0 18px 44px rgba(0, 0, 0, 0.32);
          color: #e8eef7;
          pointer-events: auto;
        }
        .movement-threshold.hidden {
          display: none;
        }
        .movement-threshold-head {
          display: grid;
          gap: 2px;
        }
        .movement-threshold-title {
          font-size: 12px;
          font-weight: 600;
          color: #eef4fb;
        }
        .movement-threshold-subtitle {
          font-size: 11px;
          color: #8fa5bc;
        }
        .movement-threshold-meta {
          display: flex;
          flex-wrap: wrap;
          gap: 8px 12px;
          font-size: 11px;
          color: #dbe5f0;
        }
        .movement-threshold-note {
          font-size: 10px;
          color: #7e93aa;
          line-height: 1.4;
        }
        .movement-threshold-chart-wrap {
          display: grid;
          gap: 6px;
        }
        .movement-threshold-chart {
          position: relative;
          display: flex;
          align-items: end;
          gap: 3px;
          height: 112px;
          padding: 10px 8px 8px;
          border-radius: 12px;
          border: 1px solid rgba(255, 255, 255, 0.07);
          background:
            linear-gradient(180deg, rgba(76, 196, 255, 0.06), rgba(10, 17, 29, 0.08)),
            rgba(15, 23, 42, 0.72);
          cursor: crosshair;
          overflow: hidden;
        }
        .movement-threshold-bar {
          flex: 1 1 0;
          min-width: 0;
          border-radius: 999px 999px 3px 3px;
          background: linear-gradient(180deg, rgba(87, 218, 174, 0.95), rgba(50, 160, 255, 0.82));
        }
        .movement-threshold-line {
          position: absolute;
          top: 7px;
          bottom: 7px;
          width: 2px;
          margin-left: -1px;
          border-radius: 999px;
          background: rgba(255, 228, 122, 0.98);
          box-shadow: 0 0 0 1px rgba(255, 228, 122, 0.16), 0 0 18px rgba(255, 228, 122, 0.34);
        }
        .movement-threshold-line::after {
          content: "";
          position: absolute;
          left: 50%;
          top: -4px;
          width: 10px;
          height: 10px;
          transform: translateX(-50%);
          border-radius: 999px;
          background: rgba(255, 228, 122, 0.98);
        }
        .movement-threshold-range {
          display: flex;
          justify-content: space-between;
          gap: 12px;
          font-size: 11px;
          color: #c8d5e4;
        }
        .movement-threshold-zoom {
          display: inline-flex;
          gap: 8px;
          flex-wrap: wrap;
        }
        .movement-threshold-zoom button {
          padding: 6px 10px;
          border-radius: 999px;
          border: 1px solid rgba(255, 255, 255, 0.12);
          background: rgba(255, 255, 255, 0.04);
          color: #dce7f3;
          font-size: 12px;
        }
        .movement-threshold-zoom button.is-active {
          background: rgba(80, 180, 255, 0.18);
          border-color: rgba(80, 180, 255, 0.35);
        }
        .movement-threshold-inline-input,
        .movement-threshold-range-input {
          min-width: 0;
          width: 88px;
          padding: 5px 8px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
          font: inherit;
          font-size: 12px;
        }
        .movement-threshold-inline-input {
          width: 96px;
          margin-left: 4px;
        }
        .movement-threshold-actions {
          display: flex;
          justify-content: flex-end;
          gap: 8px;
        }
        .movement-threshold-toggle {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          font-size: 12px;
          color: #c8d5e4;
          cursor: pointer;
        }
        .movement-threshold-toggle input {
          margin: 0;
        }
        .movement-threshold-levels {
          display: grid;
          gap: 8px;
          max-height: 220px;
          overflow: auto;
          padding-right: 4px;
        }
        .movement-threshold-level {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr) auto;
          align-items: center;
          gap: 8px;
          padding: 8px 10px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.06);
          background: rgba(15, 23, 42, 0.58);
          font-size: 12px;
          color: #e5edf7;
          cursor: pointer;
        }
        .movement-threshold-level input {
          margin: 0;
        }
        .movement-threshold-level-label {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .movement-threshold-level-count {
          color: #9bb0c6;
          font-variant-numeric: tabular-nums;
        }
        .movement-threshold-range-label {
          display: inline-flex;
          align-items: center;
          gap: 6px;
        }
        .movement-threshold button {
          padding: 8px 12px;
          border: none;
          border-radius: 10px;
          background: rgba(255, 255, 255, 0.08);
          color: #e5edf7;
          cursor: pointer;
          font: inherit;
          font-size: 13px;
        }
        .movement-threshold button:disabled {
          cursor: not-allowed;
          opacity: 0.45;
        }
        .movement-threshold button.movement-emphasis {
          background: rgba(67, 206, 162, 0.22);
          color: #d8fff3;
        }
        .movement-threshold-empty {
          padding: 12px;
          border-radius: 12px;
          border: 1px solid rgba(255, 255, 255, 0.06);
          background: rgba(15, 23, 42, 0.62);
          font-size: 12px;
          color: #9bb0c6;
          line-height: 1.45;
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
          max-height: min(88vh, 900px);
          display: flex;
          flex-direction: column;
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
          flex: 1 1 auto;
          gap: 14px;
          padding: 16px;
          overflow: auto;
        }
        .movement-modal-body label {
          display: grid;
          gap: 6px;
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-modal-body label.movement-inline-check {
          display: inline-flex;
          align-items: center;
          gap: 8px;
        }
        .movement-modal-body label.movement-inline-check input {
          min-width: 0;
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
        @media (max-width: 560px) {
          .movement-threshold-range {
            flex-direction: column;
            align-items: stretch;
          }
          .movement-threshold-range-label {
            justify-content: space-between;
          }
          .movement-threshold-inline-input,
          .movement-threshold-range-input {
            width: 100%;
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
            <div class="movement-threshold hidden" data-role="threshold-pane"></div>
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
            <label>User
              <input type="text" data-role="report-user" placeholder="Name used for attribution">
            </label>
            <label>Report type
              <select data-role="report-type">
                <option value="issue_first">Issue-first debug report</option>
                <option value="individual_profile">Per-individual profile report</option>
              </select>
            </label>
            <label>Scope
              <select data-role="report-scope">
                <option value="visible">Visible individuals</option>
                <option value="full">Full study</option>
              </select>
            </label>
            <label>Individual
              <select data-role="report-individual"></select>
            </label>
            <label data-role="report-output-mode-wrap" hidden>Output mode
              <select data-role="report-output-mode">
                <option value="combined">Single combined report</option>
                <option value="separate">Separate files + index</option>
              </select>
            </label>
            <label data-role="report-screenshot-mode-wrap">Screenshot mode
              <select data-role="report-screenshot-mode">
                <option value="auto">Auto snapshot of current map</option>
                <option value="manual">Manual placeholder</option>
              </select>
            </label>
            <label data-role="report-basemap-wrap">Report basemap
              <select data-role="report-basemap">
                <option value="current">Match current map when possible</option>
                <option value="Positron">Positron</option>
                <option value="Voyager">Voyager</option>
                <option value="Dark Matter">Dark Matter</option>
              </select>
            </label>
            <label data-role="report-snapshot-limit-wrap">Auto snapshot sample
              <input type="number" min="1" step="1" data-role="report-snapshot-limit" placeholder="All snapshot windows">
            </label>
            <label class="movement-inline-check" data-role="report-spread-individuals-wrap">
              <input type="checkbox" data-role="report-spread-individuals" checked>
              Spread auto snapshots across individuals
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
      thresholdPane: this.mountEl.querySelector('[data-role="threshold-pane"]'),
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
      reportUser: this.mountEl.querySelector('[data-role="report-user"]'),
      reportType: this.mountEl.querySelector('[data-role="report-type"]'),
      reportScope: this.mountEl.querySelector('[data-role="report-scope"]'),
      reportIndividual: this.mountEl.querySelector('[data-role="report-individual"]'),
      reportOutputModeWrap: this.mountEl.querySelector('[data-role="report-output-mode-wrap"]'),
      reportOutputMode: this.mountEl.querySelector('[data-role="report-output-mode"]'),
      reportScreenshotModeWrap: this.mountEl.querySelector('[data-role="report-screenshot-mode-wrap"]'),
      reportScreenshotMode: this.mountEl.querySelector('[data-role="report-screenshot-mode"]'),
      reportBasemapWrap: this.mountEl.querySelector('[data-role="report-basemap-wrap"]'),
      reportBasemap: this.mountEl.querySelector('[data-role="report-basemap"]'),
      reportSnapshotLimitWrap: this.mountEl.querySelector('[data-role="report-snapshot-limit-wrap"]'),
      reportSnapshotLimit: this.mountEl.querySelector('[data-role="report-snapshot-limit"]'),
      reportSpreadIndividualsWrap: this.mountEl.querySelector('[data-role="report-spread-individuals-wrap"]'),
      reportSpreadIndividuals: this.mountEl.querySelector('[data-role="report-spread-individuals"]'),
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
    this.refs.reportBasemap.value = "current";
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
      this.clearThresholdState();
      this.saveUiState();
      this.renderLegend();
      this.renderThresholdPane();
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
      this.clearThresholdState();
      this.data.selectedIndividuals = new Set(this.data.individuals);
      this.saveUiState();
      this.renderIndividuals();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      void this.loadDetailForCurrentSelection();
    });
    this.refs.selectNone.addEventListener("click", () => {
      if (!this.data) return;
      this.clearThresholdState();
      this.data.selectedIndividuals = new Set();
      this.data.selectedFixKeys = new Set();
      this.saveUiState();
      this.renderIndividuals();
      this.renderThresholdPane();
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
      void this.loadDetailForCurrentSelection();
    });
    this.refs.selectSuspicious.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedFixKeys = new Set(this.getSuspiciousFixes().map(fix => fix.fixKey));
      this.renderThresholdPane();
      this.renderSelectedFixes();
      this.renderLayers();
      this.updateActionButtons();
    });
    this.refs.clearFixes.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedFixKeys = new Set();
      this.renderThresholdPane();
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
    this.refs.thresholdPane.addEventListener("click", event => this.handleThresholdPaneClick(event));
    this.refs.thresholdPane.addEventListener("change", event => this.handleThresholdPaneChange(event));
    this.refs.thresholdPane.addEventListener("focusin", event => this.handleThresholdPaneFocusIn(event));

    this.refs.issueClose.addEventListener("click", () => this.closeModal(this.refs.issueModal, this.refs.issueSubmit));
    this.refs.issueSubmit.addEventListener("click", async () => this.submitIssueAction());
    this.refs.reportClose.addEventListener("click", () => this.closeModal(this.refs.reportModal, this.refs.reportSubmit));
    this.refs.reportType.addEventListener("change", () => {
      void this.handleReportTypeChange();
    });
    this.refs.reportScope.addEventListener("change", () => {
      void this.handleReportScopeChange();
    });
    this.refs.reportIndividual.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportOutputMode.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportScreenshotMode.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportBasemap.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportSnapshotLimit.addEventListener("input", () => this.renderReportSelection());
    this.refs.reportSpreadIndividuals.addEventListener("change", () => this.renderReportSelection());
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
      this.cancelRequest("reportDetail");
      return;
    }
    if (level === "study") {
      this.cancelRequest("study");
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("detail");
      this.cancelRequest("reportDetail");
      return;
    }
    if (level === "dataset") {
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("detail");
      this.cancelRequest("reportDetail");
      return;
    }
    if (level === "artifact") {
      this.cancelRequest("overview");
      this.cancelRequest("detail");
      this.cancelRequest("reportDetail");
    }
  }

  handleVisibilityChange() {
    this.clearThresholdState();
    this.saveUiState();
    this.renderIndividuals();
    this.renderThresholdPane();
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
    this.clearThresholdState();
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
    this.renderThresholdPane();
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
      this.renderThresholdPane();
      this.updateTimeLabel();
      this.hideOverlay();
      await this.rebuildMap(false);
      this.resetView();
      this.updateActionButtons();
      this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Loading editable fixes for the visible individuals...`);
      void this.loadDetailForCurrentSelection();
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
      this.renderThresholdPane();
      this.updateTimeLabel();
      this.hideOverlay();
      await this.rebuildMap(false);
      this.resetView();
      this.updateActionButtons();
      this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Loading editable fixes for the visible individuals...`);
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
    this.clearThresholdState();
    if (shouldSelect) {
      this.data.selectedIndividuals.add(individual);
    } else {
      this.data.selectedIndividuals.delete(individual);
    }
    this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(this.data.selectedFixKeys, this.getSelectedIndividuals());
    this.saveUiState();
    this.renderIndividuals();
    this.renderThresholdPane();
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
        empty.textContent = `Loading editable fixes for ${formatCount(this.data.detailIndividuals.length)} visible individuals...`;
      } else if (!this.getSelectedIndividuals().length) {
        empty.textContent = "Select at least one individual to review fixes.";
      } else if (!this.hasLoadedDetailSelection()) {
        empty.textContent = "Loading editable fixes for the current visible individuals...";
      } else if (this.data.detailState === "error") {
        empty.textContent = "Editable fixes could not be loaded for the current visible selection. Try selecting the individuals again.";
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
        const issueTypes = reportIssueTypes(fix);
        note.textContent = `${issueTypes.join(", ") || "Issue"}: ${fix.review.issueNote || fix.review.ownerQuestion || ""}`;
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
    const visibleSetNames = this.getVisibleSetNames();
    const pathData = [];
    const pointData = [];
    const thresholdPointData = [];
    const selectedThresholdPointData = [];
    const selectedPointData = [];
    const cursorData = [];
    const thresholdMatchKeys = this.getThresholdContext()?.matchKeys || new Set();

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
        if (thresholdMatchKeys.has(fix.fixKey)) {
          selectedThresholdPointData.push({
            fixKey: fix.fixKey,
            position: fix.position,
          });
        }
      } else {
        pointData.push(point);
        if (thresholdMatchKeys.has(fix.fixKey)) {
        thresholdPointData.push({
          fixKey: fix.fixKey,
          position: fix.position,
        });
        }
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
          id: "movement-threshold-points",
          data: thresholdPointData,
          getPosition: item => item.position,
          getLineColor: [255, 236, 148, 255],
          filled: false,
          stroked: true,
          lineWidthMinPixels: 2.5,
          getRadius: 108,
          radiusMinPixels: 6,
          radiusMaxPixels: 12,
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
          id: "movement-selected-threshold-points",
          data: selectedThresholdPointData,
          getPosition: item => item.position,
          getLineColor: [255, 236, 148, 255],
          filled: false,
          stroked: true,
          lineWidthMinPixels: 3,
          getRadius: 156,
          radiusMinPixels: 9,
          radiusMaxPixels: 18,
          pickable: false,
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
    this.renderThresholdPane();
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

  getSelectedIndividuals() {
    if (!this.data) {
      return [];
    }
    return [...this.data.selectedIndividuals].sort((left, right) => left.localeCompare(right));
  }

  getVisibleSetNames() {
    return new Set(visibleSets(this.refs.showTrain.checked, this.refs.showTest.checked));
  }

  getCurrentColorField() {
    if (!this.data) {
      return null;
    }
    return this.data.colorFieldByKey.get(this.refs.colorBy.value) || this.data.colorFields[0] || null;
  }

  getVisibleReviewFixes() {
    if (!this.data) {
      return [];
    }
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    const visibleSetNames = this.getVisibleSetNames();
    return this.data.fixes.filter(
      fix => visibleIndividuals.has(fix.individual) && visibleSetNames.has(fix.setName),
    );
  }

  clearThresholdState() {
    this.thresholdState = {
      fieldKey: "",
      value: null,
      reverse: false,
      selectedLevels: [],
      histogramMode: "full",
      histogramMin: null,
      histogramMax: null,
    };
  }

  getThresholdContext() {
    if (!this.data) {
      return null;
    }
    const field = this.getCurrentColorField();
    const visibleFixes = this.getVisibleReviewFixes();
    if (!field) {
      return {
        field: null,
        visibleFixes,
        numericFixes: [],
        thresholdValue: null,
        histogram: null,
        matchKeys: new Set(),
        uncheckedMatchKeys: new Set(),
      };
    }

    const thresholdValue = this.thresholdState.fieldKey === field.key && typeof this.thresholdState.value === "number"
      ? this.thresholdState.value
      : null;
    const reverse = this.thresholdState.fieldKey === field.key && this.thresholdState.reverse === true;
    const histogramMode = this.thresholdState.fieldKey === field.key && this.thresholdState.histogramMode === "clipped"
      ? "clipped"
      : "full";
    const manualHistogramMin = this.thresholdState.fieldKey === field.key
      ? finiteOrNull(this.thresholdState.histogramMin)
      : null;
    const manualHistogramMax = this.thresholdState.fieldKey === field.key
      ? finiteOrNull(this.thresholdState.histogramMax)
      : null;
    const selectedLevels = this.thresholdState.fieldKey === field.key && Array.isArray(this.thresholdState.selectedLevels)
      ? uniqueNonEmpty(this.thresholdState.selectedLevels)
      : [];
    if (field.kind !== "numeric") {
      const levelCounts = new Map();
      for (const fix of visibleFixes) {
        const level = discreteFieldLevelLabel(field, fix.attributes[field.key]);
        levelCounts.set(level, (levelCounts.get(level) || 0) + 1);
      }
      const levelOptions = Array.from(levelCounts.entries())
        .map(([level, count]) => ({ level, count }))
        .sort((left, right) => right.count - left.count || left.level.localeCompare(right.level, undefined, { sensitivity: "base" }));
      const selectedLevelSet = new Set(selectedLevels);
      const matchItems = selectedLevelSet.size
        ? visibleFixes.filter(fix => selectedLevelSet.has(discreteFieldLevelLabel(field, fix.attributes[field.key])))
        : [];
      const matchKeys = new Set(matchItems.map(item => item.fixKey));
      const uncheckedMatchKeys = new Set(
        matchItems
          .map(item => item.fixKey)
          .filter(fixKey => !this.data.selectedFixKeys.has(fixKey)),
      );
      return {
        field,
        visibleFixes,
        numericFixes: [],
        thresholdValue: null,
        histogram: null,
        reverse,
        histogramMode,
        histogramInputMin: null,
        histogramInputMax: null,
        selectedLevels,
        levelOptions,
        matchKeys,
        uncheckedMatchKeys,
      };
    }

    const numericFixes = visibleFixes
      .map(fix => ({
        fix,
        value: finiteOrNull(fix.attributes[field.key]),
      }))
      .filter(item => typeof item.value === "number");
    if (!numericFixes.length) {
      return {
        field,
        visibleFixes,
        numericFixes,
        thresholdValue: null,
        histogram: null,
        reverse,
        histogramMode,
        selectedLevels: [],
        levelOptions: [],
        matchKeys: new Set(),
        uncheckedMatchKeys: new Set(),
      };
    }

    const colorRange = this.data.colorStyles.get(field.key)?.range || null;
    const defaultHistogramMin = Number.isFinite(colorRange?.min) ? colorRange.min : null;
    const defaultHistogramMax = Number.isFinite(colorRange?.max) ? colorRange.max : null;
    const useCustomHistogramBounds = manualHistogramMin !== null || manualHistogramMax !== null;
    const clippedMin = useCustomHistogramBounds
      ? (manualHistogramMin ?? defaultHistogramMin)
      : defaultHistogramMin;
    const clippedMax = useCustomHistogramBounds
      ? (manualHistogramMax ?? defaultHistogramMax)
      : defaultHistogramMax;
    const histogram = computeHistogramBins(numericFixes.map(item => item.value), 24, {
      mode: (histogramMode === "clipped" || useCustomHistogramBounds) ? "clipped" : "full",
      clippedMin,
      clippedMax,
    });
    const activeThresholdValue = thresholdValue === null
      ? null
      : clampThresholdValue(thresholdValue, histogram.min, histogram.max);
    const matchItems = activeThresholdValue === null
      ? []
      : numericFixes.filter(item => (reverse ? item.value < activeThresholdValue : item.value > activeThresholdValue));
    const matchKeys = new Set(matchItems.map(item => item.fix.fixKey));
    const uncheckedMatchKeys = new Set(
      matchItems
        .map(item => item.fix.fixKey)
        .filter(fixKey => !this.data.selectedFixKeys.has(fixKey)),
    );
    return {
      field,
      visibleFixes,
      numericFixes,
      histogram,
      thresholdValue: activeThresholdValue,
      reverse,
      histogramMode: histogram?.mode === "clipped" ? "clipped" : "full",
      histogramInputMin: manualHistogramMin ?? (histogram?.mode === "clipped" ? histogram.min : histogram.observedMin),
      histogramInputMax: manualHistogramMax ?? (histogram?.mode === "clipped" ? histogram.max : histogram.observedMax),
      selectedLevels: [],
      levelOptions: [],
      matchKeys,
      uncheckedMatchKeys,
    };
  }

  renderThresholdPane() {
    const pane = this.refs.thresholdPane;
    if (!pane) {
      return;
    }
    if (!this.data) {
      pane.innerHTML = "";
      pane.classList.add("hidden");
      return;
    }

    const context = this.getThresholdContext();
    const field = context?.field;
    const visibleCount = context?.visibleFixes.length || 0;
    const numericCount = context?.numericFixes.length || 0;
    const thresholdValue = context?.thresholdValue ?? null;
    const reverse = context?.reverse === true;
    const selectedLevels = context?.selectedLevels || [];
    const levelOptions = context?.levelOptions || [];
    const matchCount = context?.matchKeys?.size || 0;
    const uncheckedCount = context?.uncheckedMatchKeys?.size || 0;
    const histogram = context?.histogram;
    const histogramMode = context?.histogramMode === "clipped" ? "clipped" : "full";
    const histogramInputMin = finiteOrNull(context?.histogramInputMin);
    const histogramInputMax = finiteOrNull(context?.histogramInputMax);

    let body = "";
    if (!field) {
      body = `
        <div class="movement-threshold-empty">
          Thresholding is unavailable until a color variable is loaded.
        </div>
      `;
    } else if (!visibleCount) {
      body = `
        <div class="movement-threshold-empty">
          No visible fixes are in scope right now. Adjust the visible individuals or train/test toggles to build a threshold.
        </div>
      `;
    } else if (field.kind !== "numeric") {
      const subtitle = `${formatCount(levelOptions.length)} levels in the visible fixes`;
      const meta = selectedLevels.length
        ? `${formatCount(matchCount)} fixes match ${formatCount(selectedLevels.length)} selected levels.`
        : "Choose one or more levels to highlight matching fixes.";
      const selectionNote = uncheckedCount > 0
        ? `${formatCount(uncheckedCount)} matching fixes are not checked yet.`
        : matchCount > 0
          ? "All fixes from the selected levels are already checked."
          : "No levels are selected yet.";
      body = `
        <div class="movement-threshold-head">
          <div>
            <div class="movement-threshold-title">${escapeHtml(field.label)}</div>
            <div class="movement-threshold-subtitle">${escapeHtml(field.source)} | ${escapeHtml(subtitle)}</div>
          </div>
          <div class="movement-threshold-meta">${escapeHtml(meta)}</div>
        </div>
        <div class="movement-threshold-levels">
          ${levelOptions.map(option => `
            <label class="movement-threshold-level">
              <input
                type="checkbox"
                data-action="toggle-threshold-level"
                data-level="${escapeHtml(option.level)}"
                ${selectedLevels.includes(option.level) ? "checked" : ""}
              >
              <span class="movement-threshold-level-label">${escapeHtml(option.level)}</span>
              <span class="movement-threshold-level-count">${escapeHtml(formatCount(option.count))}</span>
            </label>
          `).join("")}
        </div>
        <div class="movement-threshold-note">${escapeHtml(selectionNote)} Select levels to gather those fixes for review.</div>
        <div class="movement-threshold-actions">
          <button
            type="button"
            class="movement-emphasis"
            data-action="check-above-threshold"
            ${uncheckedCount === 0 ? "disabled" : ""}
          >Check selected levels${uncheckedCount > 0 ? ` (${escapeHtml(formatCount(uncheckedCount))})` : ""}</button>
          <button
            type="button"
            data-action="clear-threshold"
            ${selectedLevels.length === 0 ? "disabled" : ""}
          >Clear selection</button>
        </div>
      `;
    } else if (!numericCount || !histogram) {
      body = `
        <div class="movement-threshold-empty">
          The current numeric field has no usable values in the visible scope.
        </div>
      `;
    } else {
      const bars = histogram.bins
        .map((bin, index) => {
          const height = histogram.maxCount > 0 ? Math.max(6, (bin.count / histogram.maxCount) * 100) : 6;
          const title = `${formatColorValue(bin.start, "numeric")} to ${formatColorValue(bin.end, "numeric")} • ${formatCount(bin.count)} fixes`;
          return `
            <div
              class="movement-threshold-bar"
              style="height:${height.toFixed(2)}%;"
              title="${escapeHtml(title)}"
              data-bin-index="${index}"
            ></div>
          `;
        })
        .join("");
      const thresholdRatio = thresholdValue === null
        ? null
        : histogramValueToRatio(histogram, thresholdValue);
      const thresholdLine = thresholdRatio === null
        ? ""
        : `<div class="movement-threshold-line" style="left:${(thresholdRatio * 100).toFixed(2)}%;"></div>`;
      const subtitle = `${formatCount(numericCount)} visible fixes`;
      const thresholdPrompt = thresholdValue === null
        ? `Click the histogram or type a ${reverse ? "lower-tail" : "upper-tail"} threshold`
        : "Threshold";
      const selectionNote = thresholdValue === null
        ? "No threshold set"
        : matchCount === 0
          ? "No matches"
          : uncheckedCount > 0
            ? `${formatCount(matchCount)} matches • ${formatCount(uncheckedCount)} unchecked`
            : `${formatCount(matchCount)} matches • all checked`;
      body = `
        <div class="movement-threshold-head">
          <div>
            <div class="movement-threshold-title">${escapeHtml(field.label)}</div>
            <div class="movement-threshold-subtitle">${escapeHtml(field.source)} | ${escapeHtml(subtitle)}</div>
          </div>
          <div class="movement-threshold-meta">
            <span>${escapeHtml(thresholdPrompt)}</span>
            <span>${reverse ? "&lt;" : "&gt;"}</span>
            <input
              class="movement-threshold-inline-input"
              type="number"
              step="any"
              data-action="set-threshold-value"
              placeholder="value"
              value="${thresholdValue === null ? "" : escapeHtml(String(thresholdValue))}"
            >
          </div>
        </div>
        <div class="movement-threshold-zoom">
          <button
            type="button"
            data-action="set-histogram-mode"
            data-mode="clipped"
            class="${histogramMode === "clipped" ? "is-active" : ""}"
          >Zoom in</button>
          <button
            type="button"
            data-action="set-histogram-mode"
            data-mode="full"
            class="${histogramMode === "full" ? "is-active" : ""}"
          >Zoom out</button>
        </div>
        <div class="movement-threshold-chart-wrap">
          <div
            class="movement-threshold-chart"
            data-role="threshold-chart"
            data-field-key="${escapeHtml(field.key)}"
            data-min="${escapeHtml(String(histogram.min))}"
            data-max="${escapeHtml(String(histogram.max))}"
          >
            ${bars}
            ${thresholdLine}
          </div>
        </div>
        <div class="movement-threshold-range">
          <label class="movement-threshold-range-label">
            <span>Min</span>
            <input
              class="movement-threshold-range-input"
              type="number"
              step="any"
              data-action="set-histogram-min"
              value="${histogramInputMin === null ? "" : escapeHtml(String(histogramInputMin))}"
            >
          </label>
          <label class="movement-threshold-range-label">
            <span>Max</span>
            <input
              class="movement-threshold-range-input"
              type="number"
              step="any"
              data-action="set-histogram-max"
              value="${histogramInputMax === null ? "" : escapeHtml(String(histogramInputMax))}"
            >
          </label>
        </div>
        <label class="movement-threshold-toggle">
          <input
            type="checkbox"
            data-action="toggle-threshold-reverse"
            ${reverse ? "checked" : ""}
          >
          Reverse threshold: highlight values below the line
        </label>
        <div class="movement-threshold-note">${escapeHtml(selectionNote)}</div>
        <div class="movement-threshold-actions">
          <button
            type="button"
            class="movement-emphasis"
            data-action="check-above-threshold"
            ${uncheckedCount === 0 ? "disabled" : ""}
          >Check ${reverse ? "below" : "above"} threshold</button>
          <button
            type="button"
            data-action="clear-threshold"
            ${thresholdValue === null ? "disabled" : ""}
          >Clear threshold</button>
          <button
            type="button"
            data-action="reset-histogram-limits"
            ${(this.thresholdState.fieldKey !== field.key || (this.thresholdState.histogramMin === null && this.thresholdState.histogramMax === null && histogramMode === "full")) ? "disabled" : ""}
          >Reset limits</button>
        </div>
      `;
    }

    pane.innerHTML = body;
    pane.classList.remove("hidden");
  }

  handleThresholdPaneFocusIn(event) {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) {
      return;
    }
    if (target.closest('input[data-action="set-histogram-min"], input[data-action="set-histogram-max"], input[data-action="set-threshold-value"]')) {
      this.thresholdInputPendingBlur = true;
    }
  }

  handleThresholdPaneClick(event) {
    if (!this.data) {
      return;
    }
    const target = event.target instanceof Element ? event.target : null;
    if (!target) {
      return;
    }
    const actionButton = target.closest("button[data-action]");
    if (actionButton) {
      const action = actionButton.dataset.action || "";
      if (action === "set-histogram-mode") {
        const field = this.getCurrentColorField();
        const mode = actionButton.dataset.mode === "clipped" ? "clipped" : "full";
        const colorRange = this.data?.colorStyles.get(field?.key || "")?.range || null;
        this.thresholdState = {
          fieldKey: field?.key || this.thresholdState.fieldKey || "",
          value: this.thresholdState.value,
          reverse: this.thresholdState.reverse === true,
          selectedLevels: [],
          histogramMode: mode,
          histogramMin: mode === "clipped" ? (Number.isFinite(colorRange?.min) ? colorRange.min : null) : null,
          histogramMax: mode === "clipped" ? (Number.isFinite(colorRange?.max) ? colorRange.max : null) : null,
        };
        this.renderThresholdPane();
        this.renderLayers();
      } else if (action === "clear-threshold") {
        const field = this.getCurrentColorField();
        this.thresholdState = {
          fieldKey: field?.key || "",
          value: null,
          reverse: false,
          selectedLevels: [],
          histogramMode: this.thresholdState.histogramMode === "clipped" ? "clipped" : "full",
          histogramMin: this.thresholdState.histogramMin,
          histogramMax: this.thresholdState.histogramMax,
        };
        this.renderThresholdPane();
        this.renderLayers();
      } else if (action === "reset-histogram-limits") {
        const field = this.getCurrentColorField();
        this.thresholdState = {
          fieldKey: field?.key || this.thresholdState.fieldKey || "",
          value: this.thresholdState.value,
          reverse: this.thresholdState.reverse === true,
          selectedLevels: [],
          histogramMode: "full",
          histogramMin: null,
          histogramMax: null,
        };
        this.renderThresholdPane();
        this.renderLayers();
      } else if (action === "check-above-threshold") {
        this.checkAboveThresholdSelection();
      }
      return;
    }

    const chart = target.closest('[data-role="threshold-chart"]');
    if (chart && this.thresholdInputPendingBlur) {
      this.thresholdInputPendingBlur = false;
      return;
    }
    this.thresholdInputPendingBlur = false;
    if (!chart) {
      return;
    }
    const fieldKey = chart.dataset.fieldKey || "";
    const min = Number(chart.dataset.min);
    const max = Number(chart.dataset.max);
    if (!fieldKey || !Number.isFinite(min) || !Number.isFinite(max)) {
      return;
    }
    const rect = chart.getBoundingClientRect();
    if (!rect.width) {
      return;
    }
    const ratio = Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width));
    const context = this.getThresholdContext();
    const histogram = context?.histogram;
    const thresholdValue = histogram
      ? histogramRatioToValue(histogram, ratio)
      : (min === max ? min : min + ((max - min) * ratio));
    this.thresholdState = {
      fieldKey,
      value: thresholdValue,
      reverse: this.thresholdState.fieldKey === fieldKey && this.thresholdState.reverse === true,
      selectedLevels: [],
      histogramMode: this.thresholdState.fieldKey === fieldKey && this.thresholdState.histogramMode === "clipped" ? "clipped" : "full",
      histogramMin: this.thresholdState.fieldKey === fieldKey ? this.thresholdState.histogramMin : null,
      histogramMax: this.thresholdState.fieldKey === fieldKey ? this.thresholdState.histogramMax : null,
    };
    this.renderThresholdPane();
    this.renderLayers();
  }

  handleThresholdPaneChange(event) {
    if (!this.data) {
      return;
    }
    const target = event.target instanceof Element ? event.target : null;
    if (!target) {
      return;
    }
    const numericInput = target.closest('input[data-action="set-histogram-min"], input[data-action="set-histogram-max"], input[data-action="set-threshold-value"]');
    if (numericInput) {
      this.thresholdInputPendingBlur = false;
      const field = this.getCurrentColorField();
      if (!field || field.kind !== "numeric") {
        return;
      }
      const action = numericInput.dataset.action || "";
      const raw = String(numericInput.value || "").trim();
      const parsedValue = raw === "" ? null : finiteOrNull(raw);
      const context = this.getThresholdContext();
      const fallbackMin = Number.isFinite(context?.histogram?.observedMin) ? context.histogram.observedMin : null;
      const fallbackMax = Number.isFinite(context?.histogram?.observedMax) ? context.histogram.observedMax : null;
      let nextHistogramMin = this.thresholdState.fieldKey === field.key ? this.thresholdState.histogramMin : null;
      let nextHistogramMax = this.thresholdState.fieldKey === field.key ? this.thresholdState.histogramMax : null;
      let nextThresholdValue = this.thresholdState.fieldKey === field.key ? this.thresholdState.value : null;
      if (raw !== "" && parsedValue === null) {
        this.setStatus("Threshold inputs must be valid numbers.", true);
        this.renderThresholdPane();
        return;
      }
      if (action === "set-histogram-min") {
        nextHistogramMin = parsedValue;
      } else if (action === "set-histogram-max") {
        nextHistogramMax = parsedValue;
      } else if (action === "set-threshold-value") {
        nextThresholdValue = parsedValue;
      }
      const effectiveMin = nextHistogramMin ?? fallbackMin;
      const effectiveMax = nextHistogramMax ?? fallbackMax;
      if (
        Number.isFinite(effectiveMin)
        && Number.isFinite(effectiveMax)
        && effectiveMin >= effectiveMax
      ) {
        this.setStatus("Histogram min must be smaller than histogram max.", true);
        this.renderThresholdPane();
        return;
      }
      if (action === "set-threshold-value" && parsedValue !== null && Number.isFinite(effectiveMin) && Number.isFinite(effectiveMax)) {
        nextThresholdValue = clampThresholdValue(parsedValue, effectiveMin, effectiveMax);
      }
      const hasCustomBounds = nextHistogramMin !== null || nextHistogramMax !== null;
      this.thresholdState = {
        fieldKey: field.key,
        value: nextThresholdValue,
        reverse: this.thresholdState.fieldKey === field.key && this.thresholdState.reverse === true,
        selectedLevels: [],
        histogramMode: hasCustomBounds ? "clipped" : (this.thresholdState.histogramMode === "clipped" ? "clipped" : "full"),
        histogramMin: nextHistogramMin,
        histogramMax: nextHistogramMax,
      };
      this.renderThresholdPane();
      this.renderLayers();
      return;
    }
    const checkbox = target.closest('input[data-action="toggle-threshold-reverse"]');
    if (checkbox) {
      const field = this.getCurrentColorField();
      this.thresholdState = {
        fieldKey: field?.key || this.thresholdState.fieldKey || "",
        value: this.thresholdState.value,
        reverse: checkbox.checked,
        selectedLevels: [],
        histogramMode: this.thresholdState.histogramMode === "clipped" ? "clipped" : "full",
        histogramMin: this.thresholdState.histogramMin,
        histogramMax: this.thresholdState.histogramMax,
      };
      this.renderThresholdPane();
      this.renderLayers();
      return;
    }
    const levelInput = target.closest('input[data-action="toggle-threshold-level"]');
    if (!levelInput) {
      return;
    }
    const field = this.getCurrentColorField();
    const currentLevels = this.thresholdState.fieldKey === (field?.key || "") && Array.isArray(this.thresholdState.selectedLevels)
      ? new Set(uniqueNonEmpty(this.thresholdState.selectedLevels))
      : new Set();
    const level = String(levelInput.dataset.level || "");
    if (levelInput.checked) {
      currentLevels.add(level);
    } else {
      currentLevels.delete(level);
    }
    this.thresholdState = {
      fieldKey: field?.key || this.thresholdState.fieldKey || "",
      value: null,
      reverse: false,
      selectedLevels: [...currentLevels],
      histogramMode: this.thresholdState.histogramMode === "clipped" ? "clipped" : "full",
      histogramMin: this.thresholdState.histogramMin,
      histogramMax: this.thresholdState.histogramMax,
    };
    this.renderThresholdPane();
    this.renderLayers();
  }

  checkAboveThresholdSelection() {
    if (!this.data) {
      return;
    }
    const context = this.getThresholdContext();
    if (!context?.uncheckedMatchKeys?.size) {
      return;
    }
    const nextSelected = new Set(this.data.selectedFixKeys);
    for (const fixKey of context.uncheckedMatchKeys) {
      nextSelected.add(fixKey);
    }
    this.data.selectedFixKeys = nextSelected;
    this.renderSelectedFixes();
    this.renderThresholdPane();
    this.renderLayers();
    this.updateActionButtons();
  }

  getFixesForIndividualsFrom(items, individuals) {
    const visibleIndividuals = new Set(individuals);
    return (Array.isArray(items) ? items : []).filter(fix => visibleIndividuals.has(fix.individual));
  }

  filterSelectedFixKeysForIndividuals(fixKeys, individuals) {
    if (!this.data) {
      return new Set();
    }
    const visibleIndividuals = new Set(individuals);
    return new Set(
      [...fixKeys].filter(key => {
        const fix = this.data.fixByKey.get(key);
        return Boolean(fix && visibleIndividuals.has(fix.individual));
      }),
    );
  }

  getFixesForScope(scope, { allowPartialFull = false } = {}) {
    if (!this.data) {
      return [];
    }
    if (scope === "full") {
      if (this.data.reportAllState === "loaded") {
        return this.data.reportAllFixes;
      }
      return allowPartialFull ? this.data.overviewFixes : [];
    }
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    return this.data.fixes.filter(fix => visibleIndividuals.has(fix.individual));
  }

  getSuspiciousFixes(individual = "", { scope = "visible", allowPartialFull = false } = {}) {
    return this.getFixesForScope(scope, { allowPartialFull })
      .filter(fix => fix.review.status === "suspected" && (!individual || fix.individual === individual))
      .sort((left, right) => left.individual.localeCompare(right.individual) || left.timeMs - right.timeMs);
  }

  hasLoadedDetailSelection() {
    if (!this.data || this.data.detailState !== "loaded") {
      return false;
    }
    return arraysEqual(this.getSelectedIndividuals(), this.data.detailIndividuals);
  }

  async loadDetailForCurrentSelection({ preservedFixKeys } = {}) {
    if (!this.data || !this.currentArtifact) {
      return;
    }
    const selectedIndividuals = this.getSelectedIndividuals();
    const preserved = preservedFixKeys instanceof Set ? preservedFixKeys : new Set(this.data.selectedFixKeys);
    if (!selectedIndividuals.length) {
      this.cancelRequest("detail");
      this.data.detailState = "idle";
      this.data.detailIndividuals = [];
      this.data.detailLimit = null;
      this.data.detailMatchingFixCount = 0;
      this.data.detailReturnedFixCount = 0;
      this.data.detailTruncated = false;
      this.data.detailFixes = [];
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set();
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      return;
    }

    if (this.data.detailState === "loaded" && arraysEqual(this.data.detailIndividuals, selectedIndividuals)) {
      this.updateActionButtons();
      return;
    }

    if (this.data.overviewHasAllFixes) {
      this.cancelRequest("detail");
      this.data.detailState = "loaded";
      this.data.detailIndividuals = [...selectedIndividuals];
      this.data.detailLimit = this.data.totalRows;
      this.data.detailFixes = [];
      this.data.detailMatchingFixCount = this.getFixesForIndividualsFrom(this.data.overviewFixes, selectedIndividuals).length;
      this.data.detailReturnedFixCount = this.data.detailMatchingFixCount;
      this.data.detailTruncated = false;
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(preserved, this.data.detailIndividuals);
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      this.setStatus(`Loaded ${formatCount(this.data.detailReturnedFixCount)} editable fixes for ${formatCount(this.data.detailIndividuals.length)} visible individuals.`);
      return;
    }

    this.cancelRequest("detail");
    this.data.detailState = "loading";
    this.data.detailIndividuals = [...selectedIndividuals];
    this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(preserved, selectedIndividuals);
    this.renderSelectedFixes();
    this.renderThresholdPane();
    this.updateActionButtons();
    this.setStatus(`Loaded overview for ${this.currentArtifact}. Loading editable fixes for ${formatCount(selectedIndividuals.length)} visible individuals...`);

    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    const requestId = ++this.loadRequestId;
    try {
      const controller = this.beginRequest("detail");
      const payload = await this.fetchJSON(
        this.buildFixesRequestUrl({ familyName, studyName, datasetId, artifactName, individuals: selectedIndividuals }),
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
      this.data.detailIndividuals = Array.isArray(payload.detail_scope?.individuals)
        ? payload.detail_scope.individuals.map(value => String(value))
        : [...selectedIndividuals];
      this.data.detailLimit = payload.detail_scope?.limit ?? null;
      this.data.detailMatchingFixCount = Number(payload.matching_fix_count) || 0;
      this.data.detailReturnedFixCount = Number(payload.returned_fix_count) || 0;
      this.data.detailTruncated = Boolean(payload.truncated);
      this.data.detailFixes = parseMovementFixes(payload.fixes || []);
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(preserved, this.data.detailIndividuals);
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      if (this.data.detailTruncated) {
        this.setStatus(`Loaded ${formatCount(this.data.detailReturnedFixCount)} of ${formatCount(this.data.detailMatchingFixCount)} editable fixes for ${formatCount(this.data.detailIndividuals.length)} visible individuals due to the ${formatCount(this.data.detailLimit)}-fix cap.`, true);
      } else {
        this.setStatus(`Loaded ${formatCount(this.data.detailReturnedFixCount || this.data.detailFixes.length)} editable fixes for ${formatCount(this.data.detailIndividuals.length)} visible individuals.`);
      }
    } catch (error) {
      if (this.isAbortError(error) || requestId !== this.loadRequestId || !this.data) {
        return;
      }
      this.data.detailState = "error";
      this.data.detailLimit = null;
      this.data.detailMatchingFixCount = 0;
      this.data.detailReturnedFixCount = 0;
      this.data.detailTruncated = false;
      this.data.detailFixes = [];
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set();
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      this.setStatus(`Overview loaded, but editable fixes for ${formatCount(selectedIndividuals.length)} visible individuals failed: ${error.message}`, true);
    }
  }

  buildFixesRequestUrl({ familyName, studyName, datasetId, artifactName, individuals = [], reviewStatus = "", limit } = {}) {
    const params = new URLSearchParams({ logical_name: artifactName });
    const normalizedIndividuals = uniqueNonEmpty(individuals).sort((left, right) => left.localeCompare(right));
    const allIndividuals = this.data ? [...this.data.individuals].sort((left, right) => left.localeCompare(right)) : [];
    const shouldOmitIndividuals = normalizedIndividuals.length > 0 && arraysEqual(normalizedIndividuals, allIndividuals);
    if (!shouldOmitIndividuals) {
      for (const individual of normalizedIndividuals) {
        params.append("individuals", individual);
      }
    }
    if (reviewStatus) {
      params.set("review_status", reviewStatus);
    }
    if (limit !== undefined && limit !== null) {
      params.set("limit", String(limit));
    }
    return `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/dataset/${encodeURIComponent(datasetId)}/fixes?${params.toString()}`;
  }

  async handleReportScopeChange() {
    if (!this.data) {
      return;
    }
    if (this.getReportType() === "issue_first" && this.refs.reportScope.value === "full") {
      await this.ensureFullReportDataLoaded();
    }
    this.populateReportIndividualOptions();
    this.updateReportModeUi();
    this.renderReportSelection();
  }

  async ensureFullReportDataLoaded() {
    if (!this.data || !this.currentArtifact) {
      return;
    }
    if (this.data.overviewHasAllFixes) {
      this.data.reportAllState = "loaded";
      this.data.reportAllFixes = [...this.data.overviewFixes];
      this.data.reportAllLimit = this.data.totalRows;
      this.data.reportAllMatchingFixCount = this.data.reportAllFixes.length;
      this.data.reportAllReturnedFixCount = this.data.reportAllFixes.length;
      this.data.reportAllTruncated = false;
      return;
    }
    if (this.data.reportAllState === "loaded" || this.data.reportAllState === "loading") {
      return;
    }

    this.cancelRequest("reportDetail");
    this.data.reportAllState = "loading";
    this.data.reportAllFixes = [];
    this.data.reportAllLimit = null;
    this.data.reportAllMatchingFixCount = 0;
    this.data.reportAllReturnedFixCount = 0;
    this.data.reportAllTruncated = false;
    this.renderReportSelection();

    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    const requestId = ++this.reportDetailLoadId;
    try {
      const controller = this.beginRequest("reportDetail");
      const payload = await this.fetchJSON(
        this.buildFixesRequestUrl({ familyName, studyName, datasetId, artifactName }),
        { signal: controller.signal },
      );
      if (
        requestId !== this.reportDetailLoadId
        || this.requestControllers.reportDetail !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
        || !this.data
      ) {
        return;
      }
      this.data.reportAllState = "loaded";
      this.data.reportAllFixes = parseMovementFixes(payload.fixes || []);
      this.data.reportAllLimit = payload.detail_scope?.limit ?? null;
      this.data.reportAllMatchingFixCount = Number(payload.matching_fix_count) || 0;
      this.data.reportAllReturnedFixCount = Number(payload.returned_fix_count) || 0;
      this.data.reportAllTruncated = Boolean(payload.truncated);
    } catch (error) {
      if (this.isAbortError(error) || requestId !== this.reportDetailLoadId || !this.data) {
        return;
      }
      this.data.reportAllState = "error";
      this.data.reportAllFixes = [];
      this.data.reportAllLimit = null;
      this.data.reportAllMatchingFixCount = 0;
      this.data.reportAllReturnedFixCount = 0;
      this.data.reportAllTruncated = false;
    }

    if (this.refs.reportModal && !this.refs.reportModal.classList.contains("hidden")) {
      this.populateReportIndividualOptions();
      this.renderReportSelection();
    }
  }

  populateReportIndividualOptions() {
    const reportType = this.getReportType();
    const scope = this.refs.reportScope.value || "visible";
    const currentValue = this.refs.reportIndividual.value;
    this.refs.reportIndividual.innerHTML = "";

    const allOption = document.createElement("option");
    allOption.value = "";
    if (reportType === "issue_first") {
      const suspiciousFixes = this.getSuspiciousFixes("", {
        scope,
        allowPartialFull: scope === "full",
      });
      const individuals = uniqueStrings(suspiciousFixes.map(fix => fix.individual))
        .sort((left, right) => left.localeCompare(right));
      allOption.textContent = `All individuals (${formatCount(suspiciousFixes.length)} fixes)`;
      this.refs.reportIndividual.appendChild(allOption);
      for (const individual of individuals) {
        const option = document.createElement("option");
        option.value = individual;
        option.textContent = `${individual} (${formatCount(this.getSuspiciousFixes(individual, {
          scope,
          allowPartialFull: scope === "full",
        }).length)} fixes)`;
        this.refs.reportIndividual.appendChild(option);
      }
      this.refs.reportIndividual.value = individuals.includes(currentValue) ? currentValue : "";
      return;
    }

    const individuals = this.getReportScopeIndividuals(scope);
    allOption.textContent = `All individuals in scope (${formatCount(individuals.length)})`;
    this.refs.reportIndividual.appendChild(allOption);
    for (const individual of individuals) {
      const option = document.createElement("option");
      option.value = individual;
      option.textContent = individual;
      this.refs.reportIndividual.appendChild(option);
    }
    this.refs.reportIndividual.value = individuals.includes(currentValue) ? currentValue : "";
  }

  getReportType() {
    return this.refs.reportType.value || "issue_first";
  }

  getReportScopeIndividuals(scope = this.refs.reportScope.value || "visible") {
    if (!this.data) {
      return [];
    }
    if (scope === "full") {
      return [...(this.data.individuals || [])].sort((left, right) => left.localeCompare(right));
    }
    return this.getSelectedIndividuals();
  }

  getReportIndividuals() {
    const individuals = this.getReportScopeIndividuals();
    const selectedIndividual = this.refs.reportIndividual.value || "";
    if (selectedIndividual) {
      return individuals.includes(selectedIndividual) ? [selectedIndividual] : [];
    }
    return individuals;
  }

  getReportOutputMode(reportIndividuals = this.getReportIndividuals()) {
    if (reportIndividuals.length <= 1) {
      return "combined";
    }
    return this.refs.reportOutputMode.value || "combined";
  }

  getReportFixes() {
    const scope = this.refs.reportScope.value || "visible";
    if (scope === "full" && this.data?.reportAllState !== "loaded") {
      return [];
    }
    return this.getSuspiciousFixes(this.refs.reportIndividual.value, { scope });
  }

  getReportSnapshotWindows() {
    return this.buildReportSnapshotWindows(this.getReportFixes());
  }

  buildIndividualProfileSnapshotWindows() {
    const reportIndividuals = this.getReportIndividuals();
    if (!reportIndividuals.length) {
      return [];
    }
    const scope = this.refs.reportScope.value || "visible";
    const fixes = this.getFixesForScope(scope, { allowPartialFull: true });
    return reportIndividuals
      .map(individual => {
        const windowFixes = fixes
          .filter(fix => fix.individual === individual)
          .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
        if (!windowFixes.length) {
          return null;
        }
        const suspectedFixes = windowFixes
          .filter(fix => fix.review?.status === "suspected")
          .map(fix => fix.fixKey);
        const confirmedFixes = windowFixes
          .filter(fix => fix.review?.status === "confirmed")
          .map(fix => fix.fixKey);
        return {
          snapshotKey: `individual_profile::${individual}`,
          caption: `${individual} whole track`,
          individual,
          setName: "all",
          issueType: "",
          issueTypes: [],
          anchorFixKeys: suspectedFixes,
          secondaryFixKeys: confirmedFixes,
          reportFixKeys: [],
          startFixKey: windowFixes[0]?.fixKey || "",
          endFixKey: windowFixes[windowFixes.length - 1]?.fixKey || "",
          startTimeMs: windowFixes[0]?.timeMs || 0,
          endTimeMs: windowFixes[windowFixes.length - 1]?.timeMs || 0,
          startTimeText: formatTimestamp(windowFixes[0]?.timeMs || 0),
          endTimeText: formatTimestamp(windowFixes[windowFixes.length - 1]?.timeMs || 0),
          windowFixCount: windowFixes.length,
          windowFixes,
          reportWindowFixes: [],
          showGrid: true,
          snapshotKind: "individual_profile",
        };
      })
      .filter(Boolean);
  }

  getReviewedFixesForIndividuals(individuals, { scope = this.refs.reportScope.value || "visible", allowPartialFull = true } = {}) {
    const visibleIndividuals = new Set(individuals);
    return this.getFixesForScope(scope, { allowPartialFull })
      .filter(fix => visibleIndividuals.has(fix.individual) && Boolean(fix.review.status));
  }

  getRequestedReportSnapshotLimit() {
    const raw = String(this.refs.reportSnapshotLimit.value || "").trim();
    if (!raw) {
      return null;
    }
    const value = Math.floor(Number(raw));
    return Number.isFinite(value) && value > 0 ? value : null;
  }

  getEffectiveReportSnapshotWindows() {
    if (this.getReportType() !== "issue_first") {
      return [];
    }
    const snapshotWindows = this.getReportSnapshotWindows();
    const screenshotMode = this.refs.reportScreenshotMode.value || "manual";
    if (screenshotMode !== "auto") {
      return snapshotWindows;
    }
    const limit = this.getRequestedReportSnapshotLimit();
    return sampleReportSnapshotWindows(snapshotWindows, limit, {
      spreadIndividuals: this.refs.reportSpreadIndividuals.checked,
    });
  }

  getSelectedReportBasemapStyle() {
    const choice = this.refs.reportBasemap.value || "current";
    if (choice === "current") {
      const currentName = this.refs.basemap.value || "Blank";
      if (currentName !== "Blank" && BASEMAP_STYLES[currentName]) {
        return BASEMAP_STYLES[currentName];
      }
      return BASEMAP_STYLES.Positron;
    }
    return BASEMAP_STYLES[choice] || BASEMAP_STYLES.Positron;
  }

  updateReportModeUi() {
    const reportType = this.getReportType();
    const reportIndividuals = this.getReportIndividuals();
    const showIssueFirstControls = reportType === "issue_first";
    this.refs.reportOutputModeWrap.hidden = reportType !== "individual_profile" || reportIndividuals.length <= 1;
    this.refs.reportScreenshotModeWrap.hidden = !showIssueFirstControls;
    this.refs.reportBasemapWrap.hidden = false;
    this.refs.reportSnapshotLimitWrap.hidden = !showIssueFirstControls;
    this.refs.reportSpreadIndividualsWrap.hidden = !showIssueFirstControls;
  }

  async handleReportTypeChange() {
    if (this.getReportType() === "issue_first" && this.refs.reportScope.value === "full") {
      await this.ensureFullReportDataLoaded();
    }
    this.populateReportIndividualOptions();
    this.updateReportModeUi();
    this.renderReportSelection();
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
        issue_field: fix.review.issueField || "",
        issue_threshold: fix.review.issueThreshold || "",
        issues: (fix.review.issues || []).map(issue => ({
          status: issue.status || "",
          issue_id: issue.issueId || "",
          issue_type: issue.issueType || "",
          issue_field: issue.issueField || "",
          issue_threshold: issue.issueThreshold || "",
          issue_note: issue.issueNote || "",
          owner_question: issue.ownerQuestion || "",
          review_user: issue.reviewUser || "",
          reviewed_at: issue.reviewedAt || "",
        })),
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
    const currentField = this.getCurrentColorField();
    const thresholdFieldKey = this.thresholdState.fieldKey || "";
    const thresholdValue = typeof this.thresholdState.value === "number" ? this.thresholdState.value : null;
    const thresholdReverse = this.thresholdState.reverse === true;
    const scope = this.refs.reportScope.value || "visible";
    const trackFixesSource = this.getFixesForScope(scope, { allowPartialFull: scope === "full" });
    const trackFixesByKey = new Map();
    const indexByFixKey = new Map();
    for (const fix of trackFixesSource) {
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

    const windows = [];
    for (const window of merged) {
      const trackFixes = trackFixesByKey.get(window.trackKey) || [];
      const windowFixes = trackFixes.slice(window.startIndex, window.endIndex + 1);
      const reportFixKeys = windowFixes
        .filter(fix => reportFixMap.has(fix.fixKey))
        .map(fix => fix.fixKey);
      const reportWindowFixes = reportFixKeys
        .map(fixKey => reportFixMap.get(fixKey))
        .filter(Boolean)
        .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
      const firstWindowFix = windowFixes[0];
      const lastWindowFix = windowFixes[windowFixes.length - 1];
      const issueTypes = uniqueNonEmpty(reportWindowFixes.flatMap(fix => reportIssueTypes(fix))).sort((left, right) => left.localeCompare(right));
      for (const issueType of issueTypes) {
        const focalFixes = reportWindowFixes
          .filter(fix => reportIssueTypes(fix).includes(issueType))
          .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
        if (!focalFixes.length) {
          continue;
        }
        const focalFixKeys = focalFixes.map(fix => fix.fixKey);
        const secondaryFixKeys = reportWindowFixes
          .filter(fix => !reportIssueTypes(fix).includes(issueType))
          .map(fix => fix.fixKey)
          .sort((left, right) => left.localeCompare(right));
        const sampleValue = deriveReportSampleValue(
          focalFixes,
          currentField,
          this.data.colorFieldByKey,
          {
            thresholdFieldKey,
            thresholdValue,
            thresholdReverse,
          },
        );
        windows.push({
          snapshotKey: "",
          caption: `${issueType} | ${window.individual} | ${formatTimestamp(firstWindowFix?.timeMs)} to ${formatTimestamp(lastWindowFix?.timeMs)}`,
          individual: window.individual,
          setName: window.setName,
          issueType,
          issueTypes: [issueType],
          anchorFixKeys: focalFixKeys,
          secondaryFixKeys,
          reportFixKeys: focalFixKeys,
          startFixKey: firstWindowFix?.fixKey || "",
          endFixKey: lastWindowFix?.fixKey || "",
          startTimeMs: firstWindowFix?.timeMs || 0,
          endTimeMs: lastWindowFix?.timeMs || 0,
          startTimeText: formatTimestamp(firstWindowFix?.timeMs || 0),
          endTimeText: formatTimestamp(lastWindowFix?.timeMs || 0),
          windowFixCount: windowFixes.length,
          windowFixes,
          reportWindowFixes: focalFixes,
          sampleValue,
        });
      }
    }
    return windows.map((window, index) => ({
      ...window,
      snapshotKey: `snapshot_${String(index + 1).padStart(2, "0")}`,
    }));
  }

  renderReportSelection() {
    const reportType = this.getReportType();
    const scope = this.refs.reportScope.value || "visible";
    const scopeLabel = scope === "full" ? "Full study" : "Visible individuals";
    const individualLabel = this.refs.reportIndividual.value || "All individuals";
    this.updateReportModeUi();
    if (reportType === "issue_first" && scope === "full" && this.data?.reportAllState === "loading") {
      this.refs.reportMeta.innerHTML = `
        <div><strong>Report type:</strong> Issue-first debug report</div>
        <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
        <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
        <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
        <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
        <div><strong>Scope:</strong> ${escapeHtml(scopeLabel)}</div>
        <div><strong>Individual:</strong> ${escapeHtml(individualLabel)}</div>
        <div><strong>Report state:</strong> Loading full-study report context...</div>
      `;
      this.refs.reportSubmit.disabled = true;
      return;
    }
    if (reportType === "issue_first" && scope === "full" && this.data?.reportAllState === "error") {
      this.refs.reportMeta.innerHTML = `
        <div><strong>Report type:</strong> Issue-first debug report</div>
        <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
        <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
        <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
        <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
        <div><strong>Scope:</strong> ${escapeHtml(scopeLabel)}</div>
        <div><strong>Individual:</strong> ${escapeHtml(individualLabel)}</div>
        <div><strong>Report state:</strong> Could not load full-study report context.</div>
      `;
      this.refs.reportSubmit.disabled = true;
      return;
    }

    if (reportType === "individual_profile") {
      const reportIndividuals = this.getReportIndividuals();
      const reviewedFixes = this.getReviewedFixesForIndividuals(reportIndividuals, { scope });
      const outputMode = this.getReportOutputMode(reportIndividuals);
      const scopeNote = scope === "full"
        ? `Using all ${formatCount(this.getReportScopeIndividuals("full").length)} individuals in the study.`
        : `Using the ${formatCount(this.getSelectedIndividuals().length)} currently visible individuals.`;
      this.refs.reportMeta.innerHTML = `
        <div><strong>Report type:</strong> Per-individual profile report</div>
        <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
        <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
        <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
        <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
        <div><strong>Scope:</strong> ${escapeHtml(scopeLabel)}</div>
        <div><strong>Individual:</strong> ${escapeHtml(individualLabel)}</div>
        <div><strong>Individuals in report:</strong> ${escapeHtml(formatCount(reportIndividuals.length))}</div>
        <div><strong>Output mode:</strong> ${escapeHtml(outputMode === "combined" ? "Single combined report" : "Separate files + index")}</div>
        <div><strong>Reviewed fixes in scope:</strong> ${escapeHtml(formatCount(reviewedFixes.length))}</div>
        <div><strong>Scope note:</strong> ${escapeHtml(scopeNote)}</div>
      `;
      this.refs.reportSubmit.disabled = reportIndividuals.length === 0;
      return;
    }

    const reportFixes = this.getReportFixes();
    const snapshotWindows = this.getReportSnapshotWindows();
    const effectiveSnapshotWindows = this.getEffectiveReportSnapshotWindows();
    const screenshotMode = this.refs.reportScreenshotMode.value || "manual";
    const samplingLimit = this.getRequestedReportSnapshotLimit();
    const issueTypes = uniqueNonEmpty(reportFixes.flatMap(fix => reportIssueTypes(fix)));
    const reportBasemapLabel = this.refs.reportBasemap.value === "current"
      ? ((this.refs.basemap.value && this.refs.basemap.value !== "Blank") ? `${this.refs.basemap.value} (current)` : "Positron (fallback from Blank)")
      : (this.refs.reportBasemap.value || "Positron");
    const scopeNote = scope === "full" && this.data?.reportAllTruncated
      ? `Loaded ${formatCount(this.data.reportAllReturnedFixCount)} of ${formatCount(this.data.reportAllMatchingFixCount)} fixes for full-study report context due to the ${formatCount(this.data.reportAllLimit)}-fix cap.`
      : scope === "full"
        ? `Loaded ${formatCount(this.data?.reportAllReturnedFixCount || reportFixes.length)} fixes for full-study report context.`
        : `Using the ${formatCount(this.getSelectedIndividuals().length)} currently visible individuals.`;
    const snapshotStrategy = this.refs.reportSpreadIndividuals.checked
      ? "ensuring issue coverage first, then spreading remaining examples across individuals"
      : "ensuring issue coverage first, then sampling the remaining windows across the full list";
    const snapshotNote = screenshotMode === "auto" && samplingLimit && effectiveSnapshotWindows.length < snapshotWindows.length
      ? `Auto snapshots will sample ${formatCount(effectiveSnapshotWindows.length)} of ${formatCount(snapshotWindows.length)} snapshot windows, ${snapshotStrategy}.`
      : screenshotMode === "auto"
        ? `Auto snapshots will render ${formatCount(effectiveSnapshotWindows.length)} snapshot windows using ${reportBasemapLabel}, ${snapshotStrategy}.`
        : `Manual screenshot mode keeps ${formatCount(snapshotWindows.length)} snapshot windows without auto-rendering images.`;
    this.refs.reportMeta.innerHTML = `
      <div><strong>Report type:</strong> Issue-first debug report</div>
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Scope:</strong> ${escapeHtml(scopeLabel)}</div>
      <div><strong>Individual:</strong> ${escapeHtml(individualLabel)}</div>
      <div><strong>Snapshot basemap:</strong> ${escapeHtml(reportBasemapLabel)}</div>
      <div><strong>Suspected fixes in report:</strong> ${escapeHtml(formatCount(reportFixes.length))}</div>
      <div><strong>Snapshot windows:</strong> ${escapeHtml(formatCount(snapshotWindows.length))}</div>
      <div><strong>Auto snapshots to render:</strong> ${escapeHtml(formatCount(effectiveSnapshotWindows.length))}</div>
      <div><strong>Snapshot plan:</strong> ${escapeHtml(snapshotNote)}</div>
      <div><strong>Issue types in report:</strong> ${escapeHtml(issueTypes.length ? issueTypes.join(", ") : "Unspecified issue")}</div>
      <div><strong>Scope note:</strong> ${escapeHtml(scopeNote)}</div>
    `;
    this.refs.reportSubmit.disabled = reportFixes.length === 0;
  }

  updateActionButtons() {
    const hasData = Boolean(this.data);
    const hasSelectedIndividuals = this.getSelectedIndividuals().length > 0;
    const hasDetail = this.hasLoadedDetailSelection();
    const selectedCount = this.getSelectedFixes().length;
    const visibleSuspiciousCount = this.getSuspiciousFixes("", { scope: "visible" }).length;
    for (const button of [
      this.refs.selectSuspicious,
      this.refs.clearFixes,
      this.refs.markSuspected,
      this.refs.markConfirmed,
      this.refs.generateReport,
      this.refs.removeConfirmed,
    ]) {
      button.hidden = !hasData;
    }
    this.refs.markSuspected.disabled = !hasData || !hasSelectedIndividuals || !hasDetail || selectedCount === 0;
    this.refs.markConfirmed.disabled = !hasData || !hasSelectedIndividuals || !hasDetail || selectedCount === 0;
    this.refs.generateReport.disabled = !hasData || !(this.data?.individuals || []).length;
    this.refs.removeConfirmed.disabled = !hasData || !hasSelectedIndividuals || !hasDetail || selectedCount === 0;
    this.refs.selectSuspicious.disabled = !hasData || !hasSelectedIndividuals || !hasDetail || visibleSuspiciousCount === 0;
    this.refs.clearFixes.disabled = !hasData || selectedCount === 0;
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
    const field = this.getCurrentColorField();
    const issueThreshold = this.getCurrentIssueThreshold();
    this.pendingIssueStatus = status;
    this.refs.issueTitle.textContent = `Mark fixes as ${status}`;
    this.refs.issueMeta.innerHTML = `
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Checked fixes:</strong> ${escapeHtml(formatCount(selectedFixes.length))}</div>
      <div><strong>Issue variable:</strong> ${escapeHtml(field?.label || "Not set")}</div>
      <div><strong>Issue threshold:</strong> ${escapeHtml(issueThreshold || "Not set")}</div>
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
    if (!this.currentArtifact || !(this.data?.individuals || []).length) {
      return;
    }
    this.refs.reportUser.value = this.getUser();
    this.refs.reportType.value = "issue_first";
    this.refs.reportScope.value = "visible";
    this.refs.reportOutputMode.value = "combined";
    this.refs.reportScreenshotMode.value = "auto";
    this.refs.reportBasemap.value = "current";
    this.refs.reportSpreadIndividuals.checked = true;
    this.refs.reportLinks.innerHTML = this.lastReportLinks.map(link => (
      `<a href="${link.href}" target="_blank" rel="noreferrer">${escapeHtml(link.label)}</a>`
    )).join("");
    this.refs.reportStatus.textContent = "";
    this.refs.reportStatus.classList.remove("error");
    this.refs.reportSubmit.disabled = false;
    this.refs.reportClose.disabled = false;
    this.populateReportIndividualOptions();
    this.updateReportModeUi();
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

  getCurrentIssueThreshold() {
    const field = this.getCurrentColorField();
    return getIssueThresholdFromState(field, this.thresholdState);
  }

  async submitIssueAction() {
    const selectedFixes = this.getSelectedFixes();
    const user = this.refs.issueUser.value.trim();
    const issueType = this.refs.issueType.value.trim();
    const issueField = this.getCurrentColorField()?.key || "";
    const issueThreshold = this.getCurrentIssueThreshold();
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
            issue_field: issueField,
            issue_threshold: issueThreshold,
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
    const reportType = this.getReportType();
    const selectedFixes = this.getReportFixes();
    const reportIndividuals = this.getReportIndividuals();
    const snapshotWindows = reportType === "issue_first"
      ? this.getEffectiveReportSnapshotWindows()
      : this.buildIndividualProfileSnapshotWindows();
    const user = this.refs.reportUser.value.trim();
    if (reportType === "issue_first" && !selectedFixes.length) {
      return;
    }
    if (reportType === "individual_profile" && !reportIndividuals.length) {
      return;
    }
    if (!user) {
      this.refs.reportStatus.textContent = "User is required.";
      this.refs.reportStatus.classList.add("error");
      return;
    }

    this.refs.reportSubmit.disabled = true;
    this.refs.reportClose.disabled = true;
    this.refs.reportStatus.textContent = reportType === "issue_first"
      ? `Generating a report for ${formatCount(selectedFixes.length)} suspected fixes...`
      : `Generating profile report${reportIndividuals.length === 1 ? "" : "s"} for ${formatCount(reportIndividuals.length)} individuals...`;
    this.refs.reportStatus.classList.remove("error");
    this.setStatus(this.refs.reportStatus.textContent);

    try {
      const outputMode = this.getReportOutputMode(reportIndividuals);
      const screenshotMode = reportType === "issue_first" ? this.refs.reportScreenshotMode.value : "auto";
      const snapshots = reportType === "issue_first"
        ? (screenshotMode === "auto" ? await this.captureSnapshotsForSelection(snapshotWindows) : [])
        : await this.captureSnapshotsForSelection(snapshotWindows);
      const issueIds = reportType === "issue_first"
        ? uniqueNonEmpty(selectedFixes.flatMap(fix => reportIssueIds(fix)))
        : [];
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/generate-report`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            report_type: reportType,
            output_mode: outputMode,
            fix_keys: selectedFixes.map(fix => fix.fixKey),
            issue_ids: issueIds,
            individuals: reportIndividuals,
            report_fixes: reportType === "issue_first" ? selectedFixes.map(fix => this.serializeFixForReport(fix)) : [],
            snapshot_windows: reportType === "issue_first" ? snapshotWindows.map(window => this.serializeSnapshotWindowForReport(window)) : [],
            screenshot_mode: screenshotMode,
            snapshots,
            user,
          }),
        },
      );
      this.setUser(user);
      const analysisId = result.analysis.analysis_id;
      this.lastReportLinks = buildReportLinksFromAnalysis(result.analysis, {
        family: this.currentFamily,
        study: this.currentStudy,
      });
      this.refs.reportLinks.innerHTML = this.lastReportLinks.map(link => (
        `<a href="${link.href}" target="_blank" rel="noreferrer">${escapeHtml(link.label)}</a>`
      )).join("");
      this.refs.reportStatus.textContent = this.lastReportLinks.length
        ? `Created analysis ${analysisId}.`
        : `Created analysis ${analysisId}, but no report links were returned.`;
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
      style: this.getSelectedReportBasemapStyle(),
    });
    try {
      const snapshots = [];
      let skippedCount = 0;
      for (let index = 0; index < snapshotWindows.length; index += 1) {
        const window = snapshotWindows[index];
        this.refs.reportStatus.textContent = `Rendering snapshot ${index + 1} of ${snapshotWindows.length}...`;
        this.setStatus(this.refs.reportStatus.textContent);
        let dataUrl = null;
        try {
          dataUrl = await renderer.capture(window);
        } catch (error) {
          skippedCount += 1;
          this.refs.reportStatus.textContent = `Skipped snapshot ${index + 1} of ${snapshotWindows.length}: ${error.message}`;
          this.refs.reportStatus.classList.add("error");
          this.setStatus(this.refs.reportStatus.textContent, true);
          continue;
        }
        if (!dataUrl) {
          skippedCount += 1;
          continue;
        }
        snapshots.push({
          snapshot_key: window.snapshotKey,
          caption: window.caption,
          data_url: dataUrl,
        });
      }
      if (skippedCount > 0) {
        this.refs.reportStatus.textContent = `Rendered ${formatCount(snapshots.length)} snapshots and skipped ${formatCount(skippedCount)} that timed out or failed.`;
        this.refs.reportStatus.classList.add("error");
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
  const overviewFixes = parseMovementFixes(summary.fixes || []);

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
    overviewFixes,
    overviewHasAllFixes: overviewFixes.length >= (Number(summary.total_rows) || 0),
    detailFixes: [],
    detailState: "idle",
    detailIndividuals: [],
    detailLimit: null,
    detailMatchingFixCount: 0,
    detailReturnedFixCount: 0,
    detailTruncated: false,
    reportAllFixes: [],
    reportAllState: "idle",
    reportAllLimit: null,
    reportAllMatchingFixCount: 0,
    reportAllReturnedFixCount: 0,
    reportAllTruncated: false,
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
      issueField: String(item?.review?.issue_field || ""),
      issueThreshold: String(item?.review?.issue_threshold || ""),
      issues: normalizeReviewIssues(item?.review),
      issueNote: String(item?.review?.issue_note || ""),
      ownerQuestion: String(item?.review?.owner_question || ""),
      reviewUser: String(item?.review?.review_user || ""),
      reviewedAt: String(item?.review?.reviewed_at || ""),
    },
  })).filter(item => item.fixKey) : [];
}

function normalizeReviewIssues(review) {
  const issues = Array.isArray(review?.issues) ? review.issues : [];
  const cleaned = issues
    .filter(item => item && typeof item === "object")
    .map(item => ({
      status: String(item.status || "").trim(),
      issueId: String(item.issue_id || item.issueId || "").trim(),
      issueType: String(item.issue_type || item.issueType || "").trim(),
      issueField: String(item.issue_field || item.issueField || "").trim(),
      issueThreshold: String(item.issue_threshold || item.issueThreshold || "").trim(),
      issueNote: String(item.issue_note || item.issueNote || "").trim(),
      ownerQuestion: String(item.owner_question || item.ownerQuestion || "").trim(),
      reviewUser: String(item.review_user || item.reviewUser || "").trim(),
      reviewedAt: String(item.reviewed_at || item.reviewedAt || "").trim(),
    }))
    .filter(item => item.issueId || item.issueType);
  if (cleaned.length) {
    return cleaned;
  }
  const legacyIssue = {
    status: String(review?.status || "").trim(),
    issueId: String(review?.issue_id || review?.issueId || "").trim(),
    issueType: String(review?.issue_type || review?.issueType || "").trim(),
    issueField: String(review?.issue_field || review?.issueField || "").trim(),
    issueThreshold: String(review?.issue_threshold || review?.issueThreshold || "").trim(),
    issueNote: String(review?.issue_note || review?.issueNote || "").trim(),
    ownerQuestion: String(review?.owner_question || review?.ownerQuestion || "").trim(),
    reviewUser: String(review?.review_user || review?.reviewUser || "").trim(),
    reviewedAt: String(review?.reviewed_at || review?.reviewedAt || "").trim(),
  };
  return legacyIssue.issueId || legacyIssue.issueType ? [legacyIssue] : [];
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

function computeHistogramBins(values, requestedBinCount = 24, { mode = "full", clippedMin = null, clippedMax = null } = {}) {
  if (!Array.isArray(values) || !values.length) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const observedMin = sorted[0];
  const observedMax = sorted[sorted.length - 1];
  const useClippedRange = mode === "clipped"
    && Number.isFinite(clippedMin)
    && Number.isFinite(clippedMax)
    && clippedMin < clippedMax;
  const min = useClippedRange ? clippedMin : observedMin;
  const max = useClippedRange ? clippedMax : observedMax;
  const binCount = Math.max(1, Math.min(40, Math.floor(requestedBinCount) || 24));
  if (observedMin === observedMax) {
    return {
      min: observedMin,
      max: observedMax,
      observedMin,
      observedMax,
      scaleKind: "linear",
      scaleOffset: observedMin,
      maxCount: values.length,
      bins: [{ start: observedMin, end: observedMax, count: values.length }],
    };
  }
  const rangeValues = useClippedRange
    ? sorted.filter(value => value >= min && value <= max)
    : sorted;
  const p95 = quantile(rangeValues.length ? rangeValues : sorted, 0.95);
  const useLogScale = observedMin >= 0 && observedMax > observedMin && p95 > 0 && (observedMax / p95) >= 6;
  const scaleOffset = observedMin;
  const scaleValue = useLogScale
    ? value => Math.log1p(Math.max(0, value - scaleOffset))
    : value => value;
  const unscaleValue = useLogScale
    ? value => Math.expm1(value) + scaleOffset
    : value => value;
  const scaledMin = scaleValue(min);
  const scaledMax = scaleValue(max);
  const span = (scaledMax - scaledMin) || 1;
  const step = span / binCount;
  const bins = Array.from({ length: binCount }, (_, index) => ({
    start: unscaleValue(scaledMin + (step * index)),
    end: index === binCount - 1 ? max : unscaleValue(scaledMin + (step * (index + 1))),
    count: 0,
  }));
  for (const value of values) {
    const scaledValue = scaleValue(value);
    let index = 0;
    if (scaledValue <= scaledMin) {
      index = 0;
    } else if (scaledValue >= scaledMax) {
      index = binCount - 1;
    } else {
      index = Math.floor(((scaledValue - scaledMin) / span) * binCount);
      if (index < 0) {
        index = 0;
      } else if (index >= binCount) {
        index = binCount - 1;
      }
    }
    bins[index].count += 1;
  }
  return {
    min,
    max,
    observedMin,
    observedMax,
    mode: useClippedRange ? "clipped" : "full",
    scaleKind: useLogScale ? "log" : "linear",
    scaleOffset,
    maxCount: Math.max(1, ...bins.map(bin => bin.count)),
    bins,
  };
}

function clampThresholdValue(value, min, max) {
  if (!Number.isFinite(value)) {
    return null;
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return value;
  }
  if (min > max) {
    return value;
  }
  return Math.max(min, Math.min(max, value));
}

function histogramValueToRatio(histogram, value) {
  if (!histogram) {
    return 0;
  }
  const min = Number(histogram.min);
  const max = Number(histogram.max);
  const bins = Array.isArray(histogram.bins) ? histogram.bins : [];
  if (!Number.isFinite(value) || !Number.isFinite(min) || !Number.isFinite(max) || !bins.length || min === max) {
    return 0.5;
  }
  const boundedValue = clampThresholdValue(value, min, max);
  if (boundedValue <= bins[0].start) {
    return 0;
  }
  for (let index = 0; index < bins.length; index += 1) {
    const bin = bins[index];
    const isLast = index === bins.length - 1;
    if (boundedValue > bin.end && !isLast) {
      continue;
    }
    const start = Number(bin.start);
    const end = Number(bin.end);
    const localRatio = end > start
      ? (boundedValue - start) / (end - start)
      : 0.5;
    return Math.max(0, Math.min(1, (index + localRatio) / bins.length));
  }
  return 1;
}

function histogramRatioToValue(histogram, ratio) {
  if (!histogram) {
    return 0;
  }
  const bins = Array.isArray(histogram.bins) ? histogram.bins : [];
  const boundedRatio = Math.max(0, Math.min(1, Number(ratio) || 0));
  if (!bins.length) {
    return Number(histogram.min) || 0;
  }
  const scaledIndex = boundedRatio * bins.length;
  const index = Math.min(bins.length - 1, Math.floor(scaledIndex));
  const bin = bins[index];
  const start = Number(bin.start);
  const end = Number(bin.end);
  const localRatio = Math.max(0, Math.min(1, scaledIndex - index));
  if (!Number.isFinite(start) || !Number.isFinite(end) || start === end) {
    return start;
  }
  return start + ((end - start) * localRatio);
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
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
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

function sampleItemsEvenly(items, limit) {
  if (!Array.isArray(items)) {
    return [];
  }
  if (!Number.isFinite(limit) || limit === null || limit <= 0 || items.length <= limit) {
    return [...items];
  }
  if (limit === 1) {
    return [items[0]];
  }
  const sampled = [];
  const maxIndex = items.length - 1;
  for (let index = 0; index < limit; index += 1) {
    const itemIndex = Math.round((index / (limit - 1)) * maxIndex);
    sampled.push(items[itemIndex]);
  }
  return sampled.filter((item, index) => index === 0 || item !== sampled[index - 1]);
}

function snapshotWindowIssueTypes(window) {
  const issueTypes = uniqueNonEmpty(Array.isArray(window?.issueTypes) ? window.issueTypes : []);
  if (issueTypes.length) {
    return issueTypes;
  }
  return [String(window?.issueType || "Unspecified issue")];
}

function mostCommonIssueField(fixes) {
  const counts = new Map();
  for (const fix of Array.isArray(fixes) ? fixes : []) {
    const key = String(fix?.review?.issueField || "").trim();
    if (!key) {
      continue;
    }
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  let bestKey = "";
  let bestCount = -1;
  for (const [key, count] of counts.entries()) {
    if (count > bestCount || (count === bestCount && key.localeCompare(bestKey) < 0)) {
      bestKey = key;
      bestCount = count;
    }
  }
  return bestKey;
}

function deriveReportSampleValue(
  focalFixes,
  fallbackField,
  colorFieldByKey,
  { thresholdFieldKey = "", thresholdValue = null, thresholdReverse = false } = {},
) {
  const issueFieldKey = mostCommonIssueField(focalFixes);
  const effectiveField = (issueFieldKey && colorFieldByKey?.get(issueFieldKey)) || fallbackField;
  if (!effectiveField || effectiveField.kind !== "numeric") {
    return null;
  }
  const values = (Array.isArray(focalFixes) ? focalFixes : [])
    .map(fix => finiteOrNull(fix?.attributes?.[effectiveField.key]))
    .filter(value => typeof value === "number");
  if (!values.length) {
    return null;
  }
  if (effectiveField.key === thresholdFieldKey && typeof thresholdValue === "number") {
    const edgeValue = thresholdReverse ? Math.min(...values) : Math.max(...values);
    return Math.abs(edgeValue - thresholdValue);
  }
  const sorted = [...values].sort((left, right) => left - right);
  return quantile(sorted, 0.5);
}

function buildIssueSampleBuckets(indexed) {
  const bucketByIndex = new Map();
  const byIssueType = new Map();
  for (const candidate of indexed) {
    const issueType = candidate.issueTypes[0] || "Unspecified issue";
    const bucket = byIssueType.get(issueType) || [];
    bucket.push(candidate);
    byIssueType.set(issueType, bucket);
  }
  for (const candidates of byIssueType.values()) {
    const withValues = candidates
      .filter(candidate => Number.isFinite(candidate.sampleValue))
      .sort((left, right) => left.sampleValue - right.sampleValue || left.index - right.index);
    if (!withValues.length) {
      for (const candidate of candidates) {
        bucketByIndex.set(candidate.index, "default");
      }
      continue;
    }
    const lowCut = quantile(withValues.map(candidate => candidate.sampleValue), 1 / 3);
    const highCut = quantile(withValues.map(candidate => candidate.sampleValue), 2 / 3);
    for (const candidate of candidates) {
      if (!Number.isFinite(candidate.sampleValue)) {
        bucketByIndex.set(candidate.index, "default");
      } else if (candidate.sampleValue <= lowCut) {
        bucketByIndex.set(candidate.index, "low");
      } else if (candidate.sampleValue >= highCut) {
        bucketByIndex.set(candidate.index, "high");
      } else {
        bucketByIndex.set(candidate.index, "mid");
      }
    }
  }
  return bucketByIndex;
}

function sampleReportSnapshotWindows(snapshotWindows, limit, { spreadIndividuals = false } = {}) {
  if (!Array.isArray(snapshotWindows)) {
    return [];
  }
  if (!Number.isFinite(limit) || limit === null || limit <= 0 || snapshotWindows.length <= limit) {
    return [...snapshotWindows];
  }

  const indexed = snapshotWindows.map((window, index) => ({
    window,
    index,
    issueTypes: snapshotWindowIssueTypes(window),
    individual: String(window?.individual || ""),
    sampleValue: Number.isFinite(window?.sampleValue) ? Number(window.sampleValue) : null,
  }));
  const sampled = [];
  const sampledIndexes = new Set();
  const sampledIssueCounts = new Map();
  const sampledIssueBucketCounts = new Map();
  const sampledIndividuals = new Map();
  const sampleBuckets = buildIssueSampleBuckets(indexed);
  while (sampled.length < limit) {
    const eligibleIssueTypes = uniqueNonEmpty(
      indexed
        .filter(candidate => !sampledIndexes.has(candidate.index))
        .flatMap(candidate => candidate.issueTypes),
    );
    if (!eligibleIssueTypes.length) {
      break;
    }
    eligibleIssueTypes.sort((left, right) => {
      const leftCount = sampledIssueCounts.get(left) || 0;
      const rightCount = sampledIssueCounts.get(right) || 0;
      return leftCount - rightCount || left.localeCompare(right);
    });
    const targetIssueType = eligibleIssueTypes[0];

    let best = null;
    for (const candidate of indexed) {
      if (sampledIndexes.has(candidate.index) || !candidate.issueTypes.includes(targetIssueType)) {
        continue;
      }
      const issueCounts = candidate.issueTypes.map(issueType => sampledIssueCounts.get(issueType) || 0);
      const lowIssueCoverage = issueCounts.filter(count => count === (sampledIssueCounts.get(targetIssueType) || 0)).length;
      const issueLoad = issueCounts.reduce((sum, count) => sum + count, 0);
      const individualLoad = sampledIndividuals.get(candidate.individual) || 0;
      const sampleBucket = sampleBuckets.get(candidate.index) || "default";
      const issueBucketKey = `${targetIssueType}\u0000${sampleBucket}`;
      const issueBucketLoad = sampledIssueBucketCounts.get(issueBucketKey) || 0;
      if (
        !best
        || lowIssueCoverage > best.lowIssueCoverage
        || (lowIssueCoverage === best.lowIssueCoverage && issueBucketLoad < best.issueBucketLoad)
        || (lowIssueCoverage === best.lowIssueCoverage && issueBucketLoad === best.issueBucketLoad && issueLoad < best.issueLoad)
        || (lowIssueCoverage === best.lowIssueCoverage && issueBucketLoad === best.issueBucketLoad && issueLoad === best.issueLoad && spreadIndividuals && individualLoad < best.individualLoad)
        || (
          lowIssueCoverage === best.lowIssueCoverage
          && issueBucketLoad === best.issueBucketLoad
          && issueLoad === best.issueLoad
          && (!spreadIndividuals || individualLoad === best.individualLoad)
          && candidate.index < best.index
        )
      ) {
        best = {
          ...candidate,
          lowIssueCoverage,
          issueBucketLoad,
          issueLoad,
          individualLoad,
        };
      }
    }
    if (!best) {
      break;
    }
    sampled.push(best);
    sampledIndexes.add(best.index);
    sampledIndividuals.set(best.individual, (sampledIndividuals.get(best.individual) || 0) + 1);
    for (const issueType of best.issueTypes) {
      sampledIssueCounts.set(issueType, (sampledIssueCounts.get(issueType) || 0) + 1);
      const sampleBucket = sampleBuckets.get(best.index) || "default";
      const issueBucketKey = `${issueType}\u0000${sampleBucket}`;
      sampledIssueBucketCounts.set(issueBucketKey, (sampledIssueBucketCounts.get(issueBucketKey) || 0) + 1);
    }
  }

  return sampled
    .sort((left, right) => left.index - right.index)
    .map(item => item.window);
}

function sampleItemsEvenlyByGroup(items, limit, getGroupKey) {
  if (!Array.isArray(items)) {
    return [];
  }
  if (!Number.isFinite(limit) || limit === null || limit <= 0 || items.length <= limit) {
    return [...items];
  }
  const groups = new Map();
  for (const item of items) {
    const groupKey = String(getGroupKey(item) || "");
    const bucket = groups.get(groupKey) || [];
    bucket.push(item);
    groups.set(groupKey, bucket);
  }
  if (groups.size <= 1) {
    return sampleItemsEvenly(items, limit);
  }
  const groupEntries = Array.from(groups.entries()).map(([key, groupItems]) => ({
    key,
    items: sampleItemsEvenly(groupItems, Math.min(groupItems.length, limit)),
    index: 0,
  }));
  const sampled = [];
  while (sampled.length < limit) {
    let addedInPass = false;
    for (const entry of groupEntries) {
      if (sampled.length >= limit) {
        break;
      }
      if (entry.index >= entry.items.length) {
        continue;
      }
      sampled.push(entry.items[entry.index]);
      entry.index += 1;
      addedInPass = true;
    }
    if (!addedInPass) {
      break;
    }
  }
  return sampled;
}

function arraysEqual(left, right) {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((value, index) => value === right[index]);
}

function reportIssueTypes(fix) {
  const issues = Array.isArray(fix?.review?.issues) ? fix.review.issues : [];
  const issueTypes = uniqueNonEmpty(issues.map(issue => issue.issueType || ""));
  if (issueTypes.length) {
    return issueTypes.map(issueType => issueType || "Unspecified issue");
  }
  const issueType = String(fix?.review?.issueType || "").trim();
  return [issueType || "Unspecified issue"];
}

function reportIssueType(fix) {
  return reportIssueTypes(fix)[0] || "Unspecified issue";
}

function reportIssueIds(fix) {
  const issues = Array.isArray(fix?.review?.issues) ? fix.review.issues : [];
  const issueIds = uniqueNonEmpty(issues.map(issue => issue.issueId || ""));
  if (issueIds.length) {
    return issueIds;
  }
  const issueId = String(fix?.review?.issueId || "").trim();
  return issueId ? [issueId] : [];
}

function formatIssueThresholdSummary(field, { value = null, reverse = false, selectedLevels = [] } = {}) {
  if (!field) {
    return "";
  }
  if (field.kind === "numeric") {
    if (typeof value !== "number") {
      return "";
    }
    return `${reverse ? "<" : ">"} ${formatColorValue(value, "numeric")}`;
  }
  const levels = uniqueNonEmpty(selectedLevels);
  if (!levels.length) {
    return "";
  }
  if (levels.length === 1) {
    return `= ${levels[0]}`;
  }
  const preview = levels.slice(0, 3).join(", ");
  return levels.length <= 3 ? `in ${preview}` : `in ${preview}, +${levels.length - 3} more`;
}

function getIssueThresholdFromState(field, thresholdState) {
  if (!field || !thresholdState || thresholdState.fieldKey !== field.key) {
    return "";
  }
  return formatIssueThresholdSummary(field, {
    value: typeof thresholdState.value === "number" ? thresholdState.value : null,
    reverse: thresholdState.reverse === true,
    selectedLevels: Array.isArray(thresholdState.selectedLevels) ? thresholdState.selectedLevels : [],
  });
}

function reportTrackKey(individual, setName) {
  return `${String(individual)}\u0000${String(setName || "train")}`;
}

function reportLinkLabelForArtifact(logicalName) {
  const name = String(logicalName || "").trim();
  if (!name) {
    return "";
  }
  if (name === "movement_outlier_report.md") return "Markdown Report";
  if (name === "movement_outlier_report.html") return "HTML Report";
  if (name === "movement_outlier_fixes.csv") return "Appendix CSV";
  if (name === "movement_individual_reports.md") return "Markdown Report";
  if (name === "movement_individual_reports.html") return "HTML Report";
  if (name === "movement_individual_report_index.md") return "Markdown Index";
  if (name === "movement_individual_report_index.html") return "HTML Index";
  if (name.endsWith(".html")) return `HTML ${name.replace(/\.html$/, "")}`;
  if (name.endsWith(".md")) return `Markdown ${name.replace(/\.md$/, "")}`;
  if (name.endsWith(".csv")) return `CSV ${name.replace(/\.csv$/, "")}`;
  return name;
}

function compareReportArtifacts(left, right) {
  const leftName = String(left?.logical_name || "");
  const rightName = String(right?.logical_name || "");
  const priority = name => {
    if (name.includes("_report_index.html")) return 1;
    if (name.includes("_report_index.md")) return 2;
    if (name.endsWith(".html")) return 3;
    if (name.endsWith(".md")) return 4;
    if (name.endsWith(".csv")) return 5;
    return 9;
  };
  return priority(leftName) - priority(rightName) || leftName.localeCompare(rightName);
}

function buildReportLinksFromAnalysis(analysis, { family, study } = {}) {
  const analysisId = String(analysis?.analysis_id || "").trim();
  const outputs = Array.isArray(analysis?.realized_output_artifacts) ? [...analysis.realized_output_artifacts] : [];
  if (!analysisId || !family || !study || !outputs.length) {
    return [];
  }
  return outputs
    .filter(item => {
      const name = String(item?.logical_name || "");
      return name.endsWith(".html") || name.endsWith(".md") || name.endsWith(".csv");
    })
    .sort(compareReportArtifacts)
    .map(item => ({
      label: reportLinkLabelForArtifact(item.logical_name),
      href: `/api/apps/movement/family/${encodeURIComponent(family)}/study/${encodeURIComponent(study)}/analysis/${encodeURIComponent(analysisId)}/artifact/${encodeURIComponent(item.logical_name)}`,
    }));
}

function niceSnapshotTickStep(span) {
  if (!Number.isFinite(span) || span <= 0) {
    return 1;
  }
  const rough = span / 4;
  const magnitude = 10 ** Math.floor(Math.log10(Math.abs(rough)));
  for (const multiplier of [1, 2, 5, 10]) {
    const step = magnitude * multiplier;
    if (step >= rough) {
      return step;
    }
  }
  return magnitude * 10;
}

function buildSnapshotAxisTicks(minValue, maxValue) {
  const span = maxValue - minValue;
  if (!Number.isFinite(span) || span <= 0) {
    return [minValue];
  }
  const step = niceSnapshotTickStep(span);
  let start = Math.floor(minValue / step) * step;
  if (start > minValue) {
    start -= step;
  }
  const ticks = [];
  for (let value = start; value <= maxValue + step; value += step) {
    if (value >= minValue - (step * 0.1) && value <= maxValue + (step * 0.1)) {
      ticks.push(Number(value.toFixed(6)));
    }
  }
  return ticks.length ? ticks : [minValue, maxValue];
}

function formatSnapshotLongitude(value) {
  const direction = value >= 0 ? "E" : "W";
  return `${Math.abs(value).toFixed(2)}°${direction}`;
}

function formatSnapshotLatitude(value) {
  const direction = value >= 0 ? "N" : "S";
  return `${Math.abs(value).toFixed(2)}°${direction}`;
}

function renderSnapshotCanvasWithGrid(sourceCanvas, map) {
  const bounds = map?.getBounds?.();
  if (!bounds) {
    return sourceCanvas.toDataURL("image/png");
  }
  const outputCanvas = document.createElement("canvas");
  outputCanvas.width = sourceCanvas.width;
  outputCanvas.height = sourceCanvas.height;
  const ctx = outputCanvas.getContext("2d");
  if (!ctx) {
    return sourceCanvas.toDataURL("image/png");
  }
  ctx.drawImage(sourceCanvas, 0, 0);

  const container = map.getContainer();
  const scaleX = sourceCanvas.width / Math.max(1, container.clientWidth || sourceCanvas.width);
  const scaleY = sourceCanvas.height / Math.max(1, container.clientHeight || sourceCanvas.height);
  const bottomBand = 34 * scaleY;
  const leftBand = 66 * scaleX;
  const width = outputCanvas.width;
  const height = outputCanvas.height;
  const south = bounds.getSouth();
  const north = bounds.getNorth();
  const west = bounds.getWest();
  const east = bounds.getEast();
  const midLat = (south + north) / 2;
  const midLon = (west + east) / 2;
  const lonTicks = buildSnapshotAxisTicks(west, east);
  const latTicks = buildSnapshotAxisTicks(south, north);

  ctx.save();
  ctx.fillStyle = "rgba(255, 255, 255, 0.72)";
  ctx.fillRect(0, height - bottomBand, width, bottomBand);
  ctx.fillRect(0, 0, leftBand, height);
  ctx.strokeStyle = "rgba(74, 98, 110, 0.65)";
  ctx.lineWidth = Math.max(1, 1.2 * Math.min(scaleX, scaleY));
  ctx.setLineDash([6 * scaleX, 8 * scaleY]);

  for (const lon of lonTicks) {
    const point = map.project([lon, midLat]);
    const x = point.x * scaleX;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, height - bottomBand);
    ctx.stroke();
  }
  for (const lat of latTicks) {
    const point = map.project([midLon, lat]);
    const y = point.y * scaleY;
    ctx.beginPath();
    ctx.moveTo(leftBand, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  ctx.setLineDash([]);
  ctx.fillStyle = "#243b53";
  ctx.strokeStyle = "#243b53";
  ctx.font = `${Math.max(12, 12 * scaleY)}px Arial, sans-serif`;
  ctx.textAlign = "center";
  ctx.textBaseline = "alphabetic";
  for (const lon of lonTicks) {
    const point = map.project([lon, midLat]);
    const x = point.x * scaleX;
    ctx.beginPath();
    ctx.moveTo(x, height - bottomBand);
    ctx.lineTo(x, height - (bottomBand * 0.62));
    ctx.stroke();
    ctx.fillText(formatSnapshotLongitude(lon), x, height - (bottomBand * 0.24));
  }

  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (const lat of latTicks) {
    const point = map.project([midLon, lat]);
    const y = point.y * scaleY;
    ctx.fillText(formatSnapshotLatitude(lat), leftBand - (8 * scaleX), y);
  }
  ctx.restore();
  return outputCanvas.toDataURL("image/png");
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
  await waitForMapReady(map, REPORT_SNAPSHOT_IDLE_TIMEOUT_MS);

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
      const mapReady = await waitForMapReady(map, REPORT_SNAPSHOT_IDLE_TIMEOUT_MS);
      if (!mapReady) {
        throw new Error("Map idle timeout");
      }
      await waitForAnimationFrames(2);
      try {
        if (snapshotWindow?.showGrid) {
          return renderSnapshotCanvasWithGrid(map.getCanvas(), map);
        }
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
  const secondarySet = new Set(snapshotWindow.secondaryFixKeys || []);
  const orderedFixes = (snapshotWindow.windowFixes || []).filter(fix => (
    Number.isFinite(fix?.position?.[0]) && Number.isFinite(fix?.position?.[1])
  ));
  const contextPoints = [];
  const secondarySuspiciousPoints = [];
  const suspiciousPoints = [];
  for (const fix of orderedFixes) {
    const point = {
      position: fix.position,
      fixKey: fix.fixKey,
    };
    if (anchorSet.has(fix.fixKey)) {
      suspiciousPoints.push(point);
    } else if (secondarySet.has(fix.fixKey)) {
      secondarySuspiciousPoints.push(point);
    } else {
      contextPoints.push(point);
    }
  }
  const startFix = orderedFixes[0] || null;
  const endFix = orderedFixes[orderedFixes.length - 1] || null;
  const markerPoints = [];
  if (startFix) {
    markerPoints.push({
      position: startFix.position,
      glyph: "\u25B2",
      label: "start",
    });
  }
  if (endFix) {
    markerPoints.push({
      position: endFix.position,
      glyph: "\u25A0",
      label: "end",
    });
  }
  return [
    new deck.PathLayer({
      id: `report-snapshot-path-${snapshotWindow.snapshotKey}`,
      data: [{ path: orderedFixes.map(fix => fix.position) }],
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
      id: `report-snapshot-secondary-${snapshotWindow.snapshotKey}`,
      data: secondarySuspiciousPoints,
      getPosition: item => item.position,
      getFillColor: [245, 181, 54, 191],
      getLineColor: [48, 64, 82, 191],
      stroked: true,
      lineWidthMinPixels: 1.25,
      getRadius: 85,
      radiusMinPixels: 4,
      radiusMaxPixels: 8,
      pickable: false,
    }),
    new deck.ScatterplotLayer({
      id: `report-snapshot-anchors-${snapshotWindow.snapshotKey}`,
      data: suspiciousPoints,
      getPosition: item => item.position,
      getFillColor: [242, 80, 103, 191],
      getLineColor: [255, 255, 255, 191],
      stroked: true,
      lineWidthMinPixels: 1.5,
      getRadius: 95,
      radiusMinPixels: 5,
      radiusMaxPixels: 10,
      pickable: false,
    }),
    new deck.TextLayer({
      id: `report-snapshot-marker-labels-${snapshotWindow.snapshotKey}`,
      data: markerPoints,
      getPosition: item => item.position,
      getText: item => item.glyph,
      getColor: [32, 220, 90, 255],
      getSize: 24,
      sizeMinPixels: 18,
      sizeMaxPixels: 30,
      getTextAnchor: "middle",
      getAlignmentBaseline: "center",
      characterSet: ["\u25B2", "\u25A0"],
      pickable: false,
    }),
  ];
}

function buildWindowBounds(windowFixes) {
  const validFixes = (Array.isArray(windowFixes) ? windowFixes : []).filter(fix => (
    Number.isFinite(fix?.position?.[0]) && Number.isFinite(fix?.position?.[1])
  ));
  if (!validFixes.length) {
    return null;
  }
  let minLon = validFixes[0].position[0];
  let maxLon = validFixes[0].position[0];
  let minLat = validFixes[0].position[1];
  let maxLat = validFixes[0].position[1];
  for (const fix of validFixes) {
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

async function waitForMapReady(map, timeoutMs = REPORT_SNAPSHOT_IDLE_TIMEOUT_MS) {
  if (!map) {
    return false;
  }
  const startMs = Date.now();
  while ((Date.now() - startMs) < timeoutMs) {
    const loaded = typeof map.loaded === "function" ? map.loaded() : true;
    const styleLoaded = typeof map.isStyleLoaded === "function" ? map.isStyleLoaded() : loaded;
    const tilesLoaded = typeof map.areTilesLoaded === "function" ? map.areTilesLoaded() : loaded;
    const moving = typeof map.isMoving === "function" ? map.isMoving() : false;
    const zooming = typeof map.isZooming === "function" ? map.isZooming() : false;
    const rotating = typeof map.isRotating === "function" ? map.isRotating() : false;
    if (loaded && styleLoaded && tilesLoaded && !moving && !zooming && !rotating) {
      return true;
    }
    await new Promise(resolve => window.setTimeout(resolve, 50));
  }
  return false;
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

function discreteFieldLevelLabel(field, value) {
  if (field?.kind === "boolean") {
    if (value === true) return "True";
    if (value === false) return "False";
    return "Missing";
  }
  if (value === null || value === undefined || value === "") {
    return "Missing";
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
