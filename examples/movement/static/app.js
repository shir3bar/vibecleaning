const MOVEMENT_APP_MODE_VALUE = document
  .querySelector('meta[name="vibecleaning-movement-mode"]')
  ?.getAttribute("content");
const MOVEMENT_APP_MODE = ["slim_movement", "rds_movement"].includes(MOVEMENT_APP_MODE_VALUE)
  ? MOVEMENT_APP_MODE_VALUE
  : "movement";
const MOVEMENT_APP_CONFIG = Object.freeze({
  mode: MOVEMENT_APP_MODE,
  defaultFamily: MOVEMENT_APP_MODE === "slim_movement" ? "movement_raw"
    : MOVEMENT_APP_MODE === "rds_movement"
      ? "movement_rds"
      : "movement_clean",
  storageKey: MOVEMENT_APP_MODE === "slim_movement"
    ? "vibecleaning_slim_movement_state"
    : MOVEMENT_APP_MODE === "rds_movement"
      ? "vibecleaning_rds_movement_state"
      : "vibecleaning_movement_example_state",
  candidateQueries: MOVEMENT_APP_MODE === "movement",
  featureSpace: MOVEMENT_APP_MODE === "movement",
  osmDerivedFeatures: MOVEMENT_APP_MODE === "movement",
  rdsSource: MOVEMENT_APP_MODE === "rds_movement",
});
const OSM_INTERACTION = (MOVEMENT_APP_MODE === "movement" || MOVEMENT_APP_MODE === "rds_movement")
  ? await import("/static/osm_layer.js")
  : null;

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

const OSM_STREETS_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">OpenStreetMap</a> contributors';
const OSM_STREETS_ATTRIBUTION_TEXT = "© OpenStreetMap contributors";
const CARTO_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions" target="_blank" rel="noreferrer">CARTO</a>';
const CARTO_ATTRIBUTION_TEXT = "© OpenStreetMap contributors © CARTO";
const ESRI_WORLD_IMAGERY_ATTRIBUTION = 'Imagery &copy; <a href="https://www.esri.com/" target="_blank" rel="noreferrer">Esri</a>, Maxar, Earthstar Geographics, and the GIS User Community';
const ESRI_WORLD_IMAGERY_ATTRIBUTION_TEXT = "Imagery © Esri, Maxar, Earthstar Geographics, and the GIS User Community";
const OPENTOPO_ATTRIBUTION = 'Map data: &copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noreferrer">OpenStreetMap</a> contributors, map style: &copy; <a href="https://opentopomap.org" target="_blank" rel="noreferrer">OpenTopoMap</a>';
const OPENTOPO_ATTRIBUTION_TEXT = "Map data © OpenStreetMap contributors, style © OpenTopoMap";

function buildRasterStyle({ backgroundColor = "#08111b", sources = {}, layerIds = [] } = {}) {
  return {
    version: 8,
    sources,
    layers: [
      {
        id: "background",
        type: "background",
        paint: {
          "background-color": backgroundColor,
        },
      },
      ...layerIds.map(layerId => ({
        id: `${layerId}-raster`,
        type: "raster",
        source: layerId,
        minzoom: 0,
        maxzoom: 22,
      })),
    ],
  };
}

const OSM_STREETS_STYLE = buildRasterStyle({
  backgroundColor: "#f6f4ef",
  sources: {
    "osm-streets": {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      maxzoom: 19,
      attribution: OSM_STREETS_ATTRIBUTION,
    },
  },
  layerIds: ["osm-streets"],
});

const SATELLITE_STYLE = buildRasterStyle({
  backgroundColor: "#09111a",
  sources: {
    "esri-world-imagery": {
      type: "raster",
      tiles: ["https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
      tileSize: 256,
      maxzoom: 19,
      attribution: ESRI_WORLD_IMAGERY_ATTRIBUTION,
    },
  },
  layerIds: ["esri-world-imagery"],
});

const SATELLITE_LABELS_STYLE = buildRasterStyle({
  backgroundColor: "#09111a",
  sources: {
    "esri-world-imagery": {
      type: "raster",
      tiles: ["https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"],
      tileSize: 256,
      maxzoom: 19,
      attribution: ESRI_WORLD_IMAGERY_ATTRIBUTION,
    },
    "esri-world-transportation": {
      type: "raster",
      tiles: ["https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}"],
      tileSize: 256,
      maxzoom: 19,
      attribution: ESRI_WORLD_IMAGERY_ATTRIBUTION,
    },
    "esri-world-boundaries": {
      type: "raster",
      tiles: ["https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}"],
      tileSize: 256,
      maxzoom: 19,
      attribution: ESRI_WORLD_IMAGERY_ATTRIBUTION,
    },
  },
  layerIds: ["esri-world-imagery", "esri-world-transportation", "esri-world-boundaries"],
});

const TOPOGRAPHIC_STYLE = buildRasterStyle({
  backgroundColor: "#dde4d1",
  sources: {
    "open-topo": {
      type: "raster",
      tiles: ["https://tile.opentopomap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      maxzoom: 17,
      attribution: OPENTOPO_ATTRIBUTION,
    },
  },
  layerIds: ["open-topo"],
});

const BASEMAP_PRESETS = {
  Blank: {
    name: "Blank",
    style: LOCAL_BLANK_STYLE,
    snapshotStyle: LOCAL_BLANK_STYLE,
    attributionHtml: "",
    attributionText: "",
  },
  Positron: {
    name: "Positron",
    style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    snapshotStyle: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    attributionHtml: CARTO_ATTRIBUTION,
    attributionText: CARTO_ATTRIBUTION_TEXT,
  },
  "OSM Streets": {
    name: "OSM Streets",
    style: OSM_STREETS_STYLE,
    snapshotStyle: OSM_STREETS_STYLE,
    attributionHtml: OSM_STREETS_ATTRIBUTION,
    attributionText: OSM_STREETS_ATTRIBUTION_TEXT,
  },
  Satellite: {
    name: "Satellite",
    style: SATELLITE_STYLE,
    snapshotStyle: SATELLITE_STYLE,
    attributionHtml: ESRI_WORLD_IMAGERY_ATTRIBUTION,
    attributionText: ESRI_WORLD_IMAGERY_ATTRIBUTION_TEXT,
  },
  "Satellite + labels": {
    name: "Satellite + labels",
    style: SATELLITE_LABELS_STYLE,
    snapshotStyle: SATELLITE_LABELS_STYLE,
    attributionHtml: ESRI_WORLD_IMAGERY_ATTRIBUTION,
    attributionText: ESRI_WORLD_IMAGERY_ATTRIBUTION_TEXT,
  },
  Topographic: {
    name: "Topographic",
    style: TOPOGRAPHIC_STYLE,
    snapshotStyle: TOPOGRAPHIC_STYLE,
    attributionHtml: OPENTOPO_ATTRIBUTION,
    attributionText: OPENTOPO_ATTRIBUTION_TEXT,
  },
};

const POINT_ALPHA = 215;
const BURST_CASING_RGB = [8, 15, 26];
const BURST_FOCUS_CASING_COLOR = [216, 180, 254, 255];
const BURST_FOCUS_RING_COLOR = [216, 180, 254, 235];
const STORAGE_VERSION = 5;
const INDIVIDUAL_QUEUE_PAGE_SIZE = 25;
const INDIVIDUAL_QUEUE_GROUP_SIZE = 5;
const DEFAULT_BURST_GAP_MODE = "quantile";
const DEFAULT_BURST_GAP_SECONDS = 3600;
const DEFAULT_BURST_GAP_QUANTILE = 0.999;
const DEFAULT_SIDE_PANE_WIDTH_PX = 420;
const MIN_SIDE_PANE_WIDTH_PX = 300;
const MAX_SIDE_PANE_WIDTH_RATIO = 0.55;
const SIDE_PANE_HANDLE_WIDTH_PX = 12;
const STACKED_SIDE_LAYOUT_BREAKPOINT_PX = 1080;
const MIN_INDIVIDUAL_LIST_HEIGHT_PX = 80;
const MIN_QUEUE_CONTROLS_HEIGHT_PX = 90;
const MIN_CHECKED_FIXES_HEIGHT_PX = 60;
const MAX_SELECTED_FIXES_SHOWN = 150;
const LARGE_MAP_POINT_THRESHOLD = 50000;
const DEFAULT_FAMILY = MOVEMENT_APP_CONFIG.defaultFamily;
const NUMERIC_COLOR_MIN_QUANTILE = 0.01;
const NUMERIC_COLOR_MAX_QUANTILE = 0.99;
const REPORT_SNAPSHOT_IDLE_TIMEOUT_MS = 12000;
const ANALYSIS_JOB_POLL_INTERVAL_MS = 1500;
const TABLE_INITIAL_ROW_LIMIT = 250;
const TABLE_ROW_INCREMENT = 250;
const FIX_POPUP_DEFAULT_FIELDS = [
  "set",
  "fix_key",
  "review.status",
  "review.issue_type",
  "step_length_m",
  "speed_mps",
  "time_delta_s",
  "turn_angle_deg",
];
const FIX_POPUP_OFFSET_PX = 14;
const FIX_POPUP_EDGE_PADDING_PX = 12;
const INDIVIDUAL_COLOR_FIELD_KEY = "individual";
const GPS_SPIKE_COLOR_FIELD_KEY = "gps_spike_step_turn";
const DEFAULT_GPS_SPIKE_TURN_ANGLE_DEG = 150;
const INDIVIDUAL_LEGEND_MAX_ITEMS = 24;
const INDIVIDUAL_COLOR_FIELD = Object.freeze({
  key: INDIVIDUAL_COLOR_FIELD_KEY,
  label: "Individual ID",
  kind: "categorical",
  source: "identity",
});
const GPS_SPIKE_COLOR_FIELD = Object.freeze({
  key: GPS_SPIKE_COLOR_FIELD_KEY,
  label: "GPS spike (step + turn)",
  kind: "numeric",
  source: "derived",
});

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

function parseMovementBinary(buffer) {
  const bytes = new Uint8Array(buffer);
  if (bytes.length < 8 || String.fromCharCode(...bytes.subarray(0, 4)) !== "VCM1") {
    throw new Error("The movement binary response has an invalid header.");
  }
  const headerLength = new DataView(buffer).getUint32(4, true);
  const headerEnd = 8 + headerLength;
  if (headerEnd > bytes.length) {
    throw new Error("The movement binary response is truncated.");
  }
  const header = JSON.parse(new TextDecoder().decode(bytes.subarray(8, headerEnd)));
  if (header.format !== "vibecleaning-movement-columns" || Number(header.version) !== 1) {
    throw new Error("The movement binary response uses an unsupported version.");
  }
  const dataOffset = Math.ceil(headerEnd / 8) * 8;
  const constructors = {
    "<f8": Float64Array,
    "<f4": Float32Array,
    "<u4": Uint32Array,
    "<u2": Uint16Array,
    "|u1": Uint8Array,
    "<u1": Uint8Array,
    "<i4": Int32Array,
  };
  const arrays = {};
  for (const [name, metadata] of Object.entries(header.arrays || {})) {
    const ArrayType = constructors[String(metadata?.dtype || "")];
    if (!ArrayType) {
      throw new Error(`Unsupported movement binary dtype ${metadata?.dtype}.`);
    }
    const byteOffset = dataOffset + Number(metadata.offset || 0);
    const length = Number(metadata.length || 0);
    if (byteOffset < dataOffset || byteOffset + (length * ArrayType.BYTES_PER_ELEMENT) > bytes.length) {
      throw new Error(`Movement binary array ${name} is truncated.`);
    }
    arrays[name] = new ArrayType(buffer, byteOffset, length);
  }
  return { buffer, header, arrays };
}

class MovementExampleApp {
  constructor({ mountEl }) {
    this.mountEl = mountEl;
    this.uiState = this.loadUiState();
    this.individualSearchQuery = "";
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
    this.osmContext = null;
    this.osmContextError = "";
    this.osmContextStatus = "idle";
    this.currentTimeMs = 0;
    this.temporalFocusRenderFrame = null;
    this.temporalSliderEngaged = false;
    this.pendingMapSingleClickTimer = null;
    this.lastThresholdMatchKeys = new Set();
    this.lastCandidateMatchKeys = new Set();
    this.loadRequestId = 0;
    this.studyLoadId = 0;
    this.datasetLoadId = 0;
    this.viewTransitionId = 0;
    this.requestControllers = {
      families: null,
      studies: null,
      study: null,
      dataset: null,
      overview: null,
      reviewProjection: null,
      detail: null,
      suspicious: null,
      confirmed: null,
      reportDetail: null,
      osm: null,
      candidateQuery: null,
      queryLibrary: null,
      anomalyRanking: null,
      issueBurstScores: null,
      burstFeatureSpace: null,
      analysisHistory: null,
      binaryFixes: null,
    };
    this.map = null;
    this.mapLoaded = false;
    this.overlay = null;
    this.pendingIssueStatus = "suspected";
    this.flagTargetKind = "none";
    this.manualFlagTarget = {
      individual: "",
      burstIds: new Set(),
      selectionMethods: new Set(),
      origin: "manual",
      sourceAnalysisId: "",
    };
    this.hiddenBurstIds = new Set();
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
    this.candidateQueryPreview = this.makeEmptyCandidateQueryPreview();
    this.anomalyRanking = this.makeEmptyAnomalyRanking();
    this.burstFeatureSpace = this.makeEmptyBurstFeatureSpace();
    this.candidateQueryLibrary = {
      status: "idle",
      queries: [],
      selectedKey: "",
      parameterValues: {},
      executionScope: "whole_study",
      error: "",
    };
    this.thresholdInputPendingBlur = false;
    this.gpsSpikeTurnAngleDeg = DEFAULT_GPS_SPIKE_TURN_ANGLE_DEG;
    this.activeFixPopup = null;
    this.pendingIssueContext = null;
    this.pendingConfirmationGroups = [];
    this.pendingDismissalGroups = [];
    this.editLockProfile = {
      editable: false,
      blockers: [],
      current_dataset_id: "",
      selected_dataset_id: "",
      resume: { allowed: false },
    };
    this.studyEvents = null;
    this.studyEventsKey = "";
    this.editorReleaseDatasetId = "";
    this.focusedRankingBurst = null;
    this.tableSelection = {
      anchorFixKey: "",
      focusFixKey: "",
      selectedFixKeys: new Set(),
      contiguousRange: false,
      selectionMethod: "",
    };
    this.mapRangeAwaitingEnd = false;
    this.tableRenderState = {
      signature: "",
      rowLimit: TABLE_INITIAL_ROW_LIMIT,
    };
    this.sidePaneWidthPx = finiteOrNull(this.uiState.sidePaneWidthPx) || DEFAULT_SIDE_PANE_WIDTH_PX;
    this.sidePaneResize = {
      active: false,
      pointerId: null,
    };
    this.individualListHeightPx = finiteOrNull(this.uiState.individualListHeightPx);
    this.individualQueueListHeightPx = finiteOrNull(
      this.uiState.individualQueueListHeightPx,
    );
    this.individualPaneResize = {
      active: false,
      pointerId: null,
    };
    this.individualReviewQueue = {
      mode: "browse",
      orderMode: this.uiState.individualQueueOrder === "ranking" ? "ranking" : "dataset",
      filterMode: "all",
      pageIndex: Math.max(0, Number(this.uiState.individualQueuePage) || 0),
      groupIndex: 0,
      activeIndividual: "",
      mapScope: "group",
      browseContext: null,
      browseSideSheet: this.uiState.sideSheet || "individuals",
      queueMapView: null,
      stagedDecisions: new Map(),
      skippedIndividuals: new Set(),
      commentDrafts: new Map(),
      commentEditingIndividual: "",
      saving: false,
      appliedRankingAnalysisId: "",
      pendingRankingAnalysisId: "",
      rankingMethod: this.uiState.rankingMethod || "isolation_forest",
    };
    this.handleWindowResize = () => this.handleLayoutResize();
    this.handleSidePanePointerMove = event => this.onSidePanePointerMove(event);
    this.handleSidePanePointerUp = event => this.onSidePanePointerUp(event);
    this.handleIndividualPanePointerMove = event => this.onIndividualPanePointerMove(event);
    this.handleIndividualPanePointerUp = event => this.onIndividualPanePointerUp(event);
  }

  async init() {
    this.renderShell();
    this.bindEvents();
    if (MOVEMENT_APP_CONFIG.candidateQueries) {
      this.renderCandidateQueryLibraryControls();
    }
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
    if (MOVEMENT_APP_CONFIG.candidateQueries) {
      void this.loadCandidateQueryLibrary();
    }
    await this.loadFamilies();
  }

  loadUiState() {
    try {
      const raw = localStorage.getItem(MOVEMENT_APP_CONFIG.storageKey);
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
        basemap: "Positron",
        showTrain: true,
        showTest: true,
        showPoints: true,
        showBursts: true,
        showConfirmed: true,
        burstGapMode: DEFAULT_BURST_GAP_MODE,
        burstGapSeconds: DEFAULT_BURST_GAP_SECONDS,
        burstGapQuantile: DEFAULT_BURST_GAP_QUANTILE,
        anomalyFeatureSet: "movement_only",
        rankingMethod: "isolation_forest",
        colorBy: "step_length_m",
        sideSheet: "individuals",
        tableMode: "fixes",
        tableSort: "track_time",
        tableDescending: false,
        tableFilter: "",
        sidePaneWidthPx: DEFAULT_SIDE_PANE_WIDTH_PX,
        individualListHeightPx: null,
        individualQueueListHeightPx: null,
        individualViewMode: "browse",
        individualQueueOrder: "dataset",
        individualQueuePage: 0,
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
      showBursts: this.refs.showBursts.checked,
      showConfirmed: this.refs.showConfirmed.checked,
      burstGapMode: this.getBurstGapMode(),
      burstGapSeconds: this.getBurstGapSeconds(),
      burstGapQuantile: this.getBurstGapQuantile(),
      anomalyFeatureSet: this.getAnomalyFeatureSet(),
      rankingMethod: this.getRankingMethod(),
      colorBy: this.refs.colorBy.value,
      sideSheet: this.refs.sideSheetTabs?.dataset.activeSheet || "individuals",
      tableMode: this.refs.tableMode?.value || "fixes",
      tableSort: this.refs.tableSort?.value || "track_time",
      tableDescending: this.refs.tableSortDirection?.dataset.direction === "desc",
      tableFilter: this.refs.tableFilter?.value || "",
      sidePaneWidthPx: this.sidePaneWidthPx,
      individualListHeightPx: this.individualListHeightPx,
      individualQueueListHeightPx: this.individualQueueListHeightPx,
      individualViewMode: this.individualReviewQueue?.mode || "browse",
      individualQueueOrder: this.individualReviewQueue?.orderMode || "dataset",
      individualQueuePage: this.individualReviewQueue?.pageIndex || 0,
    };
    localStorage.setItem(MOVEMENT_APP_CONFIG.storageKey, JSON.stringify(this.uiState));
  }

  getUser() {
    return String(window.vibecleaningActor?.display_name || "");
  }

  getAnomalyFeatureSet() {
    if (!MOVEMENT_APP_CONFIG.osmDerivedFeatures) {
      return "movement_only";
    }
    if (!this.hasOsmContextFeatures()) {
      return "movement_only";
    }
    const value = String(this.refs?.anomalyFeatureSet?.value || this.uiState.anomalyFeatureSet || "movement_only").trim();
    return value === "movement_plus_context" ? "movement_plus_context" : "movement_only";
  }

  anomalyFeatureSetLabel(featureSet = this.getAnomalyFeatureSet()) {
    return featureSet === "movement_plus_context" ? "movement + OSM context" : "movement only";
  }

  getRankingMethod() {
    const value = String(this.refs?.rankingMethod?.value || this.uiState.rankingMethod || "isolation_forest");
    if (value === "isolation_forest_decision_margin") {
      return value;
    }
    if (MOVEMENT_APP_CONFIG.rdsSource && value === "source_is_outlier") {
      return value;
    }
    return "isolation_forest";
  }

  rankingMethodLabel(method = this.getRankingMethod()) {
    if (method === "source_is_outlier") {
      return "source is_outlier — total flagged fixes";
    }
    if (method === "isolation_forest_decision_margin") {
      return `isolation forest — total decision margin (${this.anomalyFeatureSetLabel()})`;
    }
    return `isolation forest — worst burst (${this.anomalyFeatureSetLabel()})`;
  }

  rankingMethodOptions() {
    const options = [
      ["isolation_forest", "Isolation forest — worst burst"],
      ["isolation_forest_decision_margin", "Isolation forest — total decision margin"],
    ];
    if (MOVEMENT_APP_CONFIG.rdsSource) {
      options.push(["source_is_outlier", "Source is_outlier — total flagged fixes"]);
    }
    return options;
  }

  syncIndividualQueueRankingOptions() {
    if (!this.refs?.individualQueueOrder) {
      return;
    }
    const selectedValue = this.individualReviewQueue?.orderMode === "ranking"
      ? String(this.individualReviewQueue.rankingMethod || this.getRankingMethod())
      : "dataset";
    this.refs.individualQueueOrder.innerHTML = [
      ["dataset", "Dataset order"],
      ...this.rankingMethodOptions(),
    ].map(([value, label]) => `<option value="${value}">${label}</option>`).join("");
    this.refs.individualQueueOrder.value = selectedValue;
    if (!this.refs.individualQueueOrder.value) {
      this.refs.individualQueueOrder.value = "dataset";
    }
  }

  syncRankingMethodControl() {
    if (!this.refs?.rankingMethod) {
      return;
    }
    const requestedMethod = String(
      this.refs.rankingMethod.value || this.uiState.rankingMethod || "isolation_forest",
    );
    const options = this.rankingMethodOptions();
    this.refs.rankingMethod.innerHTML = options
      .map(([value, label]) => `<option value="${value}">${label}</option>`)
      .join("");
    const supported = new Set(options.map(([value]) => value));
    this.refs.rankingMethod.value = supported.has(requestedMethod)
      ? requestedMethod
      : "isolation_forest";
    this.refs.runAnomalyRanking.textContent = "Rank bursts";
    this.syncIndividualQueueRankingOptions();
  }

  handleRankingMethodChange() {
    this.syncRankingMethodControl();
    this.individualReviewQueue.rankingMethod = this.getRankingMethod();
    this.saveUiState();
    this.clearAnomalyRanking();
    if (this.data) {
      void this.restoreSavedAnalyses();
    }
  }

  hasOsmContextFeatures() {
    return Boolean(
      this.data?.colorFields?.some(field => {
        const key = String(field?.key || "").toLowerCase();
        return key.startsWith("osm:") && key.endsWith("_distance_m");
      }),
    );
  }

  syncAnomalyFeatureSetOptions({ save = true } = {}) {
    if (!this.refs?.anomalyFeatureSet) {
      return;
    }
    const select = this.refs.anomalyFeatureSet;
    const hasOsmContext = MOVEMENT_APP_CONFIG.osmDerivedFeatures && this.hasOsmContextFeatures();
    const previousValue = select.value || this.uiState.anomalyFeatureSet || "movement_only";
    select.innerHTML = `
      <option value="movement_only">Movement only</option>
      ${hasOsmContext ? '<option value="movement_plus_context">Movement + OSM context</option>' : ""}
    `;
    select.value = hasOsmContext && previousValue === "movement_plus_context"
      ? "movement_plus_context"
      : "movement_only";
    if (save) {
      this.saveUiState();
    }
  }

  setUser(user) {
    // Attribution comes from the authenticated session; browser-supplied names are ignored.
  }

  getBurstGapSeconds() {
    const value = Number(this.refs?.burstGapSeconds?.value ?? this.uiState.burstGapSeconds);
    return Number.isFinite(value) && value > 0 ? value : DEFAULT_BURST_GAP_SECONDS;
  }

  getBurstGapMode() {
    const value = String(this.refs?.burstGapMode?.value ?? this.uiState.burstGapMode ?? DEFAULT_BURST_GAP_MODE).trim().toLowerCase();
    return value === "manual" || value === "quantile" ? value : DEFAULT_BURST_GAP_MODE;
  }

  getBurstGapQuantile() {
    const value = Number(this.refs?.burstGapQuantile?.value ?? this.uiState.burstGapQuantile);
    return Number.isFinite(value) && value > 0 && value <= 1 ? value : DEFAULT_BURST_GAP_QUANTILE;
  }

  syncBurstGapControls() {
    if (
      !this.refs?.burstGapMode
      || !this.refs?.burstGapQuantile
      || !this.refs?.burstGapQuantileControl
      || !this.refs?.burstGapSeconds
      || !this.refs?.burstGapSecondsControl
    ) {
      return;
    }
    if (MOVEMENT_APP_CONFIG.rdsSource) {
      this.refs.burstDefinitionControl.hidden = true;
      this.refs.burstGapQuantileControl.hidden = true;
      this.refs.burstGapSecondsControl.hidden = true;
      this.refs.burstGapMode.disabled = true;
      this.refs.burstGapQuantile.disabled = true;
      this.refs.burstGapSeconds.disabled = true;
      return;
    }
    const mode = this.getBurstGapMode();
    this.refs.burstGapMode.value = mode;
    this.refs.burstGapQuantileControl.hidden = mode !== "quantile";
    this.refs.burstGapQuantile.disabled = mode !== "quantile";
    this.refs.burstGapSecondsControl.hidden = mode !== "manual";
    this.refs.burstGapSeconds.disabled = mode !== "manual";
  }

  handleBurstGapSettingsChange() {
    this.refs.burstGapSeconds.value = String(this.getBurstGapSeconds());
    this.refs.burstGapQuantile.value = String(this.getBurstGapQuantile());
    this.syncBurstGapControls();
    this.saveUiState();
    if (this.currentArtifact) {
      void this.loadArtifact(this.captureDatasetViewContext());
    }
  }

  burstGapLabel() {
    return formatBurstGapMetadata(this.data?.burstGap);
  }

  renderBurstCountIndicator(message = "") {
    if (!this.refs?.burstCount) {
      return;
    }
    if (message) {
      this.refs.burstCount.textContent = message;
      return;
    }
    if (!this.data) {
      this.refs.burstCount.textContent = "No bursts loaded";
      return;
    }

    const total = this.data.autoBursts.length;
    const visible = this.getVisibleAutoBursts({ requireOverlay: true }).length;
    const noun = total === 1 ? "burst" : "bursts";
    const scope = this.data.overviewHasAllFixes ? "generated" : "loaded";
    let text = `${formatCount(total)} ${noun} ${scope}`;
    if (visible !== total) {
      text += `, ${formatCount(visible)} visible`;
    }
    const gapLabel = this.burstGapLabel();
    if (gapLabel) {
      text += ` at ${gapLabel}`;
    }
    if (this.data.detailState === "loading") {
      text += " (loading)";
    } else if (this.data.autoBurstsTruncated && total === 0) {
      text = "Select individuals to count bursts";
    }
    this.refs.burstCount.textContent = text;
  }

  normalizeIndividualSearchQuery(value) {
    return String(value || "").trim().toLowerCase();
  }

  getFilteredIndividuals() {
    if (!this.data) {
      return [];
    }
    const query = this.normalizeIndividualSearchQuery(this.individualSearchQuery);
    if (!query) {
      return [...this.data.individuals];
    }
    return this.data.individuals.filter(individual => individual.toLowerCase().includes(query));
  }

  isStackedSideLayout() {
    if (MOVEMENT_APP_CONFIG.mode === "slim_movement") {
      return false;
    }
    return window.matchMedia?.(`(max-width: ${STACKED_SIDE_LAYOUT_BREAKPOINT_PX}px)`)?.matches === true;
  }

  getSidePaneWidthBounds() {
    const mainWidth = Math.max(0, this.refs?.main?.clientWidth || 0);
    if (!mainWidth) {
      return {
        min: MIN_SIDE_PANE_WIDTH_PX,
        max: Math.max(MIN_SIDE_PANE_WIDTH_PX, DEFAULT_SIDE_PANE_WIDTH_PX),
      };
    }
    const maxByRatio = Math.floor(mainWidth * MAX_SIDE_PANE_WIDTH_RATIO);
    const max = Math.max(MIN_SIDE_PANE_WIDTH_PX, maxByRatio - SIDE_PANE_HANDLE_WIDTH_PX);
    return {
      min: MIN_SIDE_PANE_WIDTH_PX,
      max,
    };
  }

  clampSidePaneWidth(width) {
    const numericWidth = Number(width);
    const fallback = this.sidePaneWidthPx || DEFAULT_SIDE_PANE_WIDTH_PX;
    const requested = Number.isFinite(numericWidth) ? numericWidth : fallback;
    const bounds = this.getSidePaneWidthBounds();
    return Math.round(Math.min(bounds.max, Math.max(bounds.min, requested)));
  }

  applySidePaneWidth(width, { save = true, resizeMap = true } = {}) {
    const nextWidth = this.clampSidePaneWidth(width);
    this.sidePaneWidthPx = nextWidth;
    if (this.refs?.main) {
      if (this.isStackedSideLayout()) {
        this.refs.main.style.removeProperty("--movement-side-width");
      } else {
        this.refs.main.style.setProperty("--movement-side-width", `${nextWidth}px`);
      }
    }
    if (this.refs?.sideResize) {
      this.refs.sideResize.setAttribute("aria-valuenow", String(nextWidth));
    }
    if (save && this.refs) {
      this.saveUiState();
    }
    if (resizeMap && this.map) {
      window.requestAnimationFrame(() => {
        if (!this.map) {
          return;
        }
        try {
          this.map.resize();
        } catch {}
        this.renderLayers();
      });
    }
  }

  handleLayoutResize() {
    this.applySidePaneWidth(this.sidePaneWidthPx, { save: false });
    const listHeight = this.currentIndividualListHeight();
    if (listHeight !== null) {
      this.applyIndividualListHeight(listHeight, { save: false });
    }
  }

  currentIndividualListHeight() {
    return this.individualReviewQueue.mode === "queue"
      ? this.individualQueueListHeightPx
      : this.individualListHeightPx;
  }

  beginSidePaneResize(event) {
    if (this.isStackedSideLayout()) {
      return;
    }
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    this.sidePaneResize.active = true;
    this.sidePaneResize.pointerId = event.pointerId;
    this.refs.main?.classList.add("is-resizing");
    this.refs.sideResize?.setPointerCapture?.(event.pointerId);
    window.addEventListener("pointermove", this.handleSidePanePointerMove);
    window.addEventListener("pointerup", this.handleSidePanePointerUp);
    window.addEventListener("pointercancel", this.handleSidePanePointerUp);
  }

  onSidePanePointerMove(event) {
    if (!this.sidePaneResize.active || event.pointerId !== this.sidePaneResize.pointerId || !this.refs?.main) {
      return;
    }
    const rect = this.refs.main.getBoundingClientRect();
    if (!rect.width) {
      return;
    }
    const nextWidth = rect.right - event.clientX;
    this.applySidePaneWidth(nextWidth, { save: false });
  }

  onSidePanePointerUp(event) {
    if (!this.sidePaneResize.active || event.pointerId !== this.sidePaneResize.pointerId) {
      return;
    }
    this.sidePaneResize.active = false;
    this.sidePaneResize.pointerId = null;
    this.refs.main?.classList.remove("is-resizing");
    this.refs.sideResize?.releasePointerCapture?.(event.pointerId);
    window.removeEventListener("pointermove", this.handleSidePanePointerMove);
    window.removeEventListener("pointerup", this.handleSidePanePointerUp);
    window.removeEventListener("pointercancel", this.handleSidePanePointerUp);
    this.applySidePaneWidth(this.sidePaneWidthPx, { save: true });
  }

  getIndividualListHeightBounds() {
    const sheet = this.refs?.sideSheetIndividuals;
    const list = this.refs?.individuals;
    const resize = this.refs?.individualResize;
    const fixHead = this.refs?.fixHead;
    if (!sheet || !list) {
      return null;
    }
    const sheetRect = sheet.getBoundingClientRect();
    const listRect = list.getBoundingClientRect();
    if (
      sheet.classList.contains("hidden")
      || sheetRect.height <= 0
      || listRect.height <= 0
    ) {
      return null;
    }
    if (this.individualReviewQueue.mode === "queue") {
      const controls = this.refs?.individualQueueControls;
      const controlsRect = controls?.getBoundingClientRect();
      if (!controlsRect?.height) {
        return null;
      }
      const availableHeight = sheetRect.bottom
        - controlsRect.top
        - (resize?.offsetHeight || 0);
      return {
        mode: "queue",
        min: MIN_INDIVIDUAL_LIST_HEIGHT_PX,
        max: Math.max(
          MIN_INDIVIDUAL_LIST_HEIGHT_PX,
          Math.floor(availableHeight - MIN_QUEUE_CONTROLS_HEIGHT_PX),
        ),
        available: Math.floor(availableHeight),
      };
    }
    const availableHeight = sheetRect.bottom
      - listRect.top
      - (resize?.offsetHeight || 0)
      - (fixHead?.offsetHeight || 0);
    return {
      mode: "browse",
      min: MIN_INDIVIDUAL_LIST_HEIGHT_PX,
      max: Math.max(
        MIN_INDIVIDUAL_LIST_HEIGHT_PX,
        Math.floor(availableHeight - MIN_CHECKED_FIXES_HEIGHT_PX),
      ),
      available: Math.floor(availableHeight),
    };
  }

  applyIndividualListHeight(height, { save = true } = {}) {
    if (!this.refs?.sideSheetIndividuals || !this.refs?.individuals) {
      return;
    }
    const numericHeight = Number(height);
    const fallback = this.refs.individuals.getBoundingClientRect().height || MIN_INDIVIDUAL_LIST_HEIGHT_PX;
    const requested = Number.isFinite(numericHeight) ? numericHeight : fallback;
    const bounds = this.getIndividualListHeightBounds();
    if (!bounds) {
      return;
    }
    const nextHeight = Math.round(Math.min(bounds.max, Math.max(bounds.min, requested)));
    if (bounds.mode === "queue") {
      this.individualQueueListHeightPx = nextHeight;
      this.refs.sideSheetIndividuals.style.setProperty(
        "--movement-queue-list-height",
        `${nextHeight}px`,
      );
      this.refs.individualResize?.setAttribute("aria-valuenow", String(nextHeight));
      this.refs.individualResize?.setAttribute("aria-valuemax", String(bounds.max));
      if (save) {
        this.saveUiState();
      }
      return;
    }
    const checkedFixesHeight = Math.max(
      MIN_CHECKED_FIXES_HEIGHT_PX,
      bounds.available - nextHeight,
    );
    this.individualListHeightPx = nextHeight;
    this.refs.sideSheetIndividuals.style.setProperty("--movement-individual-list-height", `${nextHeight}px`);
    this.refs.sideSheetIndividuals.style.setProperty("--movement-checked-fixes-height", `${checkedFixesHeight}px`);
    this.refs.individualResize?.setAttribute("aria-valuenow", String(nextHeight));
    this.refs.individualResize?.setAttribute("aria-valuemax", String(bounds.max));
    if (save) {
      this.saveUiState();
    }
  }

  beginIndividualPaneResize(event) {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    this.individualPaneResize.active = true;
    this.individualPaneResize.pointerId = event.pointerId;
    this.refs.sideSheetIndividuals?.classList.add("is-resizing");
    this.refs.individualResize?.setPointerCapture?.(event.pointerId);
    window.addEventListener("pointermove", this.handleIndividualPanePointerMove);
    window.addEventListener("pointerup", this.handleIndividualPanePointerUp);
    window.addEventListener("pointercancel", this.handleIndividualPanePointerUp);
  }

  onIndividualPanePointerMove(event) {
    if (
      !this.individualPaneResize.active
      || event.pointerId !== this.individualPaneResize.pointerId
      || !this.refs?.individuals
    ) {
      return;
    }
    if (this.individualReviewQueue.mode === "queue") {
      const sheetRect = this.refs.sideSheetIndividuals.getBoundingClientRect();
      this.applyIndividualListHeight(sheetRect.bottom - event.clientY, { save: false });
      return;
    }
    const listRect = this.refs.individuals.getBoundingClientRect();
    this.applyIndividualListHeight(event.clientY - listRect.top, { save: false });
  }

  onIndividualPanePointerUp(event) {
    if (
      !this.individualPaneResize.active
      || event.pointerId !== this.individualPaneResize.pointerId
    ) {
      return;
    }
    this.individualPaneResize.active = false;
    this.individualPaneResize.pointerId = null;
    this.refs.sideSheetIndividuals?.classList.remove("is-resizing");
    this.refs.individualResize?.releasePointerCapture?.(event.pointerId);
    window.removeEventListener("pointermove", this.handleIndividualPanePointerMove);
    window.removeEventListener("pointerup", this.handleIndividualPanePointerUp);
    window.removeEventListener("pointercancel", this.handleIndividualPanePointerUp);
    this.applyIndividualListHeight(this.currentIndividualListHeight(), { save: true });
  }

  resizeIndividualPaneFromKeyboard(event) {
    if (!["ArrowUp", "ArrowDown", "Home", "End"].includes(event.key)) {
      return;
    }
    event.preventDefault();
    const bounds = this.getIndividualListHeightBounds();
    if (!bounds) {
      return;
    }
    const queueMode = this.individualReviewQueue.mode === "queue";
    const current = this.currentIndividualListHeight()
      ?? this.refs.individuals?.getBoundingClientRect().height
      ?? bounds.min;
    const step = event.shiftKey ? 40 : 10;
    let nextHeight = current;
    if (event.key === "Home") {
      nextHeight = bounds.min;
    } else if (event.key === "End") {
      nextHeight = bounds.max;
    } else if (queueMode) {
      nextHeight = current + (event.key === "ArrowUp" ? step : -step);
    } else {
      nextHeight = current + (event.key === "ArrowDown" ? step : -step);
    }
    this.applyIndividualListHeight(nextHeight, { save: true });
  }

  setSideSheet(sheet, { save = true } = {}) {
    const enabledSheets = MOVEMENT_APP_CONFIG.featureSpace
      ? ["table", "ranking", "feature_space"]
      : ["table", "ranking"];
    const nextSheet = enabledSheets.includes(sheet) ? sheet : "individuals";
    if (this.individualReviewQueue?.mode === "browse") {
      this.individualReviewQueue.browseSideSheet = nextSheet;
    }
    if (this.refs?.sideSheetTabs) {
      this.refs.sideSheetTabs.dataset.activeSheet = nextSheet;
    }
    if (this.refs?.sideTabIndividuals) {
      this.refs.sideTabIndividuals.classList.toggle("is-active", nextSheet === "individuals");
    }
    if (this.refs?.sideTabTable) {
      this.refs.sideTabTable.classList.toggle("is-active", nextSheet === "table");
    }
    if (this.refs?.sideTabRanking) {
      this.refs.sideTabRanking.classList.toggle("is-active", nextSheet === "ranking");
    }
    if (this.refs?.sideTabFeatureSpace) {
      this.refs.sideTabFeatureSpace.classList.toggle("is-active", nextSheet === "feature_space");
    }
    if (this.refs?.sideSheetIndividuals) {
      this.refs.sideSheetIndividuals.classList.toggle("hidden", nextSheet !== "individuals");
    }
    if (this.refs?.sideSheetTable) {
      this.refs.sideSheetTable.classList.toggle("hidden", nextSheet !== "table");
    }
    if (this.refs?.sideSheetRanking) {
      this.refs.sideSheetRanking.classList.toggle("hidden", nextSheet !== "ranking");
    }
    if (this.refs?.sideSheetFeatureSpace) {
      this.refs.sideSheetFeatureSpace.classList.toggle("hidden", nextSheet !== "feature_space");
    }
    if (save && this.refs) {
      this.saveUiState();
    }
    if (nextSheet === "table") {
      this.renderTableSheet();
    } else if (
      nextSheet === "ranking"
      && this.anomalyRanking?.status === "available"
    ) {
      void this.loadSavedAnomalyRanking();
    } else if (nextSheet === "feature_space") {
      this.renderBurstFeatureSpace();
    } else if (nextSheet === "individuals" && this.currentIndividualListHeight() !== null) {
      this.applyIndividualListHeight(this.currentIndividualListHeight(), { save: false });
    }
    if (this.map) {
      window.requestAnimationFrame(() => {
        if (!this.map) {
          return;
        }
        try {
          this.map.resize();
        } catch {}
        this.renderLayers();
      });
    }
  }

  applyAppProfile() {
    if (this.refs.rankingMethodControl) {
      this.refs.rankingMethodControl.hidden = false;
    }
    if (MOVEMENT_APP_CONFIG.mode === "movement") {
      return;
    }
    const isSlim = MOVEMENT_APP_CONFIG.mode === "slim_movement";
    this.mountEl.querySelector(".movement-root")?.classList.add(isSlim ? "is-slim" : "is-rds");
    const hiddenElements = isSlim
      ? [
          this.refs.familyControl,
          this.refs.artifactControl,
          this.refs.showTrainControl,
          this.refs.showTestControl,
          this.refs.candidateQueryControl,
          this.refs.checkCandidates,
          this.refs.clearCandidates,
          this.refs.anomalyFeatureSetControl,
          this.refs.runBurstFeatureSpace,
          this.refs.sideTabFeatureSpace,
          this.refs.sideSheetFeatureSpace,
        ]
      : [
          this.refs.familyControl,
          this.refs.artifactControl,
          this.refs.showTrainControl,
          this.refs.showTestControl,
          this.refs.burstDefinitionControl,
          this.refs.burstGapQuantileControl,
          this.refs.burstGapSecondsControl,
          this.refs.candidateQueryControl,
          this.refs.checkCandidates,
          this.refs.clearCandidates,
          this.refs.anomalyFeatureSetControl,
          this.refs.runBurstFeatureSpace,
          this.refs.sideTabFeatureSpace,
          this.refs.sideSheetFeatureSpace,
        ];
    for (const element of hiddenElements) {
      if (element) {
        element.hidden = true;
        element.classList.add("movement-profile-hidden");
      }
    }
    if (MOVEMENT_APP_CONFIG.rdsSource) {
      this.refs.exportReviewedCsv.textContent = "Export reviewed RDS ZIP";
    }
    this.refs.sideTabRanking.textContent = "Ranking";
    const overlayTitle = this.mountEl.querySelector(".movement-overlay-card h3");
    const overlayDescription = this.mountEl.querySelector(".movement-overlay-card p");
    if (overlayTitle) {
      overlayTitle.textContent = isSlim ? "Slim Movement Review" : "RDS Movement Review";
    }
    if (overlayDescription) {
      overlayDescription.textContent = isSlim
        ? "Visualize raw movement data, review fixes and bursts, export flags, and generate reports."
        : "Explore an indexed RDS study, review source bursts, rank outliers, and export reviewed RDS files.";
    }
  }

  renderShell() {
    this.mountEl.innerHTML = `
      <style>
        .movement-root {
          display: grid;
          grid-template-rows: auto auto auto auto minmax(0, 1fr);
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
          grid-row: 1;
        }
        .movement-status {
          grid-row: 3;
          min-width: 0;
        }
        .movement-release-notice {
          grid-row: 2;
          margin: 8px 16px 0;
          padding: 9px 11px;
          border: 1px solid rgba(116, 212, 255, 0.34);
          border-radius: 8px;
          background: rgba(19, 72, 96, 0.34);
          color: #d9f4ff;
          font-size: 12px;
        }
        .movement-release-notice button {
          margin-left: 8px;
          padding: 4px 7px;
        }
        .movement-output-links {
          grid-row: 4;
        }
        .movement-main {
          grid-row: 5;
        }
        .movement-root .movement-profile-hidden,
        .movement-root [hidden] {
          display: none !important;
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
        .movement-root.is-slim .movement-toolbar {
          gap: 8px 10px;
          padding-top: 10px;
        }
        .movement-root.is-slim .movement-anomaly-meta + .movement-anomaly-meta {
          display: none;
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
        .movement-toolbar input,
        .movement-toolbar button,
        .movement-modal select,
        .movement-modal input,
        .movement-modal textarea {
          font: inherit;
        }
        .movement-toolbar select,
        .movement-toolbar input,
        .movement-modal select,
        .movement-modal input,
        .movement-modal textarea {
          min-width: 150px;
          padding: 8px 10px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
        }
        .movement-toolbar input[type="number"] {
          min-width: 88px;
          width: 96px;
        }
        .movement-burst-gap-control {
          gap: 8px;
        }
        .movement-burst-gap-control input[data-role="burst-gap-quantile"] {
          width: 82px;
        }
        .movement-burst-count {
          display: inline-flex;
          align-items: center;
          min-height: 28px;
          padding: 4px 8px;
          border-radius: 999px;
          border: 1px solid rgba(125, 211, 252, 0.22);
          background: rgba(125, 211, 252, 0.08);
          color: #c9e7f6;
          font-size: 11px;
          white-space: nowrap;
        }
        .movement-candidate-query-control {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          flex-wrap: wrap;
          padding: 6px 8px;
          border-radius: 12px;
          border: 1px solid rgba(125, 211, 252, 0.12);
          background: rgba(15, 23, 42, 0.38);
        }
        .movement-candidate-query-control select {
          min-width: 220px;
        }
        .movement-candidate-query-meta {
          flex: 1 1 280px;
          min-width: min(100%, 280px);
          max-width: 520px;
          color: #9bb0c6;
          font-size: 11px;
          line-height: 1.35;
        }
        .movement-candidate-query-meta strong {
          color: #dbeafe;
          font-weight: 600;
        }
        .movement-candidate-query-params {
          display: inline-flex;
          align-items: center;
          gap: 8px;
          flex-wrap: wrap;
        }
        .movement-candidate-query-params.hidden {
          display: none;
        }
        .movement-candidate-query-params label {
          color: #b9cadb;
        }
        .movement-candidate-query-params input[type="checkbox"] {
          min-width: auto;
          width: auto;
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
        .movement-output-links {
          display: flex;
          gap: 10px;
          min-height: 0;
          padding: 0 16px 10px;
          font-size: 12px;
        }
        .movement-output-links:empty {
          display: none;
          padding: 0;
        }
        .movement-output-links a {
          color: #9df6dc;
        }
        .movement-edit-lock {
          display: inline-flex;
          align-items: center;
          gap: 7px;
          max-width: min(460px, 42vw);
          padding: 5px 7px;
          border: 1px solid rgba(251, 191, 36, 0.28);
          border-radius: 9px;
          background: rgba(120, 53, 15, 0.3);
          color: #fde68a;
          font-size: 11px;
          line-height: 1.25;
        }
        .movement-edit-lock.hidden {
          display: none;
        }
        .movement-edit-lock-badge {
          flex: 0 0 auto;
          font-weight: 700;
          color: #fef3c7;
        }
        .movement-edit-lock-message {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .movement-edit-lock button {
          flex: 0 0 auto;
          padding: 4px 7px;
          border: 1px solid rgba(251, 191, 36, 0.32);
          border-radius: 9px;
          background: rgba(217, 119, 6, 0.22);
          color: #fef3c7;
          cursor: pointer;
        }
        .movement-main {
          --movement-side-width: 420px;
          display: grid;
          grid-template-columns: minmax(0, 1fr) ${SIDE_PANE_HANDLE_WIDTH_PX}px minmax(${MIN_SIDE_PANE_WIDTH_PX}px, var(--movement-side-width));
          gap: 0;
          height: 100%;
          min-height: 0;
          overflow: hidden;
          padding: 0 16px 14px;
        }
        .movement-map-wrap,
        .movement-side {
          min-width: 0;
          min-height: 0;
          border-radius: 16px;
          overflow: hidden;
          border: 1px solid rgba(255, 255, 255, 0.07);
          background: rgba(255, 255, 255, 0.03);
        }
        .movement-main.is-resizing,
        .movement-main.is-resizing * {
          cursor: col-resize !important;
          user-select: none;
        }
        .movement-map-wrap {
          position: relative;
          background: #08111b;
        }
        .movement-side-resize {
          position: relative;
          display: flex;
          align-items: stretch;
          justify-content: center;
          width: 100%;
          min-height: 0;
          cursor: col-resize;
          touch-action: none;
        }
        .movement-side-resize::before {
          content: "";
          width: 4px;
          margin: 8px 0;
          border-radius: 999px;
          background: linear-gradient(180deg, rgba(87, 218, 174, 0.2), rgba(125, 211, 252, 0.48), rgba(87, 218, 174, 0.2));
          box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.04);
        }
        .movement-side-resize:hover::before,
        .movement-main.is-resizing .movement-side-resize::before {
          background: linear-gradient(180deg, rgba(87, 218, 174, 0.38), rgba(125, 211, 252, 0.8), rgba(87, 218, 174, 0.38));
        }
        .movement-map {
          width: 100%;
          height: 100%;
        }
        .movement-map-attribution {
          position: absolute;
          left: 16px;
          top: 16px;
          z-index: 4;
          max-width: min(300px, calc(100% - 88px));
          padding: 6px 9px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(7, 11, 22, 0.82);
          box-shadow: 0 12px 28px rgba(0, 0, 0, 0.24);
          color: #b8c8d8;
          font-size: 11px;
          line-height: 1.35;
        }
        .movement-map-attribution.hidden {
          display: none;
        }
        .movement-map-attribution a {
          color: #d6ecff;
          text-decoration: underline;
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
        .movement-fix-popup {
          position: absolute;
          z-index: 5;
          width: min(320px, calc(100% - 24px));
          display: grid;
          gap: 10px;
          padding: 12px 13px;
          border-radius: 14px;
          border: 1px solid rgba(255, 255, 255, 0.1);
          background: rgba(7, 11, 22, 0.94);
          box-shadow: 0 20px 46px rgba(0, 0, 0, 0.36);
          color: #e8eef7;
          pointer-events: auto;
        }
        .movement-fix-popup.hidden {
          display: none;
        }
        .movement-fix-popup-head {
          display: flex;
          align-items: start;
          justify-content: space-between;
          gap: 10px;
        }
        .movement-fix-popup-title {
          font-size: 12px;
          font-weight: 600;
          color: #eef4fb;
        }
        .movement-fix-popup-subtitle {
          font-size: 11px;
          color: #8fa5bc;
        }
        .movement-fix-popup-close {
          flex: 0 0 auto;
          width: 24px;
          height: 24px;
          padding: 0;
          border: none;
          border-radius: 999px;
          background: rgba(255, 255, 255, 0.08);
          color: #e8eef7;
          cursor: pointer;
          font: inherit;
          font-size: 14px;
          line-height: 1;
        }
        .movement-fix-popup-fields {
          display: grid;
          gap: 7px;
        }
        .movement-fix-popup-row {
          display: grid;
          grid-template-columns: minmax(78px, auto) minmax(0, 1fr);
          gap: 8px;
          align-items: start;
          font-size: 11px;
        }
        .movement-fix-popup-label {
          color: #8fa5bc;
          white-space: nowrap;
        }
        .movement-fix-popup-value {
          color: #eef4fb;
          overflow-wrap: anywhere;
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
          grid-template-rows: auto auto minmax(0, 1fr) auto;
          min-height: 0;
        }
        .movement-individual-view-tabs {
          display: flex;
          gap: 8px;
          padding: 10px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.07);
          background: rgba(6, 13, 22, 0.72);
        }
        .movement-individual-view-tabs button {
          flex: 1;
        }
        .movement-side-tabs {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          padding: 12px 14px 10px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }
        .movement-side-tabs.hidden,
        .movement-queue-controls.hidden {
          display: none;
        }
        .movement-side-tab {
          padding: 7px 12px;
          border-radius: 999px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(255, 255, 255, 0.04);
          color: #d5e1ee;
          cursor: pointer;
          font: inherit;
          font-size: 12px;
        }
        .movement-side-tab.is-active {
          background: rgba(67, 206, 162, 0.22);
          border-color: rgba(67, 206, 162, 0.34);
          color: #d8fff3;
        }
        .movement-side-content {
          min-width: 0;
          min-height: 0;
          overflow: hidden;
        }
        .movement-side-sheet {
          display: grid;
          min-width: 0;
          min-height: 0;
          height: 100%;
          overflow: hidden;
        }
        .movement-side-sheet.hidden {
          display: none;
        }
        .movement-side-sheet.individuals {
          grid-template-rows: auto auto minmax(80px, var(--movement-individual-list-height, 1fr)) 10px auto minmax(${MIN_CHECKED_FIXES_HEIGHT_PX}px, var(--movement-checked-fixes-height, 0.75fr));
          align-content: start;
        }
        .movement-side-sheet.individuals.queue-mode {
          grid-template-rows: auto minmax(${MIN_QUEUE_CONTROLS_HEIGHT_PX}px, 1fr) 10px minmax(${MIN_INDIVIDUAL_LIST_HEIGHT_PX}px, var(--movement-queue-list-height, 1fr));
        }
        .movement-side-sheet.individuals.queue-mode .movement-side-search {
          grid-row: 2;
          min-height: 0;
          overflow-y: auto;
        }
        .movement-side-sheet.individuals.queue-mode .movement-individual-resize {
          grid-row: 3;
        }
        .movement-side-sheet.individuals.queue-mode [data-role="individuals"] {
          grid-row: 4;
        }
        .movement-side-sheet.individuals.queue-mode [data-role="fix-head"],
        .movement-side-sheet.individuals.queue-mode [data-role="selected-fixes"] {
          display: none;
        }
        .movement-individual-resize {
          position: relative;
          cursor: row-resize;
          touch-action: none;
          background: rgba(255, 255, 255, 0.025);
          border-top: 1px solid rgba(255, 255, 255, 0.04);
          border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        }
        .movement-individual-resize::before {
          content: "";
          position: absolute;
          top: 3px;
          left: 50%;
          width: 48px;
          height: 3px;
          border-radius: 999px;
          transform: translateX(-50%);
          background: rgba(148, 163, 184, 0.58);
        }
        .movement-individual-resize:hover::before,
        .movement-individual-resize:focus-visible::before,
        .movement-side-sheet.individuals.is-resizing .movement-individual-resize::before {
          background: #7dd3fc;
        }
        .movement-individual-resize:focus-visible {
          outline: 2px solid rgba(125, 211, 252, 0.72);
          outline-offset: -2px;
        }
        .movement-side-sheet.individuals.is-resizing,
        .movement-side-sheet.individuals.is-resizing * {
          cursor: row-resize !important;
          user-select: none !important;
        }
        .movement-side-sheet.table {
          grid-template-rows: auto auto minmax(0, 1fr);
        }
        .movement-side-sheet.feature-space {
          grid-template-rows: auto minmax(0, 1fr);
        }
        .movement-side-sheet.ranking {
          grid-template-rows: auto minmax(0, 1fr);
        }
        .movement-side-head,
        .movement-slider-row {
          padding: 12px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-ranking-head {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          justify-content: space-between;
          gap: 8px 12px;
        }
        .movement-ranking-head label {
          display: inline-flex;
          align-items: center;
          gap: 6px;
        }
        .movement-ranking-head select {
          min-width: 158px;
          padding: 6px 8px;
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 8px;
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
          font: inherit;
        }
        .movement-individuals,
        .movement-fixes,
        .movement-anomaly-ranking {
          overflow-y: auto;
          padding: 10px 12px;
        }
        .movement-anomaly-ranking {
          min-height: 0;
          overflow-x: auto;
        }
        .movement-anomaly-meta,
        .movement-anomaly-warnings {
          display: grid;
          gap: 5px;
          margin-bottom: 10px;
          color: #95a8bb;
          font-size: 11px;
          line-height: 1.4;
        }
        .movement-anomaly-warning {
          color: #f6cf86;
        }
        .movement-anomaly-bursts {
          display: grid;
          gap: 6px;
          margin: 8px 0 2px;
        }
        .movement-anomaly-burst {
          display: grid;
          gap: 7px;
          padding: 8px 9px;
          border-radius: 10px;
          border: 1px solid rgba(125, 211, 252, 0.12);
          background: rgba(15, 23, 42, 0.48);
        }
        .movement-anomaly-burst.is-ranking-burst {
          border-color: rgba(250, 204, 21, 0.35);
          background: rgba(250, 204, 21, 0.08);
        }
        .movement-anomaly-burst-main,
        .movement-anomaly-burst-actions {
          display: flex;
          align-items: center;
          gap: 8px;
          flex-wrap: wrap;
        }
        .movement-anomaly-burst-rank-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 16px;
          height: 16px;
          border-radius: 999px;
          background: rgba(250, 204, 21, 0.18);
          color: #fde68a;
          font-size: 11px;
          line-height: 1;
        }
        .movement-anomaly-burst-actions button {
          padding: 4px 7px;
          border-radius: 8px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(255, 255, 255, 0.08);
          color: #e5edf7;
          cursor: pointer;
          font-size: 11px;
        }
        .movement-anomaly-explanation {
          display: grid;
          gap: 6px;
          color: #9bb0c6;
          font-size: 11px;
          line-height: 1.35;
        }
        .movement-anomaly-why {
          color: #c9d8e8;
          font-size: 11px;
          line-height: 1.35;
        }
        .movement-anomaly-why strong {
          color: #e5edf7;
          font-weight: 600;
        }
        .movement-anomaly-explanation-details {
          display: grid;
          gap: 5px;
        }
        .movement-anomaly-explanation-details summary {
          width: fit-content;
          cursor: pointer;
          color: #c9e7f6;
          font-size: 11px;
        }
        .movement-anomaly-explanation-note {
          color: #7f93a8;
        }
        .movement-anomaly-explanation-section {
          display: flex;
          gap: 6px;
          flex-wrap: wrap;
          align-items: center;
        }
        .movement-anomaly-explanation-label {
          color: #dbeafe;
          font-weight: 600;
        }
        .movement-anomaly-explanation-chip {
          display: inline-flex;
          gap: 4px;
          align-items: center;
          padding: 2px 6px;
          border-radius: 999px;
          background: rgba(255, 255, 255, 0.07);
          color: #cbd5e1;
        }
        .movement-anomaly-ranking .movement-table tbody tr {
          cursor: default;
        }
        .movement-feature-space {
          overflow-y: auto;
          padding: 10px 12px;
        }
        .movement-feature-space-meta {
          display: flex;
          flex-wrap: wrap;
          gap: 6px 12px;
          margin-bottom: 10px;
          color: #95a8bb;
          font-size: 11px;
        }
        .movement-feature-space-plot {
          min-height: 280px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 12px;
          background: rgba(7, 12, 22, 0.72);
          overflow: hidden;
        }
        .movement-feature-space-plot svg {
          display: block;
          width: 100%;
          min-height: 280px;
        }
        .movement-feature-space-axis {
          stroke: rgba(148, 163, 184, 0.22);
          stroke-width: 1;
        }
        .movement-feature-space-point {
          fill: rgba(125, 211, 252, 0.62);
          stroke: rgba(15, 23, 42, 0.75);
          stroke-width: 1;
          cursor: pointer;
        }
        .movement-feature-space-point.is-neighbor {
          fill: rgba(250, 204, 21, 0.9);
          stroke: rgba(254, 240, 138, 0.95);
          stroke-width: 1.5;
        }
        .movement-feature-space-point.is-selected {
          fill: rgba(216, 180, 254, 1);
          stroke: rgba(255, 255, 255, 0.98);
          stroke-width: 2.5;
        }
        .movement-feature-space-selection {
          display: grid;
          gap: 8px;
          margin-top: 10px;
          padding: 10px;
          border-radius: 10px;
          background: rgba(255, 255, 255, 0.04);
          color: #cbd5e1;
          font-size: 11px;
        }
        .movement-feature-space-selection-main,
        .movement-feature-space-neighbors {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          gap: 6px 10px;
        }
        .movement-feature-space-neighbors button {
          padding: 4px 7px;
          border-radius: 8px;
          border: 1px solid rgba(250, 204, 21, 0.22);
          background: rgba(250, 204, 21, 0.08);
          color: #fde68a;
          cursor: pointer;
          font-size: 11px;
        }
        .movement-side-search {
          display: grid;
          gap: 8px;
          padding: 12px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
          background: rgba(255, 255, 255, 0.02);
        }
        .movement-side-search label {
          display: grid;
          gap: 6px;
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-side-search input {
          width: 100%;
          min-width: 0;
          padding: 7px 9px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
          font: inherit;
        }
        .movement-queue-nav,
        .movement-queue-map-controls,
        .movement-queue-card-actions {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: 6px;
        }
        .movement-individual-view-tabs button,
        .movement-queue-controls button,
        .movement-queue-controls select,
        .movement-queue-card-actions button,
        .movement-queue-card-comment {
          min-width: 0;
          padding: 6px 8px;
          border: 1px solid rgba(255, 255, 255, 0.09);
          border-radius: 8px;
          background: rgba(15, 23, 42, 0.82);
          color: #dbe7f3;
          font: inherit;
          font-size: 11px;
        }
        .movement-individual-view-tabs button,
        .movement-queue-controls button,
        .movement-queue-card-actions button {
          cursor: pointer;
        }
        .movement-individual-view-tabs button.is-active,
        .movement-queue-map-controls button.is-active {
          border-color: rgba(67, 206, 162, 0.46);
          background: rgba(67, 206, 162, 0.2);
          color: #d8fff3;
        }
        .movement-queue-controls {
          display: grid;
          gap: 7px;
        }
        .movement-queue-order {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr);
          align-items: center;
          gap: 7px;
          color: #9bb0c6;
          font-size: 11px;
        }
        .movement-queue-ranking-state,
        .movement-queue-progress {
          color: #9bb0c6;
          font-size: 11px;
          line-height: 1.4;
        }
        .movement-queue-ranking-state {
          display: flex;
          align-items: center;
          gap: 5px;
          min-width: 0;
          font-size: 9px;
          line-height: 1.15;
        }
        .movement-queue-ranking-copy {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .movement-queue-ranking-state button {
          flex: 0 0 auto;
          padding: 3px 6px;
          font-size: 9px;
        }
        .movement-queue-ranking-state.error {
          color: #f9c98b;
        }
        .movement-queue-card-actions button[data-review-decision="ok"] {
          background: rgba(67, 206, 162, 0.2);
          color: #d8fff3;
        }
        .movement-queue-card-actions button[data-review-decision="fix_keep"],
        .movement-review-needs-check {
          background: rgba(245, 181, 54, 0.16);
          color: #ffe7a6;
        }
        .movement-queue-card-actions button[data-review-decision="remove"] {
          background: rgba(248, 113, 113, 0.16);
          color: #fecaca;
        }
        .movement-review-choice {
          position: relative;
          display: inline-flex;
          align-items: center;
        }
        .movement-review-needs-check {
          display: inline-flex;
          align-items: center;
          gap: 5px;
          padding: 5px 7px;
          border: 1px solid rgba(245, 181, 54, 0.3);
          border-radius: 6px;
          cursor: pointer;
          font-size: 11px;
          font-weight: 700;
        }
        .movement-review-help {
          position: absolute;
          z-index: 20;
          left: 0;
          bottom: calc(100% + 7px);
          width: min(280px, 70vw);
          padding: 8px 9px;
          border: 1px solid rgba(148, 163, 184, 0.5);
          border-radius: 7px;
          background: #172333;
          color: #e8eef7;
          box-shadow: 0 8px 20px rgba(0, 0, 0, 0.34);
          font-size: 11px;
          font-weight: 400;
          line-height: 1.35;
          pointer-events: none;
          opacity: 0;
          visibility: hidden;
          transform: translateY(3px);
          transition: opacity 100ms ease, transform 100ms ease;
        }
        .movement-review-choice:hover .movement-review-help,
        .movement-review-choice:focus-within .movement-review-help {
          opacity: 1;
          visibility: visible;
          transform: translateY(0);
        }
        .movement-prior-decision-badge {
          display: inline-flex;
          width: fit-content;
          margin-top: 7px;
          padding: 4px 7px;
          border-radius: 999px;
          border: 1px solid rgba(148, 163, 184, 0.35);
          color: #c8d4e3;
          background: rgba(148, 163, 184, 0.1);
          font-size: 11px;
          font-weight: 700;
        }
        .movement-prior-decision-badge.issues,
        .movement-prior-decision-badge.needs-check {
          border-color: rgba(255, 193, 77, 0.7);
          color: #ffe1a1;
          background: rgba(255, 193, 77, 0.14);
        }
        .movement-queue-card-actions button:disabled,
        .movement-queue-controls button:disabled {
          cursor: not-allowed;
          opacity: 0.45;
        }
        .movement-card.queue-active {
          border-color: rgba(125, 211, 252, 0.74);
          box-shadow: inset 3px 0 0 #7dd3fc;
        }
        .movement-card.queue-card {
          gap: 4px;
          margin-bottom: 6px;
          padding: 7px 9px;
        }
        .movement-queue-card-meta {
          overflow: hidden;
          color: #8fa4b9;
          font-size: 10px;
          line-height: 1.3;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .movement-queue-card-actions {
          padding-top: 3px;
        }
        .movement-queue-flag-target {
          display: grid;
          gap: 6px;
          margin-top: 7px;
          padding: 8px;
          border: 1px solid rgba(250, 204, 21, 0.18);
          border-radius: 9px;
          background: rgba(250, 204, 21, 0.045);
          color: #cbd5e1;
          font-size: 10px;
        }
        .movement-queue-flag-target-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 8px;
        }
        .movement-queue-flag-target button.is-active {
          border-color: rgba(250, 204, 21, 0.58);
          background: rgba(250, 204, 21, 0.2);
          color: #fff0b3;
        }
        .movement-queue-flag-bursts {
          max-height: 150px;
          overflow-y: auto;
          display: grid;
          gap: 3px;
          padding: 3px 1px;
        }
        .movement-queue-flag-burst {
          display: grid;
          grid-template-columns: minmax(0, 1fr) auto;
          align-items: center;
          gap: 6px;
          padding: 4px 5px;
          border-radius: 6px;
        }
        .movement-queue-flag-burst:hover {
          background: rgba(255, 255, 255, 0.055);
        }
        .movement-queue-flag-burst.is-visible {
          background: rgba(255, 255, 255, 0.045);
        }
        .movement-queue-flag-burst.is-hidden {
          opacity: 0.5;
        }
        .movement-queue-flag-burst.is-selected {
          box-shadow: inset -3px 0 0 rgba(250, 204, 21, 0.82);
          background: rgba(250, 204, 21, 0.1);
        }
        .movement-queue-flag-burst.is-visible.is-selected {
          box-shadow: inset -3px 0 0 rgba(250, 204, 21, 0.82);
        }
        .movement-queue-burst-show,
        .movement-queue-burst-flag {
          display: inline-flex;
          align-items: flex-start;
          gap: 5px;
          min-width: 0;
          cursor: pointer;
        }
        .movement-queue-burst-flag {
          align-items: center;
          padding: 3px 5px;
          border: 1px solid rgba(250, 204, 21, 0.22);
          border-radius: 6px;
          color: #ffe7a6;
          font-weight: 700;
        }
        .movement-queue-card-actions button.is-selected {
          outline: 2px solid rgba(255, 224, 120, 0.72);
          outline-offset: 1px;
        }
        .movement-queue-card-comment {
          width: 100%;
          margin-top: 3px;
        }
        .movement-review-state {
          padding: 3px 6px;
          border-radius: 999px;
          background: rgba(148, 163, 184, 0.12);
          color: #b7c6d6;
          font-size: 10px;
          white-space: nowrap;
        }
        .movement-review-state.ok {
          background: rgba(67, 206, 162, 0.14);
          color: #b9f6e4;
        }
        .movement-review-state.issues {
          background: rgba(245, 181, 54, 0.14);
          color: #ffe2a1;
        }
        .movement-review-state.remove,
        .movement-prior-decision-badge.remove {
          border-color: rgba(248, 113, 113, 0.42);
          background: rgba(248, 113, 113, 0.14);
          color: #fecaca;
        }
        .movement-table-toolbar {
          display: grid;
          gap: 10px;
          padding: 12px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
          background: rgba(255, 255, 255, 0.02);
        }
        .movement-table-toolbar-row {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          align-items: center;
        }
        .movement-table-toolbar label {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          font-size: 12px;
          color: #9bb0c6;
        }
        .movement-table-toolbar input,
        .movement-table-toolbar select,
        .movement-table-toolbar button {
          font: inherit;
        }
        .movement-table-toolbar input,
        .movement-table-toolbar select {
          min-width: 0;
          padding: 7px 9px;
          border-radius: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          background: rgba(15, 23, 42, 0.92);
          color: #e5edf7;
        }
        .movement-table-toolbar button {
          padding: 7px 10px;
          border: none;
          border-radius: 10px;
          background: rgba(255, 255, 255, 0.08);
          color: #e5edf7;
          cursor: pointer;
        }
        .movement-table-toolbar button:disabled {
          cursor: not-allowed;
          opacity: 0.45;
        }
        .movement-table-toolbar button.movement-emphasis {
          background: rgba(67, 206, 162, 0.22);
          color: #d8fff3;
        }
        .movement-table-toolbar button.is-active,
        .movement-toolbar button.is-active {
          border-color: #57daae;
          background: rgba(87, 218, 174, 0.2);
          color: #d9fff2;
        }
        .movement-table-meta {
          padding: 10px 14px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
          font-size: 11px;
          color: #95a8bb;
          line-height: 1.45;
        }
        .movement-table-wrap {
          min-height: 0;
          overflow: auto;
        }
        .movement-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 11px;
        }
        .movement-table thead th {
          position: sticky;
          top: 0;
          z-index: 1;
          padding: 10px 8px;
          text-align: left;
          font-weight: 600;
          color: #dce8f5;
          background: rgba(11, 18, 30, 0.98);
          border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }
        .movement-table tbody td {
          padding: 9px 8px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.05);
          color: #c9d7e4;
          vertical-align: top;
        }
        .movement-table tbody tr {
          cursor: pointer;
        }
        .movement-table tbody tr:hover {
          background: rgba(255, 255, 255, 0.03);
        }
        .movement-table tbody tr.is-anchor {
          background: rgba(96, 165, 250, 0.14);
        }
        .movement-table tbody tr.is-selected-range {
          background: rgba(67, 206, 162, 0.12);
        }
        .movement-table tbody tr.is-checked-fix {
          box-shadow: inset 3px 0 0 rgba(255, 236, 148, 0.95);
        }
        .movement-table tbody tr.is-segment-row {
          background: rgba(255, 255, 255, 0.015);
        }
        .movement-table tbody tr.is-auto-burst-row {
          background: rgba(125, 211, 252, 0.035);
        }
        .movement-burst-swatch {
          display: inline-block;
          width: 14px;
          height: 14px;
          border-radius: 999px;
          border: 1px solid rgba(255, 255, 255, 0.58);
          box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.06);
          vertical-align: -2px;
        }
        .movement-table-cell-mono {
          font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
          font-variant-numeric: tabular-nums;
        }
        .movement-table-cell-actions {
          white-space: nowrap;
        }
        .movement-table-cell-actions button {
          padding: 5px 8px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 8px;
          background: rgba(255, 255, 255, 0.04);
          color: #e8eef7;
          cursor: pointer;
          font: inherit;
          font-size: 11px;
        }
        .movement-table-empty {
          padding: 18px 14px;
          color: #8ea1b7;
          font-size: 12px;
          line-height: 1.5;
        }
        .movement-table-more-row td.movement-table-more-cell {
          padding: 12px 8px;
          text-align: center;
          color: #8ea1b7;
          font-style: italic;
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
        .movement-card.is-suspected,
        .movement-card.has-unresolved-issues {
          border-color: rgba(251, 191, 36, 0.72);
          background: rgba(245, 181, 54, 0.11);
          box-shadow: inset 4px 0 0 rgba(251, 191, 36, 0.92);
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
        .movement-fix-dismiss {
          background: rgba(245, 181, 54, 0.18);
          border: 1px solid rgba(251, 191, 36, 0.55);
          color: #ffe6a3;
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
        .movement-admin-dashboard-card {
          width: min(1080px, 100%);
        }
        .movement-admin-dashboard-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 12px;
        }
        .movement-admin-dashboard-table th,
        .movement-admin-dashboard-table td {
          padding: 8px;
          text-align: left;
          border-bottom: 1px solid rgba(255, 255, 255, 0.08);
          vertical-align: top;
        }
        .movement-admin-dashboard-actions {
          display: flex;
          gap: 6px;
          flex-wrap: wrap;
        }
        .movement-admin-individuals {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
          gap: 6px;
          padding: 8px 0;
          color: #b8c7d9;
        }
        .movement-issue-scope {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 12px;
        }
        .movement-issue-scope.hidden,
        .movement-issue-scope [hidden] {
          display: none;
        }
        .movement-burst-picker {
          display: grid;
          gap: 8px;
          grid-column: 1 / -1;
        }
        .movement-burst-picker-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 8px;
          color: #dbe4ef;
          font-size: 12px;
        }
        .movement-burst-picker-head span {
          color: #9bb0c6;
          font-size: 11px;
          text-align: right;
        }
        .movement-burst-list {
          max-height: 190px;
          overflow: auto;
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 10px;
          background: rgba(2, 8, 18, 0.44);
        }
        .movement-modal-body label.movement-burst-choice {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr) auto;
          align-items: center;
          gap: 9px;
          padding: 7px 9px;
          border-bottom: 1px solid rgba(255, 255, 255, 0.055);
          color: #dbe4ef;
          cursor: pointer;
        }
        .movement-modal-body label.movement-burst-choice:last-child {
          border-bottom: 0;
        }
        .movement-modal-body label.movement-burst-choice:hover,
        .movement-modal-body label.movement-burst-choice:has(input:checked) {
          background: rgba(82, 214, 181, 0.075);
        }
        .movement-modal-body label.movement-burst-choice input {
          min-width: 0;
          padding: 0;
        }
        .movement-burst-choice-main {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .movement-burst-choice-score {
          color: #b9f3e5;
          font-variant-numeric: tabular-nums;
          white-space: nowrap;
        }
        .movement-burst-preview {
          display: grid;
          gap: 8px;
          padding: 10px;
          border: 1px solid rgba(255, 255, 255, 0.08);
          border-radius: 12px;
          background: rgba(255, 255, 255, 0.025);
        }
        .movement-burst-preview.hidden {
          display: none;
        }
        .movement-burst-preview-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 12px;
          color: #dbe4ef;
          font-size: 12px;
        }
        .movement-burst-preview-head span {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          color: #9bb0c6;
        }
        .movement-burst-preview-list {
          display: grid;
          gap: 10px;
        }
        .movement-burst-preview-card {
          display: grid;
          gap: 7px;
          padding: 9px;
          border-radius: 10px;
          background: #e9eef5;
          color: #152033;
        }
        .movement-burst-preview-card-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 10px;
          color: #152033;
          font-size: 11px;
        }
        .movement-burst-preview-card-head span {
          color: #475569;
          font-variant-numeric: tabular-nums;
        }
        .movement-burst-preview-frame {
          height: 160px;
          overflow: hidden;
          border: 1px solid rgba(255, 255, 255, 0.06);
          border-radius: 10px;
          background:
            linear-gradient(rgba(71, 85, 105, 0.08) 1px, transparent 1px),
            linear-gradient(90deg, rgba(71, 85, 105, 0.08) 1px, transparent 1px),
            #f8fafc;
          background-size: 24px 24px;
        }
        .movement-burst-preview-frame svg {
          display: block;
          width: 100%;
          height: 100%;
        }
        .movement-burst-preview-empty {
          height: 100%;
          display: grid;
          place-items: center;
          padding: 16px;
          color: #475569;
          font-size: 12px;
          text-align: center;
        }
        .movement-burst-preview-metrics {
          display: flex;
          gap: 6px;
          flex-wrap: wrap;
        }
        .movement-burst-preview-metrics span {
          padding: 3px 7px;
          border-radius: 999px;
          background: rgba(71, 85, 105, 0.1);
          color: #334155;
          font-size: 11px;
          white-space: nowrap;
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
          .movement-root:not(.is-slim) .movement-main {
            grid-template-columns: 1fr;
            grid-template-rows: minmax(320px, 1fr) minmax(320px, 1fr);
          }
          .movement-root:not(.is-slim) .movement-side-resize {
            display: none;
          }
          .movement-root:not(.is-slim) .movement-side {
            grid-template-rows: auto minmax(320px, 1fr) auto;
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
          .movement-table-toolbar-row {
            align-items: stretch;
          }
        }
      </style>
      <div class="movement-root">
        <div class="movement-toolbar">
          <label data-role="family-control">Family <select data-role="family"></select></label>
          <label>Study <select data-role="study"></select></label>
          <label>Version <select data-role="dataset"></select></label>
          <label data-role="artifact-control">Artifact <select data-role="artifact"></select></label>
          <label>Basemap <select data-role="basemap"></select></label>
          <label>Color by <select data-role="color-by"></select></label>
          <label class="movement-toggle" data-role="show-train-control"><input type="checkbox" data-role="show-train"> Train</label>
          <label class="movement-toggle" data-role="show-test-control"><input type="checkbox" data-role="show-test"> Test</label>
          <label class="movement-toggle"><input type="checkbox" data-role="show-points"> Points</label>
          <label class="movement-toggle"><input type="checkbox" data-role="show-bursts"> Bursts</label>
          <label class="movement-toggle"><input type="checkbox" data-role="show-confirmed"> Confirmed exclusions</label>
          <label data-role="burst-definition-control">Burst definition
            <select data-role="burst-gap-mode">
              <option value="quantile">Gap quantile</option>
              <option value="manual">Fixed time gap</option>
            </select>
          </label>
          <label class="movement-burst-gap-control" data-role="burst-gap-quantile-control">Gap quantile (0–1)
            <input type="number" min="0.001" max="1" step="0.001" data-role="burst-gap-quantile">
          </label>
          <label class="movement-burst-gap-control" data-role="burst-gap-seconds-control">Time gap (seconds)
            <input type="number" min="1" step="300" data-role="burst-gap-seconds">
          </label>
          <span class="movement-burst-count" data-role="burst-count">No bursts loaded</span>
          <button type="button" data-role="select-all">All individuals</button>
          <button type="button" data-role="select-none">No individuals</button>
          <button type="button" data-role="select-suspicious">Review suspicious fixes</button>
          <button type="button" data-role="clear-fixes">Clear checked fixes</button>
          <div class="movement-candidate-query-control" data-role="candidate-query-control">
            <label>Candidate query <select data-role="candidate-query-select"></select></label>
            <label>Scope
              <select data-role="candidate-query-scope">
                <option value="whole_study">Whole study</option>
                <option value="current_individual">Current individual</option>
                <option value="all_individuals_per_individual">All individuals separately</option>
              </select>
            </label>
            <button type="button" data-role="run-candidate-query">Run filter and flag</button>
            <div class="movement-candidate-query-params hidden" data-role="candidate-query-params"></div>
            <div class="movement-candidate-query-meta" data-role="candidate-query-meta">Loading saved queries...</div>
          </div>
          <button type="button" data-role="check-candidates">Check filter matches</button>
          <button type="button" data-role="clear-candidates">Clear candidates</button>
          <button type="button" data-role="reset-view">Reset view</button>
          <button type="button" class="movement-emphasis" data-role="mark-suspected">Flag checked fixes</button>
          <button type="button" class="movement-emphasis" data-role="mark-confirmed">Mark confirmed</button>
          <button type="button" data-role="dismiss-suspected">Not suspicious</button>
          <label data-role="anomaly-feature-set-control">Ranking features
            <select data-role="anomaly-feature-set">
              <option value="movement_only">Movement only</option>
            </select>
          </label>
          <label data-role="ranking-method-control">Ranking type
            <select data-role="ranking-method">
              <option value="isolation_forest">Isolation forest — worst burst</option>
              <option value="isolation_forest_decision_margin">Isolation forest — total decision margin</option>
              <option value="source_is_outlier">Source is_outlier — total flagged fixes</option>
            </select>
          </label>
          <button type="button" data-role="run-anomaly-ranking">Rank bursts</button>
          <button type="button" data-role="run-burst-feature-space">Feature space</button>
          <button type="button" data-role="generate-report">Generate report</button>
          <button type="button" data-role="export-reviewed-csv">Export reviewed CSV</button>
          <button type="button" data-role="undo">Undo</button>
        </div>
        <div class="movement-release-notice" data-role="release-notice" hidden>
          The editor has finished making changes.
          <button type="button" data-role="load-editor-release">Load latest</button>
        </div>
        <div class="movement-status" data-role="status"></div>
        <div class="movement-output-links" data-role="output-links"></div>
        <div class="movement-main">
          <div class="movement-map-wrap">
            <div class="movement-map" data-role="map"></div>
            <div class="movement-map-attribution hidden" data-role="map-attribution"></div>
            <div class="movement-legend hidden" data-role="legend"></div>
            <div class="movement-threshold hidden" data-role="threshold-pane"></div>
            <div class="movement-fix-popup hidden" data-role="fix-popup"></div>
            <div class="movement-overlay" data-role="overlay">
              <div class="movement-overlay-card">
                <h3>Movement Outlier Review</h3>
                <p>Review fixes on the map, color them by GPS-quality or movement-derived attributes, mark suspected or confirmed issues, and generate reports for the data owners.</p>
              </div>
            </div>
          </div>
          <div
            class="movement-side-resize"
            data-role="side-resize"
            role="separator"
            aria-label="Resize side pane"
            aria-orientation="vertical"
            aria-valuemin="${MIN_SIDE_PANE_WIDTH_PX}"
          ></div>
          <div class="movement-side">
            <div class="movement-individual-view-tabs">
              <button type="button" data-role="individual-view-browse">Browse all</button>
              <button type="button" data-role="individual-view-queue">Review queue</button>
            </div>
            <div class="movement-side-tabs" data-role="side-sheet-tabs">
              <button type="button" class="movement-side-tab is-active" data-role="side-tab-individuals">Individuals</button>
              <button type="button" class="movement-side-tab" data-role="side-tab-table">Table</button>
              <button type="button" class="movement-side-tab" data-role="side-tab-ranking">Burst Ranking</button>
              <button type="button" class="movement-side-tab" data-role="side-tab-feature-space">Burst feature space</button>
            </div>
            <div class="movement-side-content">
              <div class="movement-side-sheet individuals" data-role="side-sheet-individuals">
                <div class="movement-side-head" data-role="individual-head">Individuals and coverage</div>
                <div class="movement-side-search">
                  <label data-role="individual-search-control">Search by individual ID
                    <input type="search" data-role="individual-search" placeholder="Find an individual ID">
                  </label>
                  <div class="movement-queue-controls hidden" data-role="individual-queue-controls">
                    <label class="movement-queue-order">Review order
                      <select data-role="individual-queue-order">
                        <option value="dataset">Dataset order</option>
                        <option value="isolation_forest">Isolation forest — worst burst</option>
                        <option value="isolation_forest_decision_margin">Isolation forest — total decision margin</option>
                        <option value="source_is_outlier">Source is_outlier — total flagged fixes</option>
                      </select>
                    </label>
                    <label class="movement-queue-order">Queue
                      <select data-role="individual-queue-filter">
                        <option value="all">All individuals</option>
                        <option value="unresolved">Unresolved issues</option>
                        <option value="needs_check">Needs check</option>
                      </select>
                    </label>
                    <div class="movement-queue-ranking-state" data-role="individual-queue-ranking-state"></div>
                    <div class="movement-queue-nav">
                      <button type="button" data-queue-nav="previous-page">Previous 25</button>
                      <button type="button" data-queue-nav="previous-group">Previous 5</button>
                      <button type="button" data-queue-nav="previous-individual">Previous</button>
                      <button type="button" data-queue-nav="next-individual">Next</button>
                      <button type="button" data-queue-nav="next-group">Next 5</button>
                      <button type="button" data-queue-nav="next-page">Next 25</button>
                    </div>
                    <div class="movement-queue-map-controls">
                      <button type="button" data-queue-scope="solo">Only current</button>
                      <button type="button" data-queue-scope="group">Group view</button>
                    </div>
                    <div class="movement-queue-progress" data-role="individual-queue-progress"></div>
                    <button type="button" data-role="individual-queue-save">Save decision</button>
                  </div>
                </div>
                <div class="movement-individuals" data-role="individuals"></div>
                <div
                  class="movement-individual-resize"
                  data-role="individual-resize"
                  role="separator"
                  aria-label="Resize individual list vertically"
                  aria-orientation="horizontal"
                  aria-valuemin="${MIN_INDIVIDUAL_LIST_HEIGHT_PX}"
                  tabindex="0"
                ></div>
                <div class="movement-side-head" data-role="fix-head">Checked fixes</div>
                <div class="movement-fixes" data-role="selected-fixes"></div>
              </div>
              <div class="movement-side-sheet table hidden" data-role="side-sheet-table">
                <div class="movement-table-toolbar">
                  <div class="movement-table-toolbar-row">
                    <label>Mode
                      <select data-role="table-mode">
                        <option value="fixes">Fix rows</option>
                        <option value="segments">Flagged segments</option>
                        <option value="auto_bursts">Automatic bursts</option>
                      </select>
                    </label>
                    <label>Search
                      <input type="search" data-role="table-filter" placeholder="Individual, issue, fix key">
                    </label>
                    <label>Sort
                      <select data-role="table-sort">
                        <option value="track_time">Track order</option>
                        <option value="time_desc">Newest first</option>
                        <option value="time_asc">Oldest first</option>
                        <option value="status">Status</option>
                        <option value="issue_type">Issue type</option>
                      </select>
                    </label>
                    <button type="button" data-role="table-sort-direction" data-direction="asc">Ascending</button>
                  </div>
                  <div class="movement-table-toolbar-row">
                    <button type="button" data-role="segment-clear">Clear range</button>
                    <button type="button" class="movement-emphasis" data-role="segment-confirmed">Mark segment confirmed</button>
                  </div>
                </div>
                <div class="movement-table-meta" data-role="table-meta"></div>
                <div class="movement-table-wrap" data-role="table-wrap"></div>
              </div>
              <div class="movement-side-sheet ranking hidden" data-role="side-sheet-ranking">
                <div class="movement-side-head movement-ranking-head">
                  <span>Burst ranking</span>
                </div>
                <div class="movement-anomaly-ranking" data-role="anomaly-ranking"></div>
              </div>
              <div class="movement-side-sheet feature-space hidden" data-role="side-sheet-feature-space">
                <div class="movement-side-head">Burst feature space</div>
                <div class="movement-feature-space" data-role="burst-feature-space"></div>
              </div>
            </div>
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
            <div class="movement-issue-scope hidden" data-role="issue-scope-control">
              <label>Flag scope
                <select data-role="issue-scope">
                  <option value="individual">Entire individual</option>
                  <option value="burst">By Burst</option>
                </select>
              </label>
              <div class="movement-burst-picker" data-role="issue-burst-control">
                <div class="movement-burst-picker-head">
                  <strong>Select bursts</strong>
                  <span data-role="issue-burst-order"></span>
                </div>
                <div class="movement-burst-list" data-role="issue-burst-list"></div>
              </div>
            </div>
            <div class="movement-burst-preview hidden" data-role="issue-burst-preview">
              <div class="movement-burst-preview-head">
                <strong>Selected burst previews</strong>
                <span data-role="issue-burst-preview-title"></span>
              </div>
              <div class="movement-burst-preview-list" data-role="issue-burst-preview-list"></div>
            </div>
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

      <div class="movement-modal hidden" data-role="confirm-modal">
        <div class="movement-modal-card">
          <div class="movement-modal-head">
            <h3>Confirm suspected outliers</h3>
            <button type="button" data-role="confirm-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div data-role="confirm-meta"></div>
            <div class="movement-selection-list" data-role="confirm-groups"></div>
            <label>User
              <input type="text" data-role="confirm-user" placeholder="Name used for attribution">
            </label>
            <label>Confirmation note (optional)
              <textarea data-role="confirm-note" placeholder="Add any final review context."></textarea>
            </label>
            <div class="movement-modal-status" data-role="confirm-status"></div>
          </div>
          <div class="movement-modal-foot">
            <span>Only the selected fixes in checked issue groups will be confirmed.</span>
            <button type="button" class="movement-emphasis" data-role="confirm-submit">Confirm selected issues</button>
          </div>
        </div>
      </div>

      <div class="movement-modal hidden" data-role="dismiss-modal">
        <div class="movement-modal-card">
          <div class="movement-modal-head">
            <h3>Mark as not suspicious</h3>
            <button type="button" data-role="dismiss-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div data-role="dismiss-meta"></div>
            <div class="movement-selection-list" data-role="dismiss-groups"></div>
            <label>User
              <input type="text" data-role="dismiss-user" placeholder="Name used for attribution">
            </label>
            <label>Dismissal note (optional)
              <textarea data-role="dismiss-note" placeholder="Why are these fixes not suspicious?"></textarea>
            </label>
            <div class="movement-modal-status" data-role="dismiss-status"></div>
          </div>
          <div class="movement-modal-foot">
            <span>Each selected originating suspicion will be resolved for these fixes.</span>
            <button type="button" class="movement-emphasis" data-role="dismiss-submit">Dismiss selected suspicions</button>
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
            <label data-role="report-snapshot-unit-wrap">Snapshot unit
              <select data-role="report-snapshot-unit">
                <option value="burst">One per flagged burst</option>
                <option value="context">Merge nearby flagged fixes</option>
              </select>
            </label>
            <label data-role="report-basemap-wrap">Report basemap
              <select data-role="report-basemap">
                <option value="current">Match current map when possible</option>
                <option value="Positron">Positron</option>
                <option value="OSM Streets">OSM Streets</option>
                <option value="Satellite">Satellite</option>
                <option value="Satellite + labels">Satellite + labels</option>
                <option value="Topographic">Topographic</option>
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

      <div class="movement-modal hidden" data-role="admin-dashboard-modal">
        <div class="movement-modal-card movement-admin-dashboard-card">
          <div class="movement-modal-head">
            <h3>Review dashboard</h3>
            <button type="button" data-role="admin-dashboard-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div class="movement-modal-status" data-role="admin-dashboard-status"></div>
            <div data-role="admin-dashboard-content"></div>
          </div>
        </div>
      </div>

      <div class="movement-modal hidden" data-role="resume-modal">
        <div class="movement-modal-card">
          <div class="movement-modal-head">
            <h3>Resume from historical version</h3>
            <button type="button" data-role="resume-close">Close</button>
          </div>
          <div class="movement-modal-body">
            <div data-role="resume-meta"></div>
            <div class="movement-selection-list" data-role="resume-warning"></div>
            <label>User
              <input type="text" data-role="resume-user" placeholder="Name used for attribution">
            </label>
            <div class="movement-modal-status" data-role="resume-status"></div>
          </div>
          <div class="movement-modal-foot">
            <span>This cannot be recovered from the app.</span>
            <button type="button" class="movement-danger" data-role="resume-submit">Discard forward history and resume</button>
          </div>
        </div>
      </div>

    `;

    this.refs = {
      main: this.mountEl.querySelector(".movement-main"),
      familyControl: this.mountEl.querySelector('[data-role="family-control"]'),
      family: this.mountEl.querySelector('[data-role="family"]'),
      study: this.mountEl.querySelector('[data-role="study"]'),
      dataset: this.mountEl.querySelector('[data-role="dataset"]'),
      artifactControl: this.mountEl.querySelector('[data-role="artifact-control"]'),
      artifact: this.mountEl.querySelector('[data-role="artifact"]'),
      basemap: this.mountEl.querySelector('[data-role="basemap"]'),
      colorBy: this.mountEl.querySelector('[data-role="color-by"]'),
      showTrain: this.mountEl.querySelector('[data-role="show-train"]'),
      showTrainControl: this.mountEl.querySelector('[data-role="show-train-control"]'),
      showTest: this.mountEl.querySelector('[data-role="show-test"]'),
      showTestControl: this.mountEl.querySelector('[data-role="show-test-control"]'),
      showPoints: this.mountEl.querySelector('[data-role="show-points"]'),
      showBursts: this.mountEl.querySelector('[data-role="show-bursts"]'),
      showConfirmed: this.mountEl.querySelector('[data-role="show-confirmed"]'),
      burstGapMode: this.mountEl.querySelector('[data-role="burst-gap-mode"]'),
      burstDefinitionControl: this.mountEl.querySelector('[data-role="burst-definition-control"]'),
      burstGapQuantileControl: this.mountEl.querySelector('[data-role="burst-gap-quantile-control"]'),
      burstGapSecondsControl: this.mountEl.querySelector('[data-role="burst-gap-seconds-control"]'),
      burstGapSeconds: this.mountEl.querySelector('[data-role="burst-gap-seconds"]'),
      burstGapQuantile: this.mountEl.querySelector('[data-role="burst-gap-quantile"]'),
      burstCount: this.mountEl.querySelector('[data-role="burst-count"]'),
      selectAll: this.mountEl.querySelector('[data-role="select-all"]'),
      selectNone: this.mountEl.querySelector('[data-role="select-none"]'),
      selectSuspicious: this.mountEl.querySelector('[data-role="select-suspicious"]'),
      clearFixes: this.mountEl.querySelector('[data-role="clear-fixes"]'),
      candidateQueryControl: this.mountEl.querySelector('[data-role="candidate-query-control"]'),
      candidateQuerySelect: this.mountEl.querySelector('[data-role="candidate-query-select"]'),
      candidateQueryScope: this.mountEl.querySelector('[data-role="candidate-query-scope"]'),
      candidateQueryMeta: this.mountEl.querySelector('[data-role="candidate-query-meta"]'),
      candidateQueryParams: this.mountEl.querySelector('[data-role="candidate-query-params"]'),
      runCandidateQuery: this.mountEl.querySelector('[data-role="run-candidate-query"]'),
      checkCandidates: this.mountEl.querySelector('[data-role="check-candidates"]'),
      clearCandidates: this.mountEl.querySelector('[data-role="clear-candidates"]'),
      resetView: this.mountEl.querySelector('[data-role="reset-view"]'),
      markSuspected: this.mountEl.querySelector('[data-role="mark-suspected"]'),
      markConfirmed: this.mountEl.querySelector('[data-role="mark-confirmed"]'),
      dismissSuspected: this.mountEl.querySelector('[data-role="dismiss-suspected"]'),
      anomalyFeatureSetControl: this.mountEl.querySelector('[data-role="anomaly-feature-set-control"]'),
      anomalyFeatureSet: this.mountEl.querySelector('[data-role="anomaly-feature-set"]'),
      rankingMethodControl: this.mountEl.querySelector('[data-role="ranking-method-control"]'),
      rankingMethod: this.mountEl.querySelector('[data-role="ranking-method"]'),
      runAnomalyRanking: this.mountEl.querySelector('[data-role="run-anomaly-ranking"]'),
      runBurstFeatureSpace: this.mountEl.querySelector('[data-role="run-burst-feature-space"]'),
      generateReport: this.mountEl.querySelector('[data-role="generate-report"]'),
      exportReviewedCsv: this.mountEl.querySelector('[data-role="export-reviewed-csv"]'),
      undo: this.mountEl.querySelector('[data-role="undo"]'),
      editLockProfile: document.querySelector('[data-role="edit-lock-profile"]'),
      editLockMessage: document.querySelector('[data-role="edit-lock-message"]'),
      resumeHistory: document.querySelector('[data-role="resume-history"]'),
      authIdentity: document.querySelector('[data-role="auth-identity"]'),
      reviewProgress: document.querySelector('[data-role="review-progress"]'),
      adminDashboard: document.querySelector('[data-role="admin-dashboard"]'),
      assignReview: document.querySelector('[data-role="assign-review"]'),
      completeReview: document.querySelector('[data-role="complete-review"]'),
      cancelReview: document.querySelector('[data-role="cancel-review"]'),
      editorControlStart: document.querySelector('[data-role="editor-control-start"]'),
      editorControlFinish: document.querySelector('[data-role="editor-control-finish"]'),
      releaseNotice: this.mountEl.querySelector('[data-role="release-notice"]'),
      loadEditorRelease: this.mountEl.querySelector('[data-role="load-editor-release"]'),
      status: this.mountEl.querySelector('[data-role="status"]'),
      outputLinks: this.mountEl.querySelector('[data-role="output-links"]'),
      sideSheetTabs: this.mountEl.querySelector('[data-role="side-sheet-tabs"]'),
      sideTabIndividuals: this.mountEl.querySelector('[data-role="side-tab-individuals"]'),
      sideTabTable: this.mountEl.querySelector('[data-role="side-tab-table"]'),
      sideTabRanking: this.mountEl.querySelector('[data-role="side-tab-ranking"]'),
      sideTabFeatureSpace: this.mountEl.querySelector('[data-role="side-tab-feature-space"]'),
      sideSheetIndividuals: this.mountEl.querySelector('[data-role="side-sheet-individuals"]'),
      sideSheetTable: this.mountEl.querySelector('[data-role="side-sheet-table"]'),
      sideSheetRanking: this.mountEl.querySelector('[data-role="side-sheet-ranking"]'),
      sideSheetFeatureSpace: this.mountEl.querySelector('[data-role="side-sheet-feature-space"]'),
      sideResize: this.mountEl.querySelector('[data-role="side-resize"]'),
      individualSearch: this.mountEl.querySelector('[data-role="individual-search"]'),
      individualSearchControl: this.mountEl.querySelector('[data-role="individual-search-control"]'),
      individualViewBrowse: this.mountEl.querySelector('[data-role="individual-view-browse"]'),
      individualViewQueue: this.mountEl.querySelector('[data-role="individual-view-queue"]'),
      individualQueueControls: this.mountEl.querySelector('[data-role="individual-queue-controls"]'),
      individualQueueOrder: this.mountEl.querySelector('[data-role="individual-queue-order"]'),
      individualQueueFilter: this.mountEl.querySelector('[data-role="individual-queue-filter"]'),
      individualQueueRankingState: this.mountEl.querySelector('[data-role="individual-queue-ranking-state"]'),
      individualQueueProgress: this.mountEl.querySelector('[data-role="individual-queue-progress"]'),
      individualQueueSave: this.mountEl.querySelector('[data-role="individual-queue-save"]'),
      individuals: this.mountEl.querySelector('[data-role="individuals"]'),
      individualResize: this.mountEl.querySelector('[data-role="individual-resize"]'),
      individualHead: this.mountEl.querySelector('[data-role="individual-head"]'),
      fixHead: this.mountEl.querySelector('[data-role="fix-head"]'),
      selectedFixes: this.mountEl.querySelector('[data-role="selected-fixes"]'),
      anomalyRanking: this.mountEl.querySelector('[data-role="anomaly-ranking"]'),
      burstFeatureSpace: this.mountEl.querySelector('[data-role="burst-feature-space"]'),
      tableMode: this.mountEl.querySelector('[data-role="table-mode"]'),
      tableFilter: this.mountEl.querySelector('[data-role="table-filter"]'),
      tableSort: this.mountEl.querySelector('[data-role="table-sort"]'),
      tableSortDirection: this.mountEl.querySelector('[data-role="table-sort-direction"]'),
      tableMeta: this.mountEl.querySelector('[data-role="table-meta"]'),
      tableWrap: this.mountEl.querySelector('[data-role="table-wrap"]'),
      segmentClear: this.mountEl.querySelector('[data-role="segment-clear"]'),
      segmentConfirmed: this.mountEl.querySelector('[data-role="segment-confirmed"]'),
      slider: this.mountEl.querySelector('[data-role="slider"]'),
      time: this.mountEl.querySelector('[data-role="time"]'),
      map: this.mountEl.querySelector('[data-role="map"]'),
      mapAttribution: this.mountEl.querySelector('[data-role="map-attribution"]'),
      legend: this.mountEl.querySelector('[data-role="legend"]'),
      thresholdPane: this.mountEl.querySelector('[data-role="threshold-pane"]'),
      fixPopup: this.mountEl.querySelector('[data-role="fix-popup"]'),
      overlay: this.mountEl.querySelector('[data-role="overlay"]'),
      issueModal: this.mountEl.querySelector('[data-role="issue-modal"]'),
      adminDashboardModal: this.mountEl.querySelector('[data-role="admin-dashboard-modal"]'),
      adminDashboardClose: this.mountEl.querySelector('[data-role="admin-dashboard-close"]'),
      adminDashboardStatus: this.mountEl.querySelector('[data-role="admin-dashboard-status"]'),
      adminDashboardContent: this.mountEl.querySelector('[data-role="admin-dashboard-content"]'),
      issueTitle: this.mountEl.querySelector('[data-role="issue-title"]'),
      issueMeta: this.mountEl.querySelector('[data-role="issue-meta"]'),
      issueScopeControl: this.mountEl.querySelector('[data-role="issue-scope-control"]'),
      issueScope: this.mountEl.querySelector('[data-role="issue-scope"]'),
      issueBurstControl: this.mountEl.querySelector('[data-role="issue-burst-control"]'),
      issueBurstOrder: this.mountEl.querySelector('[data-role="issue-burst-order"]'),
      issueBurstList: this.mountEl.querySelector('[data-role="issue-burst-list"]'),
      issueBurstPreview: this.mountEl.querySelector('[data-role="issue-burst-preview"]'),
      issueBurstPreviewTitle: this.mountEl.querySelector('[data-role="issue-burst-preview-title"]'),
      issueBurstPreviewList: this.mountEl.querySelector('[data-role="issue-burst-preview-list"]'),
      issueSelection: this.mountEl.querySelector('[data-role="issue-selection"]'),
      issueUser: this.mountEl.querySelector('[data-role="issue-user"]'),
      issueType: this.mountEl.querySelector('[data-role="issue-type"]'),
      issueNote: this.mountEl.querySelector('[data-role="issue-note"]'),
      issueQuestion: this.mountEl.querySelector('[data-role="issue-question"]'),
      issueStatus: this.mountEl.querySelector('[data-role="issue-status"]'),
      issueClose: this.mountEl.querySelector('[data-role="issue-close"]'),
      issueSubmit: this.mountEl.querySelector('[data-role="issue-submit"]'),
      confirmModal: this.mountEl.querySelector('[data-role="confirm-modal"]'),
      confirmMeta: this.mountEl.querySelector('[data-role="confirm-meta"]'),
      confirmGroups: this.mountEl.querySelector('[data-role="confirm-groups"]'),
      confirmUser: this.mountEl.querySelector('[data-role="confirm-user"]'),
      confirmNote: this.mountEl.querySelector('[data-role="confirm-note"]'),
      confirmStatus: this.mountEl.querySelector('[data-role="confirm-status"]'),
      confirmClose: this.mountEl.querySelector('[data-role="confirm-close"]'),
      confirmSubmit: this.mountEl.querySelector('[data-role="confirm-submit"]'),
      dismissModal: this.mountEl.querySelector('[data-role="dismiss-modal"]'),
      dismissMeta: this.mountEl.querySelector('[data-role="dismiss-meta"]'),
      dismissGroups: this.mountEl.querySelector('[data-role="dismiss-groups"]'),
      dismissUser: this.mountEl.querySelector('[data-role="dismiss-user"]'),
      dismissNote: this.mountEl.querySelector('[data-role="dismiss-note"]'),
      dismissStatus: this.mountEl.querySelector('[data-role="dismiss-status"]'),
      dismissClose: this.mountEl.querySelector('[data-role="dismiss-close"]'),
      dismissSubmit: this.mountEl.querySelector('[data-role="dismiss-submit"]'),
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
      reportSnapshotUnitWrap: this.mountEl.querySelector('[data-role="report-snapshot-unit-wrap"]'),
      reportSnapshotUnit: this.mountEl.querySelector('[data-role="report-snapshot-unit"]'),
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
      resumeModal: this.mountEl.querySelector('[data-role="resume-modal"]'),
      resumeMeta: this.mountEl.querySelector('[data-role="resume-meta"]'),
      resumeWarning: this.mountEl.querySelector('[data-role="resume-warning"]'),
      resumeUser: this.mountEl.querySelector('[data-role="resume-user"]'),
      resumeStatus: this.mountEl.querySelector('[data-role="resume-status"]'),
      resumeClose: this.mountEl.querySelector('[data-role="resume-close"]'),
      resumeSubmit: this.mountEl.querySelector('[data-role="resume-submit"]'),
    };

    this.applyAppProfile();
    this.renderReviewControls();

    for (const name of Object.keys(BASEMAP_PRESETS)) {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      this.refs.basemap.appendChild(option);
    }
    const storedBasemap = BASEMAP_PRESETS[this.uiState.basemap]
      ? this.uiState.basemap
      : "Positron";
    this.refs.basemap.value = (storedBasemap === "Blank" || storedBasemap === "OSM Streets")
      ? "Positron"
      : storedBasemap;
    this.refs.reportBasemap.value = "current";
    this.refs.showTrain.checked = MOVEMENT_APP_CONFIG.mode === "slim_movement" || this.uiState.showTrain !== false;
    this.refs.showTest.checked = MOVEMENT_APP_CONFIG.mode === "slim_movement" || this.uiState.showTest !== false;
    this.refs.showPoints.checked = this.uiState.showPoints !== false;
    this.refs.showBursts.checked = this.uiState.showBursts !== false;
    this.refs.showConfirmed.checked = this.uiState.showConfirmed !== false;
    this.refs.burstGapMode.value = ["manual", "quantile"].includes(this.uiState.burstGapMode)
      ? this.uiState.burstGapMode
      : DEFAULT_BURST_GAP_MODE;
    this.refs.burstGapSeconds.value = String(
      Number.isFinite(Number(this.uiState.burstGapSeconds)) && Number(this.uiState.burstGapSeconds) > 0
        ? Number(this.uiState.burstGapSeconds)
        : DEFAULT_BURST_GAP_SECONDS,
    );
    this.refs.burstGapQuantile.value = String(
      Number.isFinite(Number(this.uiState.burstGapQuantile))
        && Number(this.uiState.burstGapQuantile) > 0
        && Number(this.uiState.burstGapQuantile) <= 1
        ? Number(this.uiState.burstGapQuantile)
        : DEFAULT_BURST_GAP_QUANTILE,
    );
    this.refs.anomalyFeatureSet.value = this.uiState.anomalyFeatureSet === "movement_plus_context"
      ? "movement_plus_context"
      : "movement_only";
    this.refs.rankingMethod.value = [
      "isolation_forest",
      "isolation_forest_decision_margin",
      "source_is_outlier",
    ].includes(this.uiState.rankingMethod)
      ? this.uiState.rankingMethod
      : "isolation_forest";
    this.syncRankingMethodControl();
    this.syncAnomalyFeatureSetOptions({ save: false });
    this.syncBurstGapControls();
    this.renderBurstCountIndicator();
    this.refs.tableMode.value = this.uiState.tableMode || "fixes";
    this.refs.tableSort.value = this.uiState.tableSort || "track_time";
    this.refs.tableFilter.value = this.uiState.tableFilter || "";
    this.refs.tableSortDirection.dataset.direction = this.uiState.tableDescending ? "desc" : "asc";
    this.refs.tableSortDirection.textContent = this.uiState.tableDescending ? "Descending" : "Ascending";
    this.refs.individualSearch.value = this.individualSearchQuery;
    this.syncIndividualQueueRankingOptions();
    this.refs.individualQueueFilter.value = this.individualReviewQueue.filterMode;
    this.applySidePaneWidth(this.sidePaneWidthPx, { save: false, resizeMap: false });
    this.setSideSheet(
      this.individualReviewQueue.mode === "queue"
        ? "individuals"
        : this.uiState.sideSheet || "individuals",
      { save: false },
    );
    this.renderIndividuals();
    if (this.currentIndividualListHeight() !== null) {
      this.applyIndividualListHeight(this.currentIndividualListHeight(), { save: false });
    }
    this.renderAnomalyRanking();
    this.renderBurstFeatureSpace();
    this.updateActionButtons();
  }

  bindEvents() {
    window.addEventListener("resize", this.handleWindowResize);
    this.refs.assignReview?.addEventListener("click", () => void this.assignCurrentReview());
    this.refs.completeReview?.addEventListener("click", () => void this.completeCurrentReview());
    this.refs.cancelReview?.addEventListener("click", () => void this.cancelCurrentReview());
    this.refs.editorControlStart?.addEventListener("click", () => void this.startCurrentEditorControl());
    this.refs.editorControlFinish?.addEventListener("click", () => void this.finishCurrentEditorControl());
    this.refs.loadEditorRelease?.addEventListener("click", () => {
      void this.loadReleasedEditorChanges();
    });
    this.refs.adminDashboard?.addEventListener("click", () => void this.openAdminDashboard());
    this.refs.adminDashboardClose?.addEventListener("click", () => {
      this.refs.adminDashboardModal.classList.add("hidden");
    });
    this.refs.adminDashboardContent?.addEventListener("click", event => {
      void this.handleAdminDashboardClick(event);
    });
    this.refs.sideTabIndividuals.addEventListener("click", () => this.setSideSheet("individuals"));
    this.refs.sideTabTable.addEventListener("click", () => this.setSideSheet("table"));
    this.refs.sideTabRanking.addEventListener("click", () => this.setSideSheet("ranking"));
    this.refs.sideTabFeatureSpace.addEventListener("click", () => this.setSideSheet("feature_space"));
    this.refs.individualSearch.addEventListener("input", () => {
      this.individualSearchQuery = this.refs.individualSearch.value || "";
      this.renderIndividuals();
    });
    this.refs.individualViewBrowse.addEventListener("click", () => {
      void this.setIndividualViewMode("browse");
    });
    this.refs.individualViewQueue.addEventListener("click", () => {
      void this.setIndividualViewMode("queue");
    });
    this.refs.individualQueueOrder.addEventListener("change", () => {
      void this.changeIndividualQueueOrder(this.refs.individualQueueOrder.value);
    });
    this.refs.individualQueueFilter.addEventListener("change", () => {
      const filterMode = this.refs.individualQueueFilter.value;
      this.individualReviewQueue.filterMode = ["needs_check", "unresolved"].includes(filterMode)
        ? filterMode
        : "all";
      this.individualReviewQueue.pageIndex = 0;
      this.individualReviewQueue.groupIndex = 0;
      this.individualReviewQueue.activeIndividual = "";
      this.hiddenBurstIds.clear();
      this.resetManualFlagTarget();
      this.flagTargetKind = "none";
      void this.applyIndividualQueueMapScope();
    });
    this.refs.individualQueueControls.addEventListener("click", event => {
      const navButton = event.target.closest("button[data-queue-nav]");
      if (navButton) {
        void this.navigateIndividualQueue(navButton.dataset.queueNav || "");
        return;
      }
      const scopeButton = event.target.closest("button[data-queue-scope]");
      if (scopeButton) {
        void this.setIndividualQueueMapScope(scopeButton.dataset.queueScope || "group");
        return;
      }
      const queueAction = event.target.closest("button[data-queue-action]");
      if (queueAction?.dataset.queueAction === "run-ranking") {
        void this.runBurstAnomalyRanking({ openRankingSheet: false });
      } else if (queueAction?.dataset.queueAction === "load-ranking") {
        void this.loadSavedAnomalyRanking();
      } else if (queueAction?.dataset.queueAction === "check-ranking") {
        void this.restoreSavedAnalyses();
      } else if (queueAction?.dataset.queueAction === "apply-ranking") {
        void this.applyCompletedIndividualQueueRanking();
      }
    });
    this.refs.individuals.addEventListener("click", event => {
      const reviewButton = event.target.closest("button[data-review-decision]");
      if (reviewButton) {
        const individual = reviewButton.dataset.individual || "";
        const decision = reviewButton.dataset.reviewDecision || "";
        this.stageIndividualReviewDecision(individual, decision);
        return;
      }
      const wholeIndividualButton = event.target.closest("button[data-queue-flag-individual]");
      if (wholeIndividualButton) {
        this.selectEntireIndividualFlagTarget(wholeIndividualButton.dataset.individual || "");
        return;
      }
      const skipButton = event.target.closest("button[data-queue-skip]");
      if (skipButton) {
        void this.skipIndividualQueueItem(skipButton.dataset.individual || "");
        return;
      }
      const commentButton = event.target.closest("button[data-queue-comment]");
      if (commentButton) {
        this.toggleIndividualQueueComment(commentButton.dataset.individual || "");
        return;
      }
      const tableButton = event.target.closest("button[data-queue-table]");
      if (tableButton) {
        void this.viewIndividualQueueTable(tableButton.dataset.individual || "");
      }
    });
    this.refs.individuals.addEventListener("change", event => {
      const needsCheckInput = event.target.closest("input[data-review-needs-check]");
      if (needsCheckInput) {
        this.stageIndividualNeedsCheck(
          needsCheckInput.dataset.individual || "",
          needsCheckInput.checked,
        );
        return;
      }
      const burstVisibilityInput = event.target.closest("input[data-queue-burst-visible]");
      if (burstVisibilityInput) {
        this.setBurstVisible(
          burstVisibilityInput.dataset.queueBurstVisible || "",
          burstVisibilityInput.checked,
        );
        return;
      }
      const burstInput = event.target.closest("input[data-queue-flag-burst]");
      if (!burstInput) return;
      this.setBurstFlagTargetIncluded(
        burstInput.dataset.queueFlagBurst || "",
        burstInput.checked,
        { selectionMethod: "queue_burst_list" },
      );
    });
    this.refs.individuals.addEventListener("input", event => {
      const input = event.target.closest("input[data-queue-comment-input]");
      if (input) {
        this.updateIndividualReviewCommentDraft(
          input.dataset.individual || "",
          input.value,
        );
      }
    });
    this.refs.individualQueueSave.addEventListener("click", () => {
      void this.saveActiveIndividualReviewDecision();
    });
    this.refs.sideResize.addEventListener("pointerdown", event => this.beginSidePaneResize(event));
    this.refs.individualResize.addEventListener("pointerdown", event => this.beginIndividualPaneResize(event));
    this.refs.individualResize.addEventListener("keydown", event => this.resizeIndividualPaneFromKeyboard(event));
    this.refs.family.addEventListener("change", async () => {
      const nextFamily = this.refs.family.value;
      try {
        if (!this.confirmDiscardIndividualReviewDrafts()) {
          this.refs.family.value = this.currentFamily;
          return;
        }
        await this.switchFamily(nextFamily);
      } catch (error) {
        console.error("Failed to switch movement family", error);
        this.setStatus(`Could not switch family: ${error.message}`, true);
        this.showOverlay("The family changed, but the study list could not be reloaded.");
      }
    });
    this.refs.study.addEventListener("change", async () => {
      const nextStudy = this.refs.study.value;
      try {
        if (!this.confirmDiscardIndividualReviewDrafts()) {
          this.refs.study.value = this.currentStudy;
          return;
        }
        this.currentStudy = nextStudy;
        this.gpsSpikeTurnAngleDeg = DEFAULT_GPS_SPIKE_TURN_ANGLE_DEG;
        this.closeStudyEvents();
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
      const nextDatasetId = this.refs.dataset.value;
      if (!this.confirmDiscardIndividualReviewDrafts()) {
        this.refs.dataset.value = this.currentDatasetId;
        return;
      }
      this.gpsSpikeTurnAngleDeg = DEFAULT_GPS_SPIKE_TURN_ANGLE_DEG;
      await this.transitionToDataset(nextDatasetId, { reason: "dataset_switch" });
    });
    this.refs.artifact.addEventListener("change", async () => {
      const nextArtifact = this.refs.artifact.value;
      if (!this.confirmDiscardIndividualReviewDrafts()) {
        this.refs.artifact.value = this.currentArtifact;
        return;
      }
      const viewContext = this.captureDatasetViewContext();
      this.gpsSpikeTurnAngleDeg = DEFAULT_GPS_SPIKE_TURN_ANGLE_DEG;
      this.currentArtifact = nextArtifact;
      this.saveUiState();
      if (!this.currentArtifact) {
        this.clearLoadedStudyState();
        this.showOverlay("Select a study to load movement tracks.");
        this.setStatus("Select a study to load the map.");
        return;
      }
      await this.loadArtifact(viewContext);
    });
    this.refs.basemap.addEventListener("change", async () => {
      this.saveUiState();
      this.updateMapAttribution();
      await this.rebuildMap(true);
    });
    this.refs.colorBy.addEventListener("change", () => {
      this.clearThresholdState();
      this.saveUiState();
      this.renderLegend();
      this.renderThresholdPane();
      this.renderLayers();
      this.renderSelectedFixes();
      this.updateActionButtons();
    });
    this.refs.showTrain.addEventListener("change", () => this.handleVisibilityChange());
    this.refs.showTest.addEventListener("change", () => this.handleVisibilityChange());
    this.refs.showPoints.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
      this.renderTableSheet();
    });
    this.refs.showBursts.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
      this.renderTableSheet();
    });
    this.refs.showConfirmed.addEventListener("change", () => {
      this.saveUiState();
      this.renderLayers();
      if (this.refs.showConfirmed.checked && this.data?.confirmedState === "idle") {
        void this.loadConfirmedFixes();
      }
    });
    this.refs.burstGapMode.addEventListener("change", () => this.handleBurstGapSettingsChange());
    this.refs.burstGapSeconds.addEventListener("change", () => this.handleBurstGapSettingsChange());
    this.refs.burstGapQuantile.addEventListener("change", () => this.handleBurstGapSettingsChange());
    this.refs.anomalyFeatureSet.addEventListener("change", () => {
      this.saveUiState();
      this.clearAnomalyRanking();
      this.clearBurstFeatureSpace();
      void this.restoreSavedAnalyses();
    });
    this.refs.rankingMethod.addEventListener("change", () => this.handleRankingMethodChange());
    this.refs.fixPopup.addEventListener("click", event => {
      const closeButton = event.target.closest('[data-role="fix-popup-close"]');
      if (closeButton) {
        this.closeFixPopup();
      }
    });
    this.refs.selectAll.addEventListener("click", () => {
      if (!this.data) return;
      this.clearThresholdState();
      if (this.individualReviewQueue.mode === "queue") {
        this.individualReviewQueue.mapScope = "all";
      }
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
      this.cancelRequest("binaryFixes");
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
      void this.loadSuspiciousFixes();
    });
    this.refs.clearFixes.addEventListener("click", () => {
      if (!this.data) return;
      this.data.selectedFixKeys = new Set();
      if (this.flagTargetKind === "fixes") {
        this.setTableSelection();
        this.mapRangeAwaitingEnd = false;
        this.resetManualFlagTarget({ resetKind: false });
        this.flagTargetKind = "none";
      }
      this.renderThresholdPane();
      this.renderSelectedFixes();
      this.renderTableSheet();
      this.renderLayers();
      this.updateActionButtons();
    });
    this.refs.candidateQuerySelect.addEventListener("change", () => this.selectCandidateQuery(this.refs.candidateQuerySelect.value));
    this.refs.candidateQueryScope.addEventListener("change", () => this.selectCandidateQueryExecutionScope(this.refs.candidateQueryScope.value));
    this.refs.candidateQueryParams.addEventListener("input", event => this.handleCandidateQueryParameterInput(event));
    this.refs.candidateQueryParams.addEventListener("change", event => this.handleCandidateQueryParameterInput(event));
    this.refs.runCandidateQuery.addEventListener("click", () => {
      void this.runSelectedCandidateQuery();
    });
    this.refs.checkCandidates.addEventListener("click", () => {
      void this.checkCandidateQueryPreview();
    });
    this.refs.clearCandidates.addEventListener("click", () => this.clearCandidateQueryPreview({ announce: true }));
    this.refs.resetView.addEventListener("click", () => this.resetView());
    this.refs.markSuspected.addEventListener("click", () => this.openActiveFlagModal());
    this.refs.markConfirmed.addEventListener("click", () => this.openConfirmModal());
    this.refs.dismissSuspected.addEventListener("click", () => this.openDismissModal());
    this.refs.runAnomalyRanking.addEventListener("click", () => {
      void this.runBurstAnomalyRanking();
    });
    this.refs.runBurstFeatureSpace.addEventListener("click", () => {
      void this.runBurstFeatureSpace();
    });
    this.refs.generateReport.addEventListener("click", () => this.openReportModal());
    this.refs.exportReviewedCsv.addEventListener("click", () => {
      void this.exportReviewedCsv();
    });
    this.refs.outputLinks.addEventListener("click", event => {
      void this.handleAuthenticatedArtifactClick(event);
    });
    this.refs.reportLinks.addEventListener("click", event => {
      void this.handleAuthenticatedArtifactClick(event);
    });
    this.refs.undo.addEventListener("click", async () => {
      await this.undoCurrentHead();
    });
    this.refs.resumeHistory.addEventListener("click", () => this.openResumeModal());
    this.refs.slider.addEventListener("pointerdown", () => {
      this.temporalSliderEngaged = true;
      this.scheduleTemporalFocusRender();
    });
    this.refs.slider.addEventListener("input", () => {
      this.currentTimeMs = Number(this.refs.slider.value) || 0;
      this.updateTimeLabel();
      if (this.temporalSliderEngaged) {
        this.scheduleTemporalFocusRender();
      }
    });
    const finishTemporalSliderInteraction = () => {
      if (!this.temporalSliderEngaged) return;
      this.temporalSliderEngaged = false;
      this.scheduleTemporalFocusRender();
    };
    this.refs.slider.addEventListener("pointerup", finishTemporalSliderInteraction);
    this.refs.slider.addEventListener("pointercancel", finishTemporalSliderInteraction);
    this.refs.slider.addEventListener("blur", finishTemporalSliderInteraction);
    this.refs.thresholdPane.addEventListener("click", event => this.handleThresholdPaneClick(event));
    this.refs.thresholdPane.addEventListener("change", event => this.handleThresholdPaneChange(event));
    this.refs.thresholdPane.addEventListener("focusin", event => this.handleThresholdPaneFocusIn(event));
    this.refs.tableMode.addEventListener("change", () => {
      this.saveUiState();
      this.renderTableSheet();
    });
    this.refs.tableFilter.addEventListener("input", () => {
      this.saveUiState();
      this.renderTableSheet();
    });
    this.refs.tableSort.addEventListener("change", () => {
      this.saveUiState();
      this.renderTableSheet();
    });
    this.refs.tableSortDirection.addEventListener("click", () => {
      const nextDirection = this.refs.tableSortDirection.dataset.direction === "desc" ? "asc" : "desc";
      this.refs.tableSortDirection.dataset.direction = nextDirection;
      this.refs.tableSortDirection.textContent = nextDirection === "desc" ? "Descending" : "Ascending";
      this.saveUiState();
      this.renderTableSheet();
    });
    this.refs.segmentClear.addEventListener("click", () => this.clearTableSelection());
    this.refs.segmentConfirmed.addEventListener("click", () => {
      const selection = this.getCurrentSegmentSelection();
      this.openConfirmModal(selection?.fixes || []);
    });
    this.refs.tableWrap.addEventListener("click", event => this.handleTableWrapClick(event));
    this.refs.tableWrap.addEventListener("scroll", () => this.handleTableWrapScroll());
    document.addEventListener("keydown", event => {
      if (event.key === "Escape" && this.mapRangeAwaitingEnd) {
        this.clearTableSelection();
        this.setStatus("Track range selection cancelled.");
      }
    });
    this.refs.anomalyRanking.addEventListener("click", event => {
      void this.handleAnomalyRankingClick(event);
    });
    this.refs.burstFeatureSpace.addEventListener("click", event => {
      void this.handleBurstFeatureSpaceClick(event);
    });

    this.refs.issueClose.addEventListener("click", () => this.closeModal(this.refs.issueModal, this.refs.issueSubmit));
    this.refs.issueSubmit.addEventListener("click", async () => this.submitIssueAction());
    this.refs.issueScope.addEventListener("change", () => this.updateIndividualQueueIssueScope());
    this.refs.issueBurstList.addEventListener("change", event => {
      const input = event.target.closest("input[data-issue-burst-id]");
      if (input) {
        this.setIssueBurstIncluded(input.dataset.issueBurstId, input.checked);
      }
    });
    this.refs.confirmClose.addEventListener("click", () => this.closeModal(this.refs.confirmModal, this.refs.confirmSubmit));
    this.refs.confirmSubmit.addEventListener("click", async () => this.submitConfirmIssues());
    this.refs.dismissClose.addEventListener("click", () => this.closeModal(this.refs.dismissModal, this.refs.dismissSubmit));
    this.refs.dismissSubmit.addEventListener("click", async () => this.submitDismissIssues());
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
    this.refs.reportSnapshotUnit.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportBasemap.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportSnapshotLimit.addEventListener("input", () => this.renderReportSelection());
    this.refs.reportSpreadIndividuals.addEventListener("change", () => this.renderReportSelection());
    this.refs.reportSubmit.addEventListener("click", async () => this.submitGenerateReport());
    this.refs.resumeClose.addEventListener("click", () => this.closeModal(this.refs.resumeModal, this.refs.resumeSubmit));
    this.refs.resumeSubmit.addEventListener("click", async () => this.submitResumeHistory());

    for (const modal of [
      this.refs.issueModal,
      this.refs.confirmModal,
      this.refs.dismissModal,
      this.refs.reportModal,
      this.refs.resumeModal,
    ]) {
      modal.addEventListener("click", (event) => {
        if (event.target === modal) {
          const submitButton = modal === this.refs.issueModal
            ? this.refs.issueSubmit
            : modal === this.refs.confirmModal
              ? this.refs.confirmSubmit
              : modal === this.refs.dismissModal
                ? this.refs.dismissSubmit
              : modal === this.refs.reportModal
                ? this.refs.reportSubmit
                : this.refs.resumeSubmit;
          this.closeModal(modal, submitButton);
        }
      });
    }
  }

  async fetchResponse(url, options) {
    if (window.vibecleaningAuth?.fetch) {
      return window.vibecleaningAuth.fetch(url, options);
    }
    return fetch(url, { ...options, credentials: "same-origin" });
  }

  async fetchJSON(url, options) {
    const response = await this.fetchResponse(url, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(payload.error || `${response.status} ${response.statusText}`);
      error.status = response.status;
      error.payload = payload;
      throw error;
    }
    return payload;
  }

  async loadBinaryMovement({ familyName, studyName, datasetId, data = this.data } = {}) {
    if (!MOVEMENT_APP_CONFIG.rdsSource || !data) {
      return;
    }
    const controller = this.beginRequest("binaryFixes");
    const response = await this.fetchResponse(
      `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/dataset/${encodeURIComponent(datasetId)}/fixes-binary`,
      { signal: controller.signal },
    );
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || `${response.status} ${response.statusText}`);
    }
    const binary = parseMovementBinary(await response.arrayBuffer());
    if (
      this.requestControllers.binaryFixes !== controller
      || familyName !== this.currentFamily
      || studyName !== this.currentStudy
      || datasetId !== this.currentDatasetId
    ) {
      return;
    }
    if (
      data.sourceSignature
      && binary.header.source_bundle_signature
      && data.sourceSignature !== binary.header.source_bundle_signature
    ) {
      throw new Error("The indexed RDS bundle changed while its map data was loading.");
    }
    const sourceIndexes = binary.arrays.line_source_indexes;
    const targetIndexes = binary.arrays.line_target_indexes;
    const positions = binary.arrays.positions;
    const lineSourcePositions = new Float64Array(sourceIndexes.length * 2);
    const lineTargetPositions = new Float64Array(targetIndexes.length * 2);
    for (let index = 0; index < sourceIndexes.length; index += 1) {
      const sourceOffset = Number(sourceIndexes[index]) * 2;
      const targetOffset = Number(targetIndexes[index]) * 2;
      lineSourcePositions[index * 2] = positions[sourceOffset];
      lineSourcePositions[(index * 2) + 1] = positions[sourceOffset + 1];
      lineTargetPositions[index * 2] = positions[targetOffset];
      lineTargetPositions[(index * 2) + 1] = positions[targetOffset + 1];
    }
    binary.lineSourcePositions = lineSourcePositions;
    binary.lineTargetPositions = lineTargetPositions;
    binary.individualRanges = new Map();
    let rangeStart = 0;
    while (rangeStart < Number(binary.header.row_count)) {
      const code = Number(binary.arrays.individual_codes[rangeStart]);
      let rangeEnd = rangeStart + 1;
      while (
        rangeEnd < Number(binary.header.row_count)
        && Number(binary.arrays.individual_codes[rangeEnd]) === code
      ) rangeEnd += 1;
      binary.individualRanges.set(code, [rangeStart, rangeEnd]);
      rangeStart = rangeEnd;
    }
    binary.renderCache = null;
    data.binaryMovement = binary;
    data.binaryMapReady = true;
    for (const field of data.colorFields) {
      const stats = binary.header.color_stats?.[field.key];
      if (field.kind === "numeric" && stats) {
        data.colorStyles.set(field.key, {
          kind: "numeric",
          range: {
            min: Number(stats.q01),
            max: Number(stats.q99),
            observedMin: Number(stats.observed_min),
            observedMax: Number(stats.observed_max),
          },
        });
      } else if (field.kind === "boolean") {
        data.colorStyles.set(field.key, { kind: "boolean" });
      }
    }
  }

  binaryFixAt(index, { remember = true } = {}) {
    const binary = this.data?.binaryMovement;
    const arrays = binary?.arrays;
    const pointIndex = Number(index);
    if (!arrays || !Number.isInteger(pointIndex) || pointIndex < 0 || pointIndex >= Number(binary.header.row_count)) {
      return null;
    }
    const artifact = String(binary.header.artifacts?.[Number(arrays.artifact_codes[pointIndex])] || "");
    const sourceRow = Number(arrays.source_rows[pointIndex]);
    const fixKey = `file:${artifact}#row:${sourceRow}`;
    const statusCode = Number(arrays.review_status[pointIndex]);
    const fix = {
      fixKey,
      individual: String(binary.header.individuals?.[Number(arrays.individual_codes[pointIndex])] || ""),
      setName: String(binary.header.implicit_set || "train"),
      timeMs: Number(arrays.time_ms[pointIndex]),
      position: [Number(arrays.positions[pointIndex * 2]), Number(arrays.positions[(pointIndex * 2) + 1])],
      attributes: {
        individual: String(binary.header.individuals?.[Number(arrays.individual_codes[pointIndex])] || ""),
        step_length_m: Number(arrays.step_length_m[pointIndex]),
        speed_mps: Number(arrays.speed_mps[pointIndex]),
        time_delta_s: Number(arrays.time_delta_s[pointIndex]),
        turn_angle_deg: Number(arrays.turn_angle_deg[pointIndex]),
        is_outlier: Boolean(arrays.is_outlier[pointIndex]),
      },
      review: { status: statusCode === 2 ? "confirmed" : statusCode === 1 ? "suspected" : "", issues: [], effectiveIssues: [] },
      segments: [],
      analyticallyExcluded: statusCode === 2,
      sourceFlags: [],
      sourceArtifact: artifact,
      sourceRow,
      sourceBurst: Number(arrays.burst_values[pointIndex]),
      binaryIndex: pointIndex,
    };
    if (remember) {
      this.data.fixByKey.set(fixKey, fix);
    }
    return fix;
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

  renderReportLinks() {
    this.refs.reportLinks.innerHTML = this.lastReportLinks.map(link => (
      `<a href="${link.href}" target="_blank" rel="noreferrer" data-authenticated-artifact="open" data-artifact-name="${escapeHtml(link.logicalName || "")}">${escapeHtml(link.label)}</a>`
    )).join("");
  }

  async handleAuthenticatedArtifactClick(event) {
    if (MOVEMENT_APP_CONFIG.mode !== "slim_movement") {
      return;
    }
    const link = event.target.closest("a[data-authenticated-artifact]");
    if (!link) {
      return;
    }
    event.preventDefault();
    const action = link.dataset.authenticatedArtifact || "open";
    const artifactName = link.dataset.artifactName || "movement-output";
    const popup = action === "open" ? window.open("", "_blank") : null;
    if (popup) {
      popup.opener = null;
      popup.document.title = "Loading report…";
      popup.document.body.textContent = "Loading report…";
    }
    try {
      const response = await this.fetchResponse(link.href, { cache: "no-store" });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || `${response.status} ${response.statusText}`);
      }
      const blob = await response.blob();
      const objectUrl = URL.createObjectURL(blob);
      if (action === "download") {
        const downloadLink = document.createElement("a");
        downloadLink.href = objectUrl;
        downloadLink.download = artifactName;
        document.body.append(downloadLink);
        downloadLink.click();
        downloadLink.remove();
        setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
      } else if (popup) {
        popup.location.replace(objectUrl);
      } else {
        URL.revokeObjectURL(objectUrl);
        throw new Error("The report window was blocked by the browser.");
      }
    } catch (error) {
      if (popup) {
        popup.close();
      }
      this.setStatus(`Could not retrieve ${artifactName}: ${error.message}`, true);
    }
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
      this.cancelRequest("reviewProjection");
      this.cancelRequest("detail");
      this.cancelRequest("suspicious");
      this.cancelRequest("confirmed");
      this.cancelRequest("reportDetail");
      this.cancelRequest("osm");
      this.cancelRequest("candidateQuery");
      this.cancelRequest("anomalyRanking");
      this.cancelRequest("issueBurstScores");
      this.cancelRequest("burstFeatureSpace");
      this.cancelRequest("analysisHistory");
      return;
    }
    if (level === "study") {
      this.cancelRequest("study");
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("reviewProjection");
      this.cancelRequest("detail");
      this.cancelRequest("suspicious");
      this.cancelRequest("confirmed");
      this.cancelRequest("reportDetail");
      this.cancelRequest("osm");
      this.cancelRequest("candidateQuery");
      this.cancelRequest("anomalyRanking");
      this.cancelRequest("issueBurstScores");
      this.cancelRequest("burstFeatureSpace");
      this.cancelRequest("analysisHistory");
      return;
    }
    if (level === "dataset") {
      this.cancelRequest("dataset");
      this.cancelRequest("overview");
      this.cancelRequest("reviewProjection");
      this.cancelRequest("detail");
      this.cancelRequest("suspicious");
      this.cancelRequest("confirmed");
      this.cancelRequest("reportDetail");
      this.cancelRequest("osm");
      this.cancelRequest("candidateQuery");
      this.cancelRequest("anomalyRanking");
      this.cancelRequest("issueBurstScores");
      this.cancelRequest("burstFeatureSpace");
      this.cancelRequest("analysisHistory");
      return;
    }
    if (level === "artifact") {
      this.cancelRequest("overview");
      this.cancelRequest("reviewProjection");
      this.cancelRequest("detail");
      this.cancelRequest("suspicious");
      this.cancelRequest("confirmed");
      this.cancelRequest("reportDetail");
      this.cancelRequest("osm");
      this.cancelRequest("candidateQuery");
      this.cancelRequest("anomalyRanking");
      this.cancelRequest("issueBurstScores");
      this.cancelRequest("burstFeatureSpace");
      this.cancelRequest("analysisHistory");
    }
  }

  osmScopeFromPoint(fixOrLonLat, radiusM) {
    return OSM_INTERACTION?.scopeFromPoint(fixOrLonLat, radiusM) || null;
  }

  osmScopeFromMapBounds() {
    return OSM_INTERACTION?.scopeFromMapBounds(this.map) || null;
  }

  osmScopeFromSegmentBounds(fixes, paddingM) {
    return OSM_INTERACTION?.scopeFromSegmentBounds(fixes, paddingM) || null;
  }

  async queryOsmContext(query, options = {}) {
    if (!OSM_INTERACTION) {
      throw new Error("OSM context tools are not available in this application.");
    }
    const controller = this.beginRequest("osm");
    this.osmContextStatus = "loading";
    this.osmContextError = "";
    this.setStatus("Loading OSM context...");
    this.updateActionButtons();
    try {
      const payload = await OSM_INTERACTION.fetchOsmContext(query, {
        ...options,
        signal: controller.signal,
      });
      if (this.requestControllers.osm !== controller) {
        return null;
      }
      this.osmContext = payload;
      this.osmContextError = "";
      this.osmContextStatus = "loaded";
      this.setStatus(this.formatOsmContextStatus(payload));
      this.renderLayers();
      this.updateActionButtons();
      return payload;
    } catch (error) {
      if (this.isAbortError(error)) {
        return null;
      }
      if (this.requestControllers.osm === controller) {
        this.osmContext = null;
        this.osmContextError = error.message;
        this.osmContextStatus = "error";
        this.setStatus(`OSM context failed: ${error.message}`, true);
        this.renderLayers();
        this.updateActionButtons();
      }
      throw error;
    }
  }

  clearOsmContext({ render = true, announce = false } = {}) {
    this.cancelRequest("osm");
    this.osmContext = null;
    this.osmContextError = "";
    this.osmContextStatus = "idle";
    if (announce && this.refs) {
      this.setStatus("OSM context cleared.");
    }
    if (render && this.refs) {
      this.renderLayers();
      this.updateActionButtons();
    }
  }

  getOsmContextMetadata() {
    return this.osmContext?.metadata || null;
  }

  getOsmDeckLayers() {
    if (!OSM_INTERACTION) {
      return [];
    }
    return OSM_INTERACTION.buildOsmDeckLayers(this.osmContext, {
      deckInstance: window.deck,
      idPrefix: "movement-osm-context",
    });
  }

  formatOsmContextStatus(payload) {
    const metadata = payload?.metadata || {};
    const featureCount = Number(metadata.feature_count ?? payload?.features?.length ?? 0) || 0;
    const omittedCount = Number(metadata.omitted_feature_count || 0) || 0;
    const scopeType = metadata.scope?.type || "scope";
    const omittedText = omittedCount > 0 ? `; ${formatCount(omittedCount)} omitted` : "";
    return `Loaded OSM context: ${formatCount(featureCount)} features from ${scopeType}${omittedText}.`;
  }

  makeEmptyCandidateQueryPreview() {
    return {
      analysisId: "",
      matchKeys: new Set(),
      candidates: [],
      evidenceByFixKey: new Map(),
      status: "idle",
      warnings: [],
      candidateCount: 0,
      returnedCount: 0,
    };
  }

  candidateQueryKey(query) {
    return `${String(query?.query_id || "")}::${String(query?.version || "")}`;
  }

  getSelectedCandidateQuery() {
    const key = this.candidateQueryLibrary.selectedKey;
    return (this.candidateQueryLibrary.queries || []).find(query => this.candidateQueryKey(query) === key) || null;
  }

  defaultCandidateQueryExecutionScope(query) {
    const requiredFields = Array.isArray(query?.required_fields) ? query.required_fields : [];
    if (requiredFields.some(field => String(field || "").startsWith("osm:"))) {
      return "current_individual";
    }
    return query?.evaluator?.type === "fix_osm_proximity" ? "current_individual" : "whole_study";
  }

  candidateQueryParameterDescriptors(query) {
    const parameters = query?.parameters && typeof query.parameters === "object" && !Array.isArray(query.parameters)
      ? query.parameters
      : {};
    return Object.entries(parameters).map(([name, rawSpec]) => {
      const isSpecObject = rawSpec && typeof rawSpec === "object" && !Array.isArray(rawSpec);
      const spec = isSpecObject ? rawSpec : { default: rawSpec };
      const rawDefault = Object.prototype.hasOwnProperty.call(spec, "default")
        ? spec.default
        : Object.prototype.hasOwnProperty.call(spec, "value")
          ? spec.value
          : "";
      let type = String(spec.type || spec.kind || "").trim().toLowerCase();
      if (!["number", "string", "boolean"].includes(type)) {
        if (typeof rawDefault === "number") {
          type = "number";
        } else if (typeof rawDefault === "boolean") {
          type = "boolean";
        } else {
          type = "string";
        }
      }
      return {
        name: String(name),
        label: String(spec.label || name),
        description: String(spec.description || ""),
        type,
        defaultValue: rawDefault,
      };
    }).filter(item => item.name);
  }

  defaultCandidateQueryParameterValues(query) {
    const values = {};
    for (const descriptor of this.candidateQueryParameterDescriptors(query)) {
      if (descriptor.type === "boolean") {
        values[descriptor.name] = Boolean(descriptor.defaultValue);
      } else if (descriptor.defaultValue === null || descriptor.defaultValue === undefined) {
        values[descriptor.name] = "";
      } else {
        values[descriptor.name] = String(descriptor.defaultValue);
      }
    }
    return values;
  }

  async loadCandidateQueryLibrary() {
    const controller = this.beginRequest("queryLibrary");
    this.candidateQueryLibrary.status = "loading";
    this.candidateQueryLibrary.error = "";
    this.renderCandidateQueryLibraryControls();
    try {
      const payload = await this.fetchJSON("/api/query-library/queries?app=movement", { signal: controller.signal });
      if (this.requestControllers.queryLibrary !== controller) {
        return;
      }
      const queries = Array.isArray(payload.queries) ? payload.queries : [];
      this.candidateQueryLibrary.status = "loaded";
      this.candidateQueryLibrary.queries = queries;
      this.candidateQueryLibrary.error = "";
      const currentStillExists = queries.some(query => this.candidateQueryKey(query) === this.candidateQueryLibrary.selectedKey);
      const selected = currentStillExists ? this.getSelectedCandidateQuery() : queries[0] || null;
      this.candidateQueryLibrary.selectedKey = selected ? this.candidateQueryKey(selected) : "";
      this.candidateQueryLibrary.parameterValues = selected ? this.defaultCandidateQueryParameterValues(selected) : {};
      if (!currentStillExists) {
        this.candidateQueryLibrary.executionScope = this.defaultCandidateQueryExecutionScope(selected);
      }
      this.renderCandidateQueryLibraryControls();
      this.updateActionButtons();
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      this.candidateQueryLibrary.status = "error";
      this.candidateQueryLibrary.error = error.message;
      this.candidateQueryLibrary.queries = [];
      this.candidateQueryLibrary.selectedKey = "";
      this.candidateQueryLibrary.parameterValues = {};
      this.renderCandidateQueryLibraryControls();
      this.updateActionButtons();
    } finally {
      if (this.requestControllers.queryLibrary === controller) {
        this.requestControllers.queryLibrary = null;
      }
    }
  }

  selectCandidateQuery(key) {
    this.candidateQueryLibrary.selectedKey = String(key || "");
    const query = this.getSelectedCandidateQuery();
    this.candidateQueryLibrary.parameterValues = query ? this.defaultCandidateQueryParameterValues(query) : {};
    this.candidateQueryLibrary.executionScope = this.defaultCandidateQueryExecutionScope(query);
    this.renderCandidateQueryLibraryControls();
    this.updateActionButtons();
  }

  selectCandidateQueryExecutionScope(value) {
    const allowed = new Set(["whole_study", "current_individual", "all_individuals_per_individual"]);
    const nextValue = String(value || "whole_study");
    this.candidateQueryLibrary.executionScope = allowed.has(nextValue) ? nextValue : "whole_study";
    this.renderCandidateQueryLibraryControls();
    this.updateActionButtons();
  }

  handleCandidateQueryParameterInput(event) {
    const input = event.target.closest?.("[data-param-name]");
    if (!input) {
      return;
    }
    const name = String(input.dataset.paramName || "");
    if (!name) {
      return;
    }
    this.candidateQueryLibrary.parameterValues = {
      ...(this.candidateQueryLibrary.parameterValues || {}),
      [name]: input.type === "checkbox" ? input.checked : input.value,
    };
  }

  renderCandidateQueryLibraryControls() {
    if (!this.refs?.candidateQuerySelect || !this.refs?.candidateQueryScope || !this.refs?.candidateQueryMeta || !this.refs?.candidateQueryParams) {
      return;
    }
    const select = this.refs.candidateQuerySelect;
    const scopeSelect = this.refs.candidateQueryScope;
    const queries = this.candidateQueryLibrary.queries || [];
    select.innerHTML = "";
    scopeSelect.value = this.candidateQueryLibrary.executionScope || "whole_study";
    if (this.candidateQueryLibrary.status === "loading") {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "Loading queries...";
      select.appendChild(option);
      select.disabled = true;
      scopeSelect.disabled = true;
      this.refs.candidateQueryMeta.textContent = "Loading saved movement queries...";
      this.refs.candidateQueryParams.innerHTML = "";
      this.refs.candidateQueryParams.classList.add("hidden");
      return;
    }
    if (this.candidateQueryLibrary.status === "error") {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "Could not load queries";
      select.appendChild(option);
      select.disabled = true;
      scopeSelect.disabled = true;
      this.refs.candidateQueryMeta.textContent = `Could not load query library: ${this.candidateQueryLibrary.error}`;
      this.refs.candidateQueryParams.innerHTML = "";
      this.refs.candidateQueryParams.classList.add("hidden");
      return;
    }
    if (!queries.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No saved movement queries";
      select.appendChild(option);
      select.disabled = true;
      scopeSelect.disabled = true;
      this.refs.candidateQueryMeta.textContent = "No saved movement candidate queries.";
      this.refs.candidateQueryParams.innerHTML = "";
      this.refs.candidateQueryParams.classList.add("hidden");
      return;
    }
    for (const query of queries) {
      const option = document.createElement("option");
      option.value = this.candidateQueryKey(query);
      option.textContent = `${query.name || query.query_id} v${query.version}`;
      select.appendChild(option);
    }
    select.disabled = false;
    scopeSelect.disabled = false;
    if (!this.candidateQueryLibrary.selectedKey || !queries.some(query => this.candidateQueryKey(query) === this.candidateQueryLibrary.selectedKey)) {
      this.candidateQueryLibrary.selectedKey = this.candidateQueryKey(queries[0]);
      this.candidateQueryLibrary.parameterValues = this.defaultCandidateQueryParameterValues(queries[0]);
    }
    select.value = this.candidateQueryLibrary.selectedKey;
    const selected = this.getSelectedCandidateQuery();
    if (!selected) {
      this.refs.candidateQueryMeta.textContent = "Select a candidate query.";
      return;
    }
    const evaluatorType = selected.evaluator?.type || "unknown";
    const requiredFields = Array.isArray(selected.required_fields) && selected.required_fields.length
      ? selected.required_fields.join(", ")
      : "none";
    const scopeHint = evaluatorType === "fix_osm_proximity"
      ? " | OSM scope: select one individual, or choose all individuals separately"
      : "";
    this.refs.candidateQueryMeta.innerHTML = `
      <strong>${escapeHtml(selected.name || selected.query_id)}</strong>
      ${escapeHtml(selected.description || "No description.")}
      v${escapeHtml(String(selected.version || ""))}
      | ${escapeHtml(selected.candidate_kind || "candidate")}
      | evaluator ${escapeHtml(evaluatorType)}
      | fields ${escapeHtml(requiredFields)}
      ${escapeHtml(scopeHint)}
    `;
    const descriptors = this.candidateQueryParameterDescriptors(selected);
    if (!descriptors.length) {
      this.refs.candidateQueryParams.innerHTML = "";
      this.refs.candidateQueryParams.classList.add("hidden");
      return;
    }
    const values = this.candidateQueryLibrary.parameterValues || {};
    this.refs.candidateQueryParams.innerHTML = descriptors.map(descriptor => {
      const rawValue = values[descriptor.name] ?? descriptor.defaultValue ?? "";
      const title = descriptor.description ? ` title="${escapeHtml(descriptor.description)}"` : "";
      if (descriptor.type === "boolean") {
        return `
          <label${title}>${escapeHtml(descriptor.label)}
            <input type="checkbox" data-param-name="${escapeHtml(descriptor.name)}" ${rawValue ? "checked" : ""}>
          </label>
        `;
      }
      const inputType = descriptor.type === "number" ? "number" : "text";
      const step = descriptor.type === "number" ? ' step="any"' : "";
      return `
        <label${title}>${escapeHtml(descriptor.label)}
          <input type="${inputType}"${step} data-param-name="${escapeHtml(descriptor.name)}" value="${escapeHtml(String(rawValue))}">
        </label>
      `;
    }).join("");
    this.refs.candidateQueryParams.classList.remove("hidden");
  }

  getCandidateQueryParameterValues(query) {
    const values = {};
    const currentValues = this.candidateQueryLibrary.parameterValues || {};
    for (const descriptor of this.candidateQueryParameterDescriptors(query)) {
      const rawValue = currentValues[descriptor.name] ?? descriptor.defaultValue ?? "";
      if (descriptor.type === "boolean") {
        values[descriptor.name] = Boolean(rawValue);
      } else if (descriptor.type === "number") {
        const numericValue = Number(rawValue);
        values[descriptor.name] = Number.isFinite(numericValue) ? numericValue : rawValue;
      } else {
        values[descriptor.name] = String(rawValue);
      }
    }
    return values;
  }

  getCandidateQueryCurrentIndividual() {
    if (!this.data) {
      return "";
    }
    const selectedIndividuals = this.getSelectedIndividuals();
    return selectedIndividuals.length === 1 ? selectedIndividuals[0] : "";
  }

  getCandidateQueryExecutionScope() {
    const scope = this.candidateQueryLibrary.executionScope || "whole_study";
    if (scope === "current_individual") {
      const individual = this.getCandidateQueryCurrentIndividual();
      return individual ? { type: "current_individual", individual } : null;
    }
    if (scope === "all_individuals_per_individual") {
      return { type: "all_individuals_per_individual" };
    }
    return { type: "whole_study" };
  }

  getCandidateQueryMatchKeys() {
    if (!this.data || !(this.candidateQueryPreview?.matchKeys instanceof Set)) {
      return new Set();
    }
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    const visibleSetNames = this.getVisibleSetNames();
    return new Set(
      [...this.candidateQueryPreview.matchKeys].filter(fixKey => {
        const fix = this.data.fixByKey.get(fixKey);
        return Boolean(fix && visibleIndividuals.has(fix.individual) && visibleSetNames.has(fix.setName));
      }),
    );
  }

  getCandidateQueryReturnedMatchKeys() {
    if (!(this.candidateQueryPreview?.matchKeys instanceof Set)) {
      return new Set();
    }
    return new Set(this.candidateQueryPreview.matchKeys);
  }

  clearCandidateQueryPreview({ render = true, announce = false } = {}) {
    this.cancelRequest("candidateQuery");
    this.candidateQueryPreview = this.makeEmptyCandidateQueryPreview();
    if (announce && this.refs) {
      this.setStatus("Candidate preview cleared.");
    }
    if (render && this.refs) {
      this.renderLayers();
      this.updateActionButtons();
    }
  }

  makeBurstScoreMap(items) {
    const scores = new Map();
    for (const burst of Array.isArray(items) ? items : []) {
      const burstId = String(burst?.burst_id || burst?.burstId || "");
      const score = finiteOrNull(burst?.anomaly_score ?? burst?.anomalyScore);
      if (burstId && score !== null) {
        scores.set(burstId, score);
      }
    }
    return scores;
  }

  makeEmptyAnomalyRanking() {
    return {
      analysisId: "",
      status: "idle",
      rankedIndividuals: [],
      burstScores: new Map(),
      warnings: [],
      burstGap: null,
      modelFit: null,
      rankingSummary: null,
      rankingMethod: "",
      createdAt: "",
      user: "",
      loadedFromHistory: false,
      restoreError: "",
    };
  }

  clearAnomalyRanking({ render = true } = {}) {
    this.cancelRequest("anomalyRanking");
    this.cancelRequest("issueBurstScores");
    this.anomalyRanking = this.makeEmptyAnomalyRanking();
    this.focusedRankingBurst = null;
    if (this.individualReviewQueue) {
      this.individualReviewQueue.appliedRankingAnalysisId = "";
      this.individualReviewQueue.pendingRankingAnalysisId = "";
      if (this.individualReviewQueue.orderMode === "ranking") {
        this.individualReviewQueue.orderMode = "dataset";
      }
    }
    if (render && this.refs) {
      this.renderAnomalyRanking();
      this.renderIndividuals();
      this.renderLayers();
      this.updateActionButtons();
    }
  }

  makeEmptyBurstFeatureSpace() {
    return {
      analysisId: "",
      status: "idle",
      featureSet: "movement_only",
      points: [],
      selectedBurstId: "",
      warnings: [],
      burstGap: null,
      featureMatrix: null,
      pca: null,
      createdAt: "",
      user: "",
      loadedFromHistory: false,
    };
  }

  clearBurstFeatureSpace({ render = true } = {}) {
    this.cancelRequest("burstFeatureSpace");
    this.burstFeatureSpace = this.makeEmptyBurstFeatureSpace();
    if (render && this.refs) {
      this.renderBurstFeatureSpace();
      this.updateActionButtons();
    }
  }

  async restoreSavedAnalyses() {
    if (!this.data || !this.currentFamily || !this.currentStudy || !this.currentDatasetId || !this.currentArtifact) {
      return;
    }
    const loadedRanking = this.hasCompatibleIndividualQueueRanking()
      ? this.anomalyRanking
      : null;
    const loadedRankingAnalysisId = String(loadedRanking?.analysisId || "");
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    const controller = this.beginRequest("analysisHistory");
    if (!loadedRanking) {
      this.anomalyRanking = {
        ...this.makeEmptyAnomalyRanking(),
        status: "checking",
      };
      this.renderAnomalyRanking();
      this.renderIndividuals();
    }
    const params = new URLSearchParams({
      dataset_id: datasetId,
      logical_name: artifactName,
      burst_gap_mode: this.getBurstGapMode(),
      burst_gap_seconds: String(this.getBurstGapSeconds()),
      burst_gap_quantile: String(this.getBurstGapQuantile()),
      feature_set: this.getAnomalyFeatureSet(),
      ranking_method: this.getRankingMethod(),
    });
    try {
      const history = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/analyses?${params.toString()}`,
        { signal: controller.signal },
      );
      if (
        this.requestControllers.analysisHistory !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
      ) {
        return;
      }
      const items = Array.isArray(history?.items) ? history.items : [];
      const byId = new Map(items.map(item => [String(item?.analysis_id || ""), item]));
      const latest = history?.latest_compatible_by_action || {};
      const ranking = byId.get(String(latest.run_burst_anomaly_ranking || ""));
      const featureSpace = byId.get(String(latest.run_burst_feature_space || ""));
      const restored = [];
      const tasks = [];
      if (ranking) {
        const rankingAnalysisId = String(ranking.analysis_id || "");
        this.anomalyRanking = (
          loadedRanking
          && rankingAnalysisId
          && rankingAnalysisId === loadedRankingAnalysisId
        )
          ? {
            ...loadedRanking,
            createdAt: String(ranking.created_at || loadedRanking.createdAt || ""),
            user: String(ranking.user || loadedRanking.user || ""),
            loadedFromHistory: true,
          }
          : {
            ...this.makeEmptyAnomalyRanking(),
            analysisId: rankingAnalysisId,
            status: "available",
            rankingMethod: String(
              ranking?.parameters?.ranking_method || this.getRankingMethod(),
            ),
            createdAt: String(ranking.created_at || ""),
            user: String(ranking.user || ""),
            loadedFromHistory: true,
          };
      } else {
        this.anomalyRanking = {
          ...this.makeEmptyAnomalyRanking(),
          status: "unavailable",
        };
      }
      if (featureSpace && MOVEMENT_APP_CONFIG.featureSpace) {
        tasks.push(this.restoreSavedBurstFeatureSpace(featureSpace, controller.signal).then(() => {
          restored.push(`feature space from ${formatDateTime(featureSpace.created_at)}`);
        }));
      }
      const results = await Promise.allSettled(tasks);
      if (this.requestControllers.analysisHistory !== controller) {
        return;
      }
      for (const result of results) {
        if (result.status === "rejected" && !this.isAbortError(result.reason)) {
          console.warn("Could not restore saved movement analysis", result.reason);
        }
      }
      this.renderAnomalyRanking();
      this.renderBurstFeatureSpace();
      this.noteCompletedIndividualQueueRanking();
      this.renderIndividuals();
      this.updateActionButtons();
      if (restored.length) {
        this.setStatus(`Restored saved ${restored.join(" and ")}.`);
      }
      if (
        ranking
        && this.individualReviewQueue.mode === "browse"
        && this.refs.sideSheetTabs?.dataset.activeSheet === "ranking"
      ) {
        void this.loadSavedAnomalyRanking();
      }
    } catch (error) {
      if (!this.isAbortError(error)) {
        console.warn("Could not load saved movement analyses", error);
        this.anomalyRanking = loadedRanking || {
          ...this.makeEmptyAnomalyRanking(),
          status: "history_error",
          restoreError: error.message,
        };
        this.renderAnomalyRanking();
        this.renderIndividuals();
      }
    } finally {
      if (this.requestControllers.analysisHistory === controller) {
        this.requestControllers.analysisHistory = null;
      }
    }
  }

  async loadSavedAnomalyRanking() {
    if (!["available", "restore_error"].includes(this.anomalyRanking?.status)) {
      return;
    }
    const metadata = { ...this.anomalyRanking };
    const analysisId = String(metadata.analysisId || "");
    if (!analysisId) {
      return;
    }
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    const controller = this.beginRequest("anomalyRanking");
    this.anomalyRanking = {
      ...metadata,
      status: "restoring",
      restoreError: "",
    };
    this.renderAnomalyRanking();
    this.renderIndividuals();
    this.updateActionButtons();
    try {
      const artifact = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/analysis/${encodeURIComponent(analysisId)}/artifact/burst_anomaly_ranking.json`,
        { signal: controller.signal },
      );
      if (
        this.requestControllers.anomalyRanking !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
      ) {
        return;
      }
      this.anomalyRanking = {
        analysisId,
        status: String(artifact?.run_status || "completed"),
        rankedIndividuals: Array.isArray(artifact?.ranked_individuals) ? artifact.ranked_individuals : [],
        burstScores: this.makeBurstScoreMap(artifact?.scored_bursts),
        warnings: Array.isArray(artifact?.warnings) ? artifact.warnings : [],
        burstGap: artifact?.burst_gap || null,
        modelFit: artifact?.model_fit || artifact?.scorer || null,
        rankingSummary: artifact?.individual_ranking_summary || null,
        rankingMethod: String(
          artifact?.ranking_method
          || artifact?.model_fit?.ranking_method
          || metadata.rankingMethod
          || this.getRankingMethod(),
        ),
        createdAt: String(metadata.createdAt || ""),
        user: String(metadata.user || ""),
        loadedFromHistory: true,
        restoreError: "",
      };
      this.noteCompletedIndividualQueueRanking();
      this.renderAnomalyRanking();
      this.renderIndividuals();
      this.updateActionButtons();
      this.setStatus(
        metadata.createdAt
          ? `Loaded saved burst ranking from ${formatDateTime(metadata.createdAt)}.`
          : "Loaded saved burst ranking.",
      );
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      if (this.requestControllers.anomalyRanking === controller) {
        this.anomalyRanking = {
          ...metadata,
          status: "restore_error",
          restoreError: error.message,
        };
        this.renderAnomalyRanking();
        this.renderIndividuals();
        this.updateActionButtons();
        this.setStatus(`Could not load the saved burst ranking: ${error.message}`, true);
      }
    } finally {
      if (this.requestControllers.anomalyRanking === controller) {
        this.requestControllers.anomalyRanking = null;
        this.updateActionButtons();
      }
    }
  }

  async restoreSavedBurstFeatureSpace(item, signal) {
    const analysisId = String(item?.analysis_id || "");
    const artifact = await this.fetchJSON(
      `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis/${encodeURIComponent(analysisId)}/artifact/burst_feature_space.json`,
      { signal },
    );
    const points = Array.isArray(artifact?.points) ? artifact.points : [];
    this.burstFeatureSpace = {
      analysisId,
      status: String(artifact?.run_status || item?.summary?.run_status || "completed"),
      featureSet: String(artifact?.feature_set || item?.parameters?.feature_set || "movement_only"),
      points,
      selectedBurstId: "",
      warnings: Array.isArray(artifact?.warnings) ? artifact.warnings : [],
      burstGap: artifact?.burst_gap || null,
      featureMatrix: artifact?.feature_matrix || null,
      pca: artifact?.pca || null,
      createdAt: String(item?.created_at || ""),
      user: String(item?.user || ""),
      loadedFromHistory: true,
    };
  }

  async runBurstAnomalyRanking({ openRankingSheet = true } = {}) {
    if (!this.data || !this.currentFamily || !this.currentStudy || !this.currentDatasetId || !this.currentArtifact) {
      return;
    }
    const controller = this.beginRequest("anomalyRanking");
    this.anomalyRanking = {
      ...this.makeEmptyAnomalyRanking(),
      status: "loading",
    };
    const featureSet = this.getAnomalyFeatureSet();
    const rankingMethod = this.getRankingMethod();
    const featureSetLabel = this.rankingMethodLabel(rankingMethod);
    this.renderAnomalyRanking();
    this.renderIndividuals();
    this.updateActionButtons();
    this.setStatus(`Running burst anomaly ranking analysis (${featureSetLabel})...`);
    try {
      let result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/run-burst-anomaly-ranking`,
        {
          method: "POST",
          signal: controller.signal,
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            burst_gap_mode: this.getBurstGapMode(),
            burst_gap_seconds: this.getBurstGapSeconds(),
            burst_gap_quantile: this.getBurstGapQuantile(),
            feature_set: featureSet,
            ranking_method: rankingMethod,
            user: this.getUser() || "reviewer",
          }),
        },
      );
      const jobId = String(result?.job_id || "");
      if (jobId) {
        if (openRankingSheet) {
          this.setSideSheet("ranking");
        }
        this.setStatus(
          `Burst anomaly ranking started in the background (${featureSetLabel}). You can keep using the map while it runs.`,
        );
        result = await this.waitForAnomalyRankingJob(jobId, controller);
      }
      if (this.requestControllers.anomalyRanking !== controller) {
        return;
      }
      const summary = result?.summary || {};
      this.anomalyRanking = {
        analysisId: String(result?.analysis_id || result?.analysis?.analysis_id || ""),
        status: String(summary.run_status || "completed"),
        rankedIndividuals: Array.isArray(summary.ranked_individuals) ? summary.ranked_individuals : [],
        burstScores: this.makeBurstScoreMap(summary.scored_bursts),
        warnings: Array.isArray(summary.warnings) ? summary.warnings : [],
        burstGap: summary.burst_gap || null,
        modelFit: summary.model_fit || summary.scorer || null,
        rankingSummary: summary.individual_ranking_summary || null,
        rankingMethod: String(summary.ranking_method || summary.model_fit?.ranking_method || rankingMethod),
        createdAt: String(result?.analysis?.created_at || ""),
        user: String(result?.analysis?.user || ""),
        loadedFromHistory: false,
      };
      if (openRankingSheet) {
        this.setSideSheet("ranking");
      }
      this.noteCompletedIndividualQueueRanking();
      this.renderAnomalyRanking();
      this.renderIndividuals();
      this.updateActionButtons();
      const count = formatCount(this.anomalyRanking.rankedIndividuals.length);
      if (this.anomalyRanking.status === "unresolved") {
        this.setStatus("Burst anomaly ranking could not be resolved for this artifact. Review its warnings below.", true);
      } else {
        const returnedMethod = String(this.anomalyRanking.modelFit?.ranking_method || rankingMethod);
        this.setStatus(`Created ${this.rankingMethodLabel(returnedMethod)} ranking analysis for ${count} individuals.`);
      }
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      if (this.requestControllers.anomalyRanking === controller) {
        this.anomalyRanking = {
          ...this.makeEmptyAnomalyRanking(),
          status: "error",
          warnings: [error.message],
        };
        this.renderAnomalyRanking();
        this.renderIndividuals();
        this.updateActionButtons();
        this.setStatus(`Burst anomaly ranking failed: ${error.message}`, true);
      }
    } finally {
      if (this.requestControllers.anomalyRanking === controller) {
        this.requestControllers.anomalyRanking = null;
        this.updateActionButtons();
      }
    }
  }

  async waitForAnomalyRankingJob(jobId, controller) {
    const startedAt = Date.now();
    while (true) {
      if (controller.signal.aborted) {
        throw new DOMException("Request aborted", "AbortError");
      }
      const job = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis-jobs/${encodeURIComponent(jobId)}`,
        { signal: controller.signal },
      );
      if (job.status === "completed") {
        return job.result || {};
      }
      if (job.status === "failed") {
        throw new Error(job.error || "Burst anomaly ranking failed");
      }
      const elapsedSeconds = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
      this.setStatus(
        `Running burst anomaly ranking in the background (${this.anomalyFeatureSetLabel()}; ${formatCount(elapsedSeconds)} s elapsed)...`,
      );
      await waitForAbortableDelay(ANALYSIS_JOB_POLL_INTERVAL_MS, controller.signal);
    }
  }

  async runBurstFeatureSpace() {
    if (!this.data || !this.currentFamily || !this.currentStudy || !this.currentDatasetId || !this.currentArtifact) {
      return;
    }
    const controller = this.beginRequest("burstFeatureSpace");
    const featureSet = this.getAnomalyFeatureSet();
    const featureSetLabel = this.anomalyFeatureSetLabel(featureSet);
    this.burstFeatureSpace = {
      ...this.makeEmptyBurstFeatureSpace(),
      status: "loading",
      featureSet,
    };
    this.renderBurstFeatureSpace();
    this.updateActionButtons();
    this.setStatus(`Building burst feature space (${featureSetLabel})...`);
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/run-burst-feature-space`,
        {
          method: "POST",
          signal: controller.signal,
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            burst_gap_mode: this.getBurstGapMode(),
            burst_gap_seconds: this.getBurstGapSeconds(),
            burst_gap_quantile: this.getBurstGapQuantile(),
            feature_set: featureSet,
            user: this.getUser() || "reviewer",
          }),
        },
      );
      if (this.requestControllers.burstFeatureSpace !== controller) {
        return;
      }
      const analysisId = String(result?.analysis_id || result?.analysis?.analysis_id || "");
      if (!analysisId) {
        throw new Error("Feature-space analysis did not return an analysis id.");
      }
      const artifact = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis/${encodeURIComponent(analysisId)}/artifact/burst_feature_space.json`,
        { signal: controller.signal },
      );
      if (this.requestControllers.burstFeatureSpace !== controller) {
        return;
      }
      const points = Array.isArray(artifact?.points) ? artifact.points : [];
      const focusedBurstId = String(this.focusedRankingBurst?.burstId || "");
      this.burstFeatureSpace = {
        analysisId,
        status: String(artifact?.run_status || "completed"),
        featureSet: String(artifact?.feature_set || featureSet),
        points,
        selectedBurstId: points.some(point => String(point?.burst_id || "") === focusedBurstId)
          ? focusedBurstId
          : "",
        warnings: Array.isArray(artifact?.warnings) ? artifact.warnings : [],
        burstGap: artifact?.burst_gap || null,
        featureMatrix: artifact?.feature_matrix || null,
        pca: artifact?.pca || null,
        createdAt: String(result?.analysis?.created_at || ""),
        user: String(result?.analysis?.user || ""),
        loadedFromHistory: false,
      };
      this.setSideSheet("feature_space");
      this.renderBurstFeatureSpace();
      this.updateActionButtons();
      if (this.burstFeatureSpace.status === "unresolved") {
        this.setStatus("Burst feature space could not be resolved for this artifact. Review its warnings below.", true);
      } else {
        this.setStatus(`Created ${featureSetLabel} burst feature space for ${formatCount(points.length)} bursts.`);
      }
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      if (this.requestControllers.burstFeatureSpace === controller) {
        this.burstFeatureSpace = {
          ...this.makeEmptyBurstFeatureSpace(),
          status: "error",
          featureSet,
          warnings: [error.message],
        };
        this.renderBurstFeatureSpace();
        this.updateActionButtons();
        this.setStatus(`Burst feature space failed: ${error.message}`, true);
      }
    } finally {
      if (this.requestControllers.burstFeatureSpace === controller) {
        this.requestControllers.burstFeatureSpace = null;
        this.updateActionButtons();
      }
    }
  }

  getBurstFeatureSpacePoint(burstId) {
    const target = String(burstId || "");
    if (!target) {
      return null;
    }
    return (this.burstFeatureSpace?.points || []).find(
      point => String(point?.burst_id || "") === target,
    ) || null;
  }

  selectBurstFeatureSpacePoint(burstId, { render = true } = {}) {
    const point = this.getBurstFeatureSpacePoint(burstId);
    if (!point || !this.burstFeatureSpace) {
      return null;
    }
    this.burstFeatureSpace.selectedBurstId = String(point.burst_id || "");
    if (render) {
      this.renderBurstFeatureSpace();
    }
    return point;
  }

  getBurstFeatureSpaceNeighbors(point) {
    return (Array.isArray(point?.nearest_neighbors) ? point.nearest_neighbors : [])
      .map(item => ({
        burstId: String(item?.burst_id || ""),
        distance: finiteOrNull(item?.distance),
        rank: finiteOrNull(item?.rank),
      }))
      .filter(item => item.burstId);
  }

  renderBurstFeatureSpaceSelection(point) {
    if (!point) {
      return "";
    }
    const neighbors = this.getBurstFeatureSpaceNeighbors(point);
    const main = [
      String(point.burst_id || ""),
      point.individual ? `individual ${point.individual}` : "",
      point.set_name ? `track ${point.set_name}` : "",
      Number.isFinite(Number(point.n_fixes)) ? `${formatCount(point.n_fixes)} fixes` : "",
    ].filter(Boolean);
    return `
      <div class="movement-feature-space-selection" data-role="feature-space-selection">
        <div class="movement-feature-space-selection-main">
          ${main.map((item, index) => (
            index === 0
              ? `<strong class="movement-table-cell-mono">${escapeHtml(item)}</strong>`
              : `<span>${escapeHtml(item)}</span>`
          )).join("")}
        </div>
        ${neighbors.length ? `
          <div class="movement-feature-space-neighbors" data-role="feature-space-neighbors">
            <span>Nearest</span>
            ${neighbors.map(neighbor => {
              const neighborPoint = this.getBurstFeatureSpacePoint(neighbor.burstId);
              const label = neighborPoint?.burst_id || neighbor.burstId;
              const distance = neighbor.distance === null ? "" : ` · ${formatMaybeNumber(neighbor.distance, "")}`;
              return `<button type="button" data-action="focus-feature-space-neighbor" data-burst-id="${escapeHtml(neighbor.burstId)}">${escapeHtml(label)}${escapeHtml(distance)}</button>`;
            }).join("")}
          </div>
        ` : ""}
      </div>
    `;
  }

  renderBurstFeatureSpacePlot(points, selectedPoint) {
    const plottedPoints = points
      .map(point => ({
        point,
        x: finiteOrNull(point?.pc1),
        y: finiteOrNull(point?.pc2),
      }))
      .filter(item => item.x !== null && item.y !== null);
    if (!plottedPoints.length) {
      return '<div class="movement-table-empty">No projected burst points are available.</div>';
    }
    const width = 640;
    const height = 420;
    const padding = 30;
    const xs = plottedPoints.map(item => item.x);
    const ys = plottedPoints.map(item => item.y);
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);
    const minY = Math.min(...ys);
    const maxY = Math.max(...ys);
    const spanX = maxX - minX || 1;
    const spanY = maxY - minY || 1;
    const scaleX = value => padding + ((value - minX) / spanX) * (width - (2 * padding));
    const scaleY = value => height - padding - ((value - minY) / spanY) * (height - (2 * padding));
    const selectedId = String(selectedPoint?.burst_id || "");
    const neighborIds = new Set(this.getBurstFeatureSpaceNeighbors(selectedPoint).map(item => item.burstId));
    const zeroX = minX <= 0 && maxX >= 0 ? scaleX(0) : null;
    const zeroY = minY <= 0 && maxY >= 0 ? scaleY(0) : null;
    return `
      <div class="movement-feature-space-plot">
        <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="PCA projection of burst feature rows">
          ${zeroX === null ? "" : `<line class="movement-feature-space-axis" x1="${zeroX}" x2="${zeroX}" y1="${padding}" y2="${height - padding}"></line>`}
          ${zeroY === null ? "" : `<line class="movement-feature-space-axis" x1="${padding}" x2="${width - padding}" y1="${zeroY}" y2="${zeroY}"></line>`}
          ${plottedPoints.map(item => {
            const burstId = String(item.point?.burst_id || "");
            const isSelected = burstId === selectedId;
            const isNeighbor = neighborIds.has(burstId);
            const classNames = [
              "movement-feature-space-point",
              isNeighbor ? "is-neighbor" : "",
              isSelected ? "is-selected" : "",
            ].filter(Boolean).join(" ");
            const radius = isSelected ? 7 : isNeighbor ? 5 : 3;
            return `
              <circle
                class="${classNames}"
                data-action="focus-feature-space-burst"
                data-burst-id="${escapeHtml(burstId)}"
                cx="${scaleX(item.x)}"
                cy="${scaleY(item.y)}"
                r="${radius}"
              ><title>${escapeHtml(burstId)}</title></circle>
            `;
          }).join("")}
        </svg>
      </div>
    `;
  }

  renderBurstFeatureSpace() {
    if (!this.refs?.burstFeatureSpace) {
      return;
    }
    const result = this.burstFeatureSpace || this.makeEmptyBurstFeatureSpace();
    if (result.status === "idle") {
      this.refs.burstFeatureSpace.innerHTML = '<div class="movement-table-empty">Run feature space to project automatic bursts.</div>';
      return;
    }
    if (result.status === "loading") {
      this.refs.burstFeatureSpace.innerHTML = '<div class="movement-table-empty">Building burst feature space...</div>';
      return;
    }
    const points = Array.isArray(result.points) ? result.points : [];
    const explainedVariance = Array.isArray(result.pca?.explained_variance_ratio)
      ? result.pca.explained_variance_ratio
      : [];
    const fittedFeatures = Array.isArray(result.featureMatrix?.fitted_features)
      ? result.featureMatrix.fitted_features
      : [];
    const metadata = [
      result.createdAt ? `${result.loadedFromHistory ? "Restored" : "Created"}: ${formatDateTime(result.createdAt)}${result.user ? ` by ${result.user}` : ""}` : "",
      result.featureSet ? `Feature set: ${String(result.featureSet).replaceAll("_", " ")}` : "",
      Number.isFinite(Number(explainedVariance[0])) ? `PC1: ${(Number(explainedVariance[0]) * 100).toFixed(1)}%` : "",
      Number.isFinite(Number(explainedVariance[1])) ? `PC2: ${(Number(explainedVariance[1]) * 100).toFixed(1)}%` : "",
      `Features: ${formatCount(fittedFeatures.length)}`,
      `Bursts: ${formatCount(points.length)}`,
    ].filter(Boolean);
    const warningsHtml = result.warnings.length
      ? `<div class="movement-anomaly-warnings">${result.warnings.map(warning => `<div class="movement-anomaly-warning">${escapeHtml(String(warning))}</div>`).join("")}</div>`
      : "";
    const selectedPoint = this.getBurstFeatureSpacePoint(result.selectedBurstId);
    this.refs.burstFeatureSpace.innerHTML = `
      <div class="movement-feature-space-meta">${metadata.map(item => `<span>${escapeHtml(item)}</span>`).join("")}</div>
      ${warningsHtml}
      ${points.length ? this.renderBurstFeatureSpacePlot(points, selectedPoint) : '<div class="movement-table-empty">No burst feature-space points were produced.</div>'}
      ${this.renderBurstFeatureSpaceSelection(selectedPoint)}
    `;
  }

  async inspectBurstFeatureSpacePoint(burstId) {
    const point = this.selectBurstFeatureSpacePoint(burstId);
    if (!point) {
      this.setStatus("Could not find that burst in the current feature-space result.", true);
      return;
    }
    await this.inspectBurstRef(point, {
      checkFixes: false,
      isolateIndividual: true,
    });
  }

  async inspectBurstFeatureSpaceNeighbor(burstId) {
    const point = this.getBurstFeatureSpacePoint(burstId);
    if (!point) {
      this.setStatus("Could not find that neighbor burst in the current feature-space result.", true);
      return;
    }
    await this.inspectBurstRef(point, {
      checkFixes: false,
      isolateIndividual: true,
      preserveFeatureSpaceSelection: true,
    });
  }

  async handleBurstFeatureSpaceClick(event) {
    const target = event.target.closest("[data-action]");
    if (!target) {
      return;
    }
    const action = target.dataset.action || "";
    if (action === "focus-feature-space-burst") {
      await this.inspectBurstFeatureSpacePoint(target.dataset.burstId || "");
    } else if (action === "focus-feature-space-neighbor") {
      await this.inspectBurstFeatureSpaceNeighbor(target.dataset.burstId || "");
    }
  }

  normalizeRankingBurstRefs(row) {
    const refs = Array.isArray(row?.ranked_burst_refs) ? row.ranked_burst_refs : [];
    return refs
      .map(ref => ({
        ...ref,
        individual: String(ref?.individual || row?.individual || ""),
        burst_id: String(ref?.burst_id || ""),
        fix_keys: Array.isArray(ref?.fix_keys) ? ref.fix_keys.map(value => String(value || "")).filter(Boolean) : [],
      }))
      .filter(ref => ref.burst_id);
  }

  normalizeAnomalyExplanationItems(items) {
    return (Array.isArray(items) ? items : [])
      .map(item => ({
        feature: String(item?.feature || ""),
        displayValue: String(item?.display_value ?? item?.displayValue ?? ""),
        percentile: finiteOrNull(item?.percentile),
        direction: String(item?.direction || ""),
      }))
      .filter(item => item.feature);
  }

  anomalyFeatureLabel(feature) {
    const key = String(feature || "");
    const labels = {
      duration_s: "duration",
      path_length_m: "path length",
      mean_step_length_m: "mean step length",
      sd_step_length_m: "step length variability",
      net_displacement_m: "net displacement",
      straightness: "straightness",
      mean_speed_mps: "mean speed",
      median_speed_mps: "median speed",
      max_speed_mps: "max speed",
      sd_speed_mps: "speed variability",
      max_time_gap_s: "max time gap",
    };
    if (labels[key]) {
      return labels[key];
    }
    let cleaned = key;
    if (cleaned.startsWith("osm:")) {
      cleaned = cleaned.replace(/^osm:nearest_/, "").replace(/_distance_m/g, " distance");
    }
    cleaned = cleaned
      .replace(/__(mean|median|min|max|sd)$/g, " $1")
      .replace(/_mps/g, "")
      .replace(/_m/g, "")
      .replace(/_s/g, "")
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return cleaned || key;
  }

  explanationExtremeness(item) {
    const percentile = finiteOrNull(item?.percentile);
    if (percentile === null) {
      return -1;
    }
    if (item.direction === "high") {
      return percentile;
    }
    if (item.direction === "low") {
      return 100 - percentile;
    }
    return -1;
  }

  isCompactWhyItem(item) {
    const percentile = finiteOrNull(item?.percentile);
    if (percentile === null) {
      return false;
    }
    if (item.direction === "high") {
      return percentile >= 80;
    }
    if (item.direction === "low") {
      return percentile <= 20;
    }
    return false;
  }

  whyDirectionLabel(item) {
    const percentile = finiteOrNull(item?.percentile);
    if (item.direction === "high") {
      return percentile >= 95 ? "very high" : "high";
    }
    if (item.direction === "low") {
      return percentile <= 5 ? "very low" : "low";
    }
    return "";
  }

  hasAnomalyExplanationData(ref) {
    return Boolean(
      this.normalizeAnomalyExplanationItems(ref.top_high_quantile_features).length
      || this.normalizeAnomalyExplanationItems(ref.top_low_quantile_features).length
      || this.normalizeAnomalyExplanationItems(ref.missing_features).length
    );
  }

  getCompactAnomalyWhyItems(ref) {
    const high = this.normalizeAnomalyExplanationItems(ref.top_high_quantile_features)
      .filter(item => item.direction === "high");
    const low = this.normalizeAnomalyExplanationItems(ref.top_low_quantile_features)
      .filter(item => item.direction === "low");
    return [...high, ...low]
      .filter(item => this.isCompactWhyItem(item))
      .sort((left, right) => (
        this.explanationExtremeness(right) - this.explanationExtremeness(left)
        || left.feature.localeCompare(right.feature)
      ))
      .slice(0, 3);
  }

  renderAnomalyWhy(ref) {
    const items = this.getCompactAnomalyWhyItems(ref);
    if (!items.length) {
      if (this.hasAnomalyExplanationData(ref)) {
        return `<div class="movement-anomaly-why"><strong>Why:</strong> mixed feature pattern</div>`;
      }
      return "";
    }
    const parts = items.map(item => {
      const directionLabel = this.whyDirectionLabel(item);
      return `${directionLabel} ${this.anomalyFeatureLabel(item.feature)}`;
    });
    return `<div class="movement-anomaly-why"><strong>Why:</strong> ${escapeHtml(parts.join("; "))}</div>`;
  }

  formatAnomalyExplanationPercentile(percentile) {
    const value = finiteOrNull(percentile);
    if (value === null) {
      return "";
    }
    const formatted = Math.abs(value - Math.round(value)) < 0.05
      ? String(Math.round(value))
      : value.toFixed(1);
    return `${formatted}th percentile`;
  }

  renderAnomalyExplanationItems(label, items, direction) {
    const normalized = this.normalizeAnomalyExplanationItems(items)
      .filter(item => item.direction === direction)
      .slice(0, direction === "missing" ? 5 : 3);
    if (!normalized.length) {
      return "";
    }
    const totalCount = this.normalizeAnomalyExplanationItems(items)
      .filter(item => item.direction === direction).length;
    const moreCount = Math.max(0, totalCount - normalized.length);
    return `
      <div class="movement-anomaly-explanation-section">
        <span class="movement-anomaly-explanation-label">${escapeHtml(label)}</span>
        ${normalized.map(item => {
          const valueLabel = item.displayValue || (item.direction === "missing" ? "NA" : "n/a");
          const percentileLabel = this.formatAnomalyExplanationPercentile(item.percentile);
          return `
            <span class="movement-anomaly-explanation-chip" data-explanation-direction="${escapeHtml(item.direction)}">
              <span>${escapeHtml(item.feature)}</span>
              <span>=</span>
              <span>${escapeHtml(valueLabel)}</span>
              ${percentileLabel ? `<span>(${escapeHtml(percentileLabel)})</span>` : ""}
            </span>
          `;
        }).join("")}
        ${moreCount ? `<span class="movement-subtle">+${escapeHtml(formatCount(moreCount))} more</span>` : ""}
      </div>
    `;
  }

  renderAnomalyBurstExplanation(ref) {
    const highHtml = this.renderAnomalyExplanationItems(
      "High observed quantiles",
      ref.top_high_quantile_features,
      "high",
    );
    const lowHtml = this.renderAnomalyExplanationItems(
      "Low observed quantiles",
      ref.top_low_quantile_features,
      "low",
    );
    const missingHtml = this.renderAnomalyExplanationItems(
      "Missing fitted features",
      ref.missing_features,
      "missing",
    );
    if (!highHtml && !lowHtml && !missingHtml) {
      return "";
    }
    return `
      <details class="movement-anomaly-explanation-details" data-role="ranking-burst-explanation">
        <summary>Details</summary>
        <div class="movement-anomaly-explanation">
          <div class="movement-anomaly-explanation-note">Observed feature-value quantiles, not SHAP/model attribution.</div>
          ${highHtml}
          ${lowHtml}
          ${missingHtml}
        </div>
      </details>
    `;
  }

  renderAnomalyBurstRefs(row) {
    const refs = this.normalizeRankingBurstRefs(row);
    if (!refs.length) {
      return "";
    }
    return `
      <div class="movement-anomaly-bursts" data-role="ranking-burst-refs">
        ${refs.map((ref, refIndex) => {
          const isTopRankingBurst = refIndex === 0;
          const fixCount = finiteOrNull(ref.n_fixes ?? ref.fix_count) ?? ref.fix_keys.length;
          const meta = [
            ref.set_name ? `track ${ref.set_name}` : "",
            Number.isFinite(Number(fixCount)) ? `${formatCount(fixCount)} fixes` : "",
            finiteOrNull(ref.outlier_margin) !== null
              ? `decision margin ${formatMaybeNumber(finiteOrNull(ref.outlier_margin), "")}`
              : "",
            Number.isFinite(Number(ref.is_outlier_count))
              ? `${formatCount(ref.is_outlier_count)} source outliers`
              : "",
          ].filter(Boolean).join(" • ");
          return `
            <div class="movement-anomaly-burst${isTopRankingBurst ? " is-ranking-burst" : ""}" data-ranking-burst-id="${escapeHtml(ref.burst_id)}">
              <div class="movement-anomaly-burst-main">
                ${isTopRankingBurst ? `<span class="movement-anomaly-burst-rank-badge" aria-label="ranking burst">★</span>` : ""}
                <span class="movement-table-cell-mono">${escapeHtml(ref.burst_id)}</span>
                <span>score ${escapeHtml(formatMaybeNumber(finiteOrNull(ref.anomaly_score), ""))}</span>
                ${meta ? `<span class="movement-subtle">${escapeHtml(meta)}</span>` : ""}
              </div>
              ${this.renderAnomalyWhy(ref)}
              ${this.renderAnomalyBurstExplanation(ref)}
              <div class="movement-anomaly-burst-actions">
                <button type="button" data-action="zoom-ranking-burst" data-burst-id="${escapeHtml(ref.burst_id)}">Zoom to burst</button>
                <button type="button" data-action="check-ranking-burst" data-burst-id="${escapeHtml(ref.burst_id)}">Check fixes</button>
                <button type="button" data-action="select-ranking-burst" data-burst-id="${escapeHtml(ref.burst_id)}"${this.canPersistEdits() ? "" : " disabled"}>Select for flagging</button>
              </div>
            </div>
          `;
        }).join("")}
      </div>
    `;
  }

  getRankingBurstRef(burstId) {
    const target = String(burstId || "");
    if (!target || !this.anomalyRanking) {
      return null;
    }
    for (const row of this.anomalyRanking.rankedIndividuals || []) {
      const ref = this.normalizeRankingBurstRefs(row).find(item => item.burst_id === target);
      if (ref) {
        return ref;
      }
    }
    return null;
  }

  getRankingBurstPath(ref) {
    if (!this.data || !ref) {
      return [];
    }
    const pathFromFixKeys = (ref.fix_keys || [])
      .map(fixKey => this.data.fixByKey.get(fixKey))
      .filter(Boolean)
      .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey))
      .map(fix => fix.position)
      .filter(position => Array.isArray(position) && position.length >= 2);
    if (pathFromFixKeys.length) {
      return pathFromFixKeys;
    }
    const burst = this.data.autoBurstById?.get(ref.burst_id || "");
    return burst?.path?.length ? burst.path : [];
  }

  setFocusedRankingBurst(ref) {
    if (!ref?.burst_id) {
      this.focusedRankingBurst = null;
      return;
    }
    this.focusedRankingBurst = {
      burstId: String(ref.burst_id || ""),
      individual: String(ref.individual || ""),
      setName: String(ref.set_name || ref.setName || ""),
      fixKeys: Array.isArray(ref.fix_keys) ? ref.fix_keys.map(value => String(value || "")).filter(Boolean) : [],
    };
    if (this.getBurstFeatureSpacePoint(this.focusedRankingBurst.burstId)) {
      this.burstFeatureSpace.selectedBurstId = this.focusedRankingBurst.burstId;
      this.renderBurstFeatureSpace();
    }
  }

  clearFocusedRankingBurstIfHidden() {
    if (
      this.focusedRankingBurst?.individual
      && this.data?.selectedIndividuals instanceof Set
      && !this.data.selectedIndividuals.has(this.focusedRankingBurst.individual)
    ) {
      this.focusedRankingBurst = null;
    }
  }

  getFocusedRankingBurstFixes() {
    this.clearFocusedRankingBurstIfHidden();
    if (!this.data || !this.focusedRankingBurst?.fixKeys?.length) {
      return [];
    }
    const focusedFixes = this.focusedRankingBurst.fixKeys
      .map(fixKey => this.data.fixByKey.get(fixKey))
      .filter(Boolean)
      .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
    if (!focusedFixes.length) {
      return [];
    }
    const visibleSetNames = this.getVisibleSetNames();
    return focusedFixes.filter(fix => (
      this.data.selectedIndividuals.has(fix.individual)
      && visibleSetNames.has(fix.setName)
    ));
  }

  mutedRankingContextColor(color, alpha = 42) {
    const source = Array.isArray(color) ? color : [124, 136, 153, 255];
    const red = Number(source[0]) || 0;
    const green = Number(source[1]) || 0;
    const blue = Number(source[2]) || 0;
    const gray = Math.round((red * 0.2126) + (green * 0.7152) + (blue * 0.0722));
    const mutedChannel = channel => Math.round((gray * 0.82) + (channel * 0.18));
    return [mutedChannel(red), mutedChannel(green), mutedChannel(blue), alpha];
  }

  isFocusedBurstItem(item, focusedBurstId) {
    return Boolean(focusedBurstId)
      && String(item?.burst?.burstId || "") === focusedBurstId;
  }

  burstFillWidth(item) {
    return item?.sourceFlagged ? 2 : 5;
  }

  burstCasingWidth(item, focusedBurstId) {
    return this.burstFillWidth(item)
      + (this.isFocusedBurstItem(item, focusedBurstId) ? 7 : 4);
  }

  burstFillColor(item, focusedBurstId) {
    const individual = item?.burst?.individual;
    if (this.isFocusedBurstItem(item, focusedBurstId)) {
      return this.queueMapColor(item.color, individual);
    }
    if (focusedBurstId) {
      // Source-flagged context stays dimmer than clean context so the
      // distinction survives while another burst is focused.
      return this.queueMapColor(
        this.mutedRankingContextColor(item.color, item?.sourceFlagged ? 22 : 36),
        individual,
      );
    }
    if (item?.sourceFlagged) {
      return this.queueMapColor(this.mutedRankingContextColor(item.color, 58), individual);
    }
    return this.queueMapColor(item.color, individual);
  }

  burstCasingColor(item, focusedBurstId) {
    const individual = item?.burst?.individual;
    if (this.isFocusedBurstItem(item, focusedBurstId)) {
      return this.queueMapColor(BURST_FOCUS_CASING_COLOR, individual);
    }
    const alpha = focusedBurstId
      ? (item?.sourceFlagged ? 40 : 70)
      : (item?.sourceFlagged ? 120 : 205);
    return this.queueMapColor([...BURST_CASING_RGB, alpha], individual);
  }

  queueActiveIndividual() {
    if (this.individualReviewQueue.mode !== "queue") {
      return "";
    }
    if (!this.individualReviewQueue.activeIndividual && this.data) {
      // The map can render before the queue list has re-resolved the active
      // individual, and an empty value would draw the whole batch at full
      // opacity. Resolve it the same way the list does instead of trusting
      // the raw field. This repairs the field, so later calls are cheap.
      this.getIndividualQueuePosition();
    }
    return String(this.individualReviewQueue.activeIndividual || "");
  }

  queueMapOpacity(individual) {
    const activeIndividual = this.queueActiveIndividual();
    if (activeIndividual && individual && individual !== activeIndividual) {
      return 0.25;
    }
    return 1;
  }

  queueMapColor(color, individual) {
    const source = Array.isArray(color) ? color : [124, 136, 153, 255];
    const opacity = this.queueMapOpacity(individual);
    if (opacity === 1) {
      return source;
    }
    const sourceAlpha = Number(source[3]);
    return [
      Number(source[0]) || 0,
      Number(source[1]) || 0,
      Number(source[2]) || 0,
      Math.round((Number.isFinite(sourceAlpha) ? sourceAlpha : 255) * opacity),
    ];
  }

  async inspectRankingBurst(burstId, { checkFixes = false } = {}) {
    if (!this.data) {
      return;
    }
    const ref = this.getRankingBurstRef(burstId);
    if (!ref) {
      this.setStatus("Could not find that ranked burst in the current ranking result.", true);
      return;
    }
    await this.inspectBurstRef(ref, {
      checkFixes,
      isolateIndividual: true,
    });
  }

  async inspectBurstRef(
    ref,
    {
      checkFixes = false,
      isolateIndividual = false,
      preserveFeatureSpaceSelection = false,
      updateTime = true,
      focus = true,
      zoom = true,
    } = {},
  ) {
    if (!this.data || !ref?.burst_id) {
      return;
    }
    const fixKeys = Array.isArray(ref.fix_keys) ? ref.fix_keys : [];
    const preservedFixKeys = new Set(this.data.selectedFixKeys);
    if (checkFixes) {
      for (const fixKey of fixKeys) {
        preservedFixKeys.add(fixKey);
      }
    }
    if (ref.individual && isolateIndividual) {
      this.data.selectedIndividuals = new Set([ref.individual]);
    } else if (ref.individual) {
      this.data.selectedIndividuals.add(ref.individual);
    }
    const startTimeMs = finiteOrNull(ref.start_time_ms);
    if (updateTime && startTimeMs !== null) {
      this.currentTimeMs = startTimeMs;
      this.refs.slider.value = String(startTimeMs);
      this.updateTimeLabel();
    }
    this.saveUiState();
    this.renderIndividuals();
    await this.loadDetailForCurrentSelection({ preservedFixKeys });

    const preservedFeatureSpaceBurstId = preserveFeatureSpaceSelection
      ? String(this.burstFeatureSpace?.selectedBurstId || "")
      : "";
    if (focus) {
      this.setFocusedRankingBurst(ref);
    }
    if (preserveFeatureSpaceSelection && preservedFeatureSpaceBurstId) {
      this.burstFeatureSpace.selectedBurstId = preservedFeatureSpaceBurstId;
      this.renderBurstFeatureSpace();
    }
    if (checkFixes) {
      const nextSelected = new Set(this.data.selectedFixKeys);
      for (const fixKey of fixKeys) {
        if (this.data.fixByKey.has(fixKey)) {
          nextSelected.add(fixKey);
        }
      }
      this.data.selectedFixKeys = nextSelected;
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.updateActionButtons();
    }
    this.renderLayers();

    const path = this.getRankingBurstPath(ref);
    if (zoom && path.length) {
      this.zoomToPath(path);
      const action = checkFixes ? "checked and zoomed to" : "zoomed to";
      this.setStatus(`Selected ${ref.individual || "individual"} and ${action} burst ${ref.burst_id}.`);
    } else if (zoom) {
      this.setStatus(`Selected ${ref.individual || "individual"}, but burst ${ref.burst_id} fixes are not loaded for zooming.`, true);
    }
  }

  async handleAnomalyRankingClick(event) {
    const actionButton = event.target.closest("button[data-action]");
    if (!actionButton) {
      return;
    }
    const action = actionButton.dataset.action || "";
    if (action === "load-saved-ranking") {
      await this.loadSavedAnomalyRanking();
    } else if (action === "check-saved-ranking") {
      await this.restoreSavedAnalyses();
    } else if (action === "zoom-ranking-burst") {
      await this.inspectRankingBurst(actionButton.dataset.burstId || "", { checkFixes: false });
    } else if (action === "check-ranking-burst") {
      await this.inspectRankingBurst(actionButton.dataset.burstId || "", { checkFixes: true });
    } else if (action === "select-ranking-burst") {
      const ref = this.getRankingBurstRef(actionButton.dataset.burstId || "");
      if (ref) {
        if (!this.data?.autoBurstById?.has(ref.burst_id)) {
          await this.inspectBurstRef(ref, {
            checkFixes: false,
            updateTime: false,
            focus: false,
            zoom: false,
          });
        }
        this.setBurstFlagTargetIncluded(ref.burst_id, true, {
          origin: "algorithm",
          sourceAnalysisId: this.anomalyRanking?.analysisId || "",
          selectionMethod: "ranking_burst_result",
          replace: true,
        });
      }
    }
  }

  renderAnomalyRanking() {
    if (!this.refs?.anomalyRanking) {
      return;
    }
    const result = this.anomalyRanking || this.makeEmptyAnomalyRanking();
    if (result.status === "checking") {
      this.refs.anomalyRanking.innerHTML = '<div class="movement-table-empty">Checking for a compatible saved burst ranking…</div>';
      return;
    }
    if (result.status === "available") {
      const created = result.createdAt
        ? ` from ${formatDateTime(result.createdAt)}`
        : "";
      this.refs.anomalyRanking.innerHTML = (
        `<div class="movement-table-empty">A compatible saved burst ranking${escapeHtml(created)} is available. `
        + '<button type="button" data-action="load-saved-ranking">Load saved ranking</button></div>'
      );
      return;
    }
    if (result.status === "restoring") {
      this.refs.anomalyRanking.innerHTML = '<div class="movement-table-empty">Loading saved burst ranking…</div>';
      return;
    }
    if (result.status === "restore_error") {
      this.refs.anomalyRanking.innerHTML = (
        `<div class="movement-table-empty">The saved burst ranking could not be loaded: ${escapeHtml(result.restoreError || "unknown error")} `
        + '<button type="button" data-action="load-saved-ranking">Retry loading</button></div>'
      );
      return;
    }
    if (result.status === "history_error") {
      this.refs.anomalyRanking.innerHTML = (
        `<div class="movement-table-empty">Could not check saved burst rankings: ${escapeHtml(result.restoreError || "unknown error")} `
        + '<button type="button" data-action="check-saved-ranking">Try again</button></div>'
      );
      return;
    }
    if (result.status === "idle" || result.status === "unavailable") {
      this.refs.anomalyRanking.innerHTML = '<div class="movement-table-empty">Run burst ranking to prioritize individual review.</div>';
      return;
    }
    if (result.status === "loading") {
      this.refs.anomalyRanking.innerHTML = '<div class="movement-table-empty">Running burst anomaly ranking analysis...</div>';
      return;
    }
    const burstGapLabel = result.burstGap
      ? formatBurstGapMetadata(parseMovementBurstGap({ burst_gap: result.burstGap }))
      : "";
    const modelFit = result.modelFit || {};
    const rankingSummary = result.rankingSummary || {};
    const rankingMethod = String(result.rankingMethod || modelFit.ranking_method || this.getRankingMethod());
    const individualScoreLabel = rankingMethod === "source_is_outlier"
      ? "Total source outliers"
      : rankingMethod === "isolation_forest_decision_margin"
        ? "Total decision margin"
        : "Worst burst score";
    const metadata = [
      result.analysisId ? `Analysis: ${result.analysisId}` : "",
      result.createdAt ? `${result.loadedFromHistory ? "Restored" : "Created"}: ${formatDateTime(result.createdAt)}${result.user ? ` by ${result.user}` : ""}` : "",
      burstGapLabel ? `Burst gap: ${burstGapLabel}` : "",
      modelFit.model ? `Model: ${modelFit.model}` : "",
      rankingMethod ? `Ranking: ${this.rankingMethodLabel(rankingMethod)}` : "",
      rankingSummary.ranking_method
        ? `Individual aggregation: ${String(rankingSummary.ranking_method).replaceAll("_", " ")}`
        : "",
      modelFit.feature_set ? `Feature set: ${String(modelFit.feature_set).replaceAll("_", " ")}` : "",
      Number.isFinite(Number(modelFit.scored_burst_count))
        ? `Scored bursts: ${formatCount(modelFit.scored_burst_count)}`
        : "",
      Array.isArray(modelFit.fitted_features)
        ? `Fitted features: ${formatCount(modelFit.fitted_features.length)}`
        : "",
      modelFit.excluded_by_feature_set && typeof modelFit.excluded_by_feature_set === "object"
        ? `Feature-set exclusions: ${formatCount(Object.keys(modelFit.excluded_by_feature_set).length)}`
        : "",
    ].filter(Boolean);
    const metadataHtml = metadata.length
      ? `<div class="movement-anomaly-meta">${metadata.map(item => `<div>${escapeHtml(item)}</div>`).join("")}</div>`
      : "";
    const fittedFeatureNames = Array.isArray(modelFit.fitted_features)
      ? modelFit.fitted_features.slice(0, 12).map(item => String(item))
      : [];
    const fittedFeaturesHtml = fittedFeatureNames.length
      ? `<div class="movement-anomaly-meta"><div>Fitted feature sample: ${escapeHtml(fittedFeatureNames.join(", "))}${modelFit.fitted_features.length > fittedFeatureNames.length ? "..." : ""}</div></div>`
      : "";
    const warningsHtml = result.warnings.length
      ? `<div class="movement-anomaly-warnings">${result.warnings.map(warning => `<div class="movement-anomaly-warning">${escapeHtml(String(warning))}</div>`).join("")}</div>`
      : "";
    const rows = Array.isArray(result.rankedIndividuals) ? result.rankedIndividuals : [];
    if (!rows.length) {
      const message = result.status === "error"
        ? "The ranking analysis failed."
        : "No individuals were ranked.";
      this.refs.anomalyRanking.innerHTML = `${metadataHtml}${fittedFeaturesHtml}${warningsHtml}<div class="movement-table-empty">${escapeHtml(message)}</div>`;
      return;
    }
    this.refs.anomalyRanking.innerHTML = `
      ${metadataHtml}
      ${fittedFeaturesHtml}
      ${warningsHtml}
      <table class="movement-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Individual</th>
            <th>${escapeHtml(individualScoreLabel)}</th>
            <th>Top burst score</th>
            <th>Top burst</th>
            <th>Bursts</th>
            <th>Scored</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(row => {
            const burstRefsHtml = this.renderAnomalyBurstRefs(row);
            return `
              <tr>
                <td class="movement-table-cell-mono">${escapeHtml(String(row.rank ?? ""))}</td>
                <td>${escapeHtml(String(row.individual || ""))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatMaybeNumber(finiteOrNull(row.individual_score ?? row.top_burst_score), ""))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatMaybeNumber(finiteOrNull(row.top_burst_score), ""))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(String(row.top_burst_id || ""))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatCount(row.burst_count))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatCount(row.scored_burst_count))}</td>
              </tr>
              ${burstRefsHtml ? `<tr class="movement-anomaly-burst-row"><td colspan="7">${burstRefsHtml}</td></tr>` : ""}
            `;
          }).join("")}
        </tbody>
      </table>
    `;
  }

  async runSelectedCandidateQuery() {
    if (!this.data || !this.currentFamily || !this.currentStudy || !this.currentDatasetId || !this.currentArtifact) {
      return;
    }
    const selectedQuery = this.getSelectedCandidateQuery();
    if (!selectedQuery) {
      this.setStatus("No saved movement candidate query is selected.", true);
      return;
    }
    const executionScope = this.getCandidateQueryExecutionScope();
    if (!executionScope) {
      this.setStatus("Select exactly one individual before running the current-individual candidate query scope.", true);
      return;
    }
    const controller = this.beginRequest("candidateQuery");
    this.candidateQueryPreview = {
      ...this.makeEmptyCandidateQueryPreview(),
      status: "loading",
    };
    this.setStatus(`Running filter ${selectedQuery.name || selectedQuery.query_id} and flagging its matches...`);
    this.renderLayers();
    this.updateActionButtons();

    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/run-candidate-query`,
        {
          method: "POST",
          signal: controller.signal,
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            user: this.getUser() || "reviewer",
            preview_limit: 1000,
            query_id: selectedQuery.query_id,
            query_version: selectedQuery.version,
            query_parameters: this.getCandidateQueryParameterValues(selectedQuery),
            execution_scope: executionScope,
            expected_current_dataset_id: this.expectedCurrentDatasetId(),
            expected_review_revision: this.expectedReviewRevision(),
          }),
        },
      );
      if (this.requestControllers.candidateQuery !== controller) {
        return;
      }
      const summary = result?.summary || {};
      const candidates = Array.isArray(summary.candidates) ? summary.candidates : [];
      const matchKeys = new Set();
      const evidenceByFixKey = new Map();
      for (const candidate of candidates) {
        const fixKey = String(candidate?.fix_key || "");
        if (!fixKey) {
          continue;
        }
        matchKeys.add(fixKey);
        evidenceByFixKey.set(fixKey, candidate.evidence || {});
      }
      this.candidateQueryPreview = {
        analysisId: String(result?.analysis_id || result?.analysis?.analysis_id || ""),
        matchKeys,
        candidates,
        evidenceByFixKey,
        status: String(summary.run_status || "success"),
        warnings: Array.isArray(summary.warnings) ? summary.warnings : [],
        candidateCount: Number(summary.candidate_count) || matchKeys.size,
        returnedCount: Number(summary.returned_count) || candidates.length,
      };
      const createdDatasetId = String(result?.dataset?.dataset_id || "");
      if (createdDatasetId) {
        const flaggedCount = this.candidateQueryPreview.candidateCount;
        const analysisId = this.candidateQueryPreview.analysisId;
        await this.loadStudyAtDataset(createdDatasetId, {
          preserveAnnotationContext: true,
          result,
          clearTarget: "filter",
        });
        this.setStatus(
          `Filter run ${analysisId || "completed"} flagged ${formatCount(flaggedCount)} fixes as suspicious in a new review step.`,
        );
        return;
      }
      const visibleCount = this.getCandidateQueryMatchKeys().size;
      const returnedCount = this.getCandidateQueryReturnedMatchKeys().size;
      const warningText = this.candidateQueryPreview.warnings.length
        ? ` ${this.candidateQueryPreview.warnings[0]}`
        : "";
      if (this.candidateQueryPreview.status === "unresolved") {
        this.setStatus(`Filter run unresolved; no review step was created. ${warningText}`.trim(), true);
      } else {
        const visibilityText = visibleCount === returnedCount
          ? `${formatCount(visibleCount)} visible`
          : `${formatCount(visibleCount)} visible now; ${formatCount(returnedCount)} returned and available to check`;
        this.setStatus(`Filter found no flaggable matches; no review step was created. ${visibilityText}.${warningText}`.trim());
      }
      this.renderLayers();
      this.updateActionButtons();
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      if (this.requestControllers.candidateQuery === controller) {
        this.candidateQueryPreview = {
          ...this.makeEmptyCandidateQueryPreview(),
          status: "error",
          warnings: [error.message],
        };
        this.setStatus(`Candidate query failed: ${error.message}`, true);
        this.renderLayers();
        this.updateActionButtons();
      }
    } finally {
      if (this.requestControllers.candidateQuery === controller) {
        this.requestControllers.candidateQuery = null;
      }
    }
  }

  async checkCandidateQueryPreview() {
    if (!this.data) {
      return;
    }
    const matchKeys = this.getCandidateQueryReturnedMatchKeys();
    if (!matchKeys.size) {
      return;
    }
    const candidateFixes = parseMovementFixes(this.candidateQueryPreview.candidates || []);
    this.data.candidateFixes = candidateFixes;
    refreshMovementFixCollections(this.data);
    for (const fix of candidateFixes) {
      if (fix.individual) {
        this.data.selectedIndividuals.add(fix.individual);
      }
    }
    const nextSelected = new Set(this.data.selectedFixKeys);
    for (const fixKey of matchKeys) {
      if (this.data.fixByKey.has(fixKey)) {
        nextSelected.add(fixKey);
      }
    }
    this.data.selectedFixKeys = nextSelected;
    this.resetManualFlagTarget({ resetKind: false });
    this.flagTargetKind = "fixes";
    this.saveUiState();
    this.setSideSheet("individuals");
    this.renderIndividuals();
    this.renderSelectedFixes();
    this.renderThresholdPane();
    this.renderLayers();
    this.updateActionButtons();
    await this.loadDetailForCurrentSelection({ preservedFixKeys: nextSelected });
  }

  handleVisibilityChange() {
    this.clearThresholdState();
    this.saveUiState();
    this.renderIndividuals();
    this.renderThresholdPane();
    this.renderLayers();
    this.updateActionButtons();
  }

  setStatus(message, isError = false) {
    this.refs.status.textContent = message;
    this.refs.status.classList.toggle("error", isError);
  }

  canPersistEdits() {
    return this.editLockProfile?.editable === true;
  }

  expectedCurrentDatasetId() {
    return String(
      this.editLockProfile?.current_dataset_id
      || this.graph?.current_dataset_id
      || "",
    );
  }

  expectedReviewRevision() {
    return Number(this.editLockProfile?.review_revision || 0);
  }

  reviewActionUrl(action) {
    return `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/${action}`;
  }

  reviewMutationBody(extra = {}) {
    return {
      expected_current_dataset_id: this.expectedCurrentDatasetId(),
      expected_review_revision: this.expectedReviewRevision(),
      ...extra,
    };
  }

  renderReviewControls() {
    const profile = this.editLockProfile || {};
    const actor = profile.actor || window.vibecleaningActor || {};
    const capabilities = profile.capabilities || {};
    const review = profile.review || null;
    const coverage = profile.coverage || null;
    const control = profile.editor_control || null;
    if (this.refs.authIdentity) {
      this.refs.authIdentity.textContent = actor.display_name
        ? `${actor.display_name} · ${actor.role || "user"}`
        : "";
    }
    if (this.refs.reviewProgress) {
      if (!review) {
        this.refs.reviewProgress.textContent = this.currentStudy ? "No active review" : "";
      } else if (coverage) {
        const needsCheck = Number(coverage.needs_check_count || 0);
        const priorNeedsCheck = Number(coverage.prior_needs_check_count || 0);
        this.refs.reviewProgress.textContent = (
          `${coverage.reviewed_count || 0}/${coverage.required_count || 0} reviewed`
          + (needsCheck ? ` · ${needsCheck} needs check` : "")
          + (priorNeedsCheck ? ` · ${priorNeedsCheck} carried forward` : "")
        );
      }
    }
    if (this.refs.assignReview) {
      this.refs.assignReview.hidden = !(
        this.currentStudy && capabilities.can_manage_assignment && !review
      );
    }
    if (this.refs.adminDashboard) {
      this.refs.adminDashboard.hidden = actor.role !== "editor";
    }
    if (this.refs.completeReview) {
      this.refs.completeReview.hidden = !(review && (
        capabilities.can_complete
        || actor.role === "editor"
        || review.reviewer_user_id === actor.user_id
      ));
      this.refs.completeReview.disabled = capabilities.can_complete !== true;
      this.refs.completeReview.title = capabilities.can_complete
        ? "Complete this study-level review"
        : `${coverage?.remaining_count || 0} individual(s) still require a decision`;
    }
    if (this.refs.cancelReview) {
      this.refs.cancelReview.hidden = !(actor.role === "editor" && review);
    }
    if (this.refs.editorControlStart) {
      this.refs.editorControlStart.hidden = !(actor.role === "editor" && review && control?.owner_user_id !== actor.user_id);
      this.refs.editorControlStart.textContent = control ? "Take over editor control" : "Start editor control";
    }
    if (this.refs.editorControlFinish) {
      this.refs.editorControlFinish.hidden = !(actor.role === "editor" && control?.owner_user_id === actor.user_id);
    }
  }

  async openAdminDashboard() {
    if (!this.refs.adminDashboardModal) return;
    this.refs.adminDashboardModal.classList.remove("hidden");
    this.refs.adminDashboardStatus.textContent = "Loading study review summaries...";
    this.refs.adminDashboardStatus.classList.remove("error");
    this.refs.adminDashboardContent.innerHTML = "";
    try {
      const payload = await this.fetchJSON(
        "/api/apps/movement/admin/review-summary",
        { cache: "no-store" },
      );
      this.renderAdminDashboard(payload);
      this.refs.adminDashboardStatus.textContent = "";
    } catch (error) {
      this.refs.adminDashboardStatus.textContent = `Could not load review dashboard: ${error.message}`;
      this.refs.adminDashboardStatus.classList.add("error");
    }
  }

  renderAdminDashboard(payload) {
    const studies = Array.isArray(payload?.studies) ? payload.studies : [];
    if (!studies.length) {
      this.refs.adminDashboardContent.innerHTML = '<div class="movement-empty">No movement studies are available.</div>';
      return;
    }
    this.refs.adminDashboardContent.innerHTML = `
      <table class="movement-admin-dashboard-table">
        <thead><tr>
          <th>Study</th><th>Review</th><th>Reviewer</th><th>Progress</th>
          <th>OK</th><th>Fix &amp; Keep</th><th>Remove</th><th>Needs check</th><th>Undecided</th><th>Actions</th>
        </tr></thead>
        <tbody>
          ${studies.map(item => {
            const counts = item.counts || {};
            const review = item.review || {};
            const reviewer = review.reviewer?.display_name || review.reviewer?.username || "—";
            return `
              <tr data-admin-study-row data-family="${escapeHtml(item.family || "")}" data-study="${escapeHtml(item.study || "")}">
                <td><strong>${escapeHtml(item.study || "")}</strong><br>${escapeHtml(item.family || "")}</td>
                <td>${escapeHtml(review.status || "Not reviewed")}</td>
                <td>${escapeHtml(reviewer)}</td>
                <td>${escapeHtml(formatCount(counts.reviewed || 0))}/${escapeHtml(formatCount(counts.required || 0))}</td>
                <td>${escapeHtml(formatCount(counts.ok || 0))}</td>
                <td>${escapeHtml(formatCount(counts.fix_keep || 0))}</td>
                <td>${escapeHtml(formatCount(counts.remove || 0))}</td>
                <td>${escapeHtml(formatCount(counts.needs_check || 0))}</td>
                <td>${escapeHtml(formatCount(counts.undecided || 0))}</td>
                <td><div class="movement-admin-dashboard-actions">
                  <button type="button" data-admin-action="expand">Individuals</button>
                  <button type="button" data-admin-action="open">Open study</button>
                </div></td>
              </tr>
              <tr data-admin-detail-row data-family="${escapeHtml(item.family || "")}" data-study="${escapeHtml(item.study || "")}" hidden>
                <td colspan="10"><div class="movement-admin-individuals">Loading...</div></td>
              </tr>
            `;
          }).join("")}
        </tbody>
      </table>
    `;
  }

  async handleAdminDashboardClick(event) {
    const button = event.target.closest("button[data-admin-action]");
    if (!button) return;
    const row = button.closest("tr[data-admin-study-row]");
    const family = row?.dataset.family || "";
    const study = row?.dataset.study || "";
    if (!family || !study) return;
    if (button.dataset.adminAction === "open") {
      if (!this.confirmDiscardIndividualReviewDrafts()) return;
      this.refs.adminDashboardModal.classList.add("hidden");
      if (family !== this.currentFamily) {
        await this.switchFamily(family);
      }
      this.currentStudy = study;
      this.refs.study.value = study;
      this.closeStudyEvents();
      this.currentDatasetId = "";
      this.currentArtifact = "";
      this.currentDataset = null;
      this.saveUiState();
      await this.loadStudy();
      return;
    }
    const detailRow = [...this.refs.adminDashboardContent.querySelectorAll("tr[data-admin-detail-row]")]
      .find(item => item.dataset.family === family && item.dataset.study === study);
    if (!detailRow) return;
    if (detailRow.dataset.loaded === "true") {
      detailRow.hidden = !detailRow.hidden;
      return;
    }
    detailRow.hidden = false;
    button.disabled = true;
    try {
      const query = new URLSearchParams({
        family,
        study,
        include_individuals: "true",
      });
      const payload = await this.fetchJSON(
        `/api/apps/movement/admin/review-summary?${query}`,
        { cache: "no-store" },
      );
      const individuals = payload?.studies?.[0]?.individuals || [];
      detailRow.querySelector(".movement-admin-individuals").innerHTML = individuals.length
        ? individuals.map(item => (
          `<div><strong>${escapeHtml(item.individual || "")}</strong><br>${escapeHtml(reviewDecisionLabel(item.review_decision))}${item.needs_check ? " • Needs check" : ""}</div>`
        )).join("")
        : '<div class="movement-empty">No individual decisions.</div>';
      detailRow.dataset.loaded = "true";
    } catch (error) {
      detailRow.querySelector(".movement-admin-individuals").textContent = `Could not load individuals: ${error.message}`;
    } finally {
      button.disabled = false;
    }
  }

  async assignCurrentReview() {
    if (!this.currentStudy) return;
    try {
      const payload = await this.fetchJSON("/api/apps/movement/reviewers", { cache: "no-store" });
      const reviewers = Array.isArray(payload.reviewers) ? payload.reviewers : [];
      if (!reviewers.length) {
        this.setStatus("No enabled reviewer accounts are available.", true);
        return;
      }
      const menu = reviewers.map(item => `${item.username} — ${item.display_name}`).join("\n");
      const username = window.prompt(`Assign this review to which username?\n\n${menu}`, reviewers[0].username);
      if (username === null) return;
      const reviewer = reviewers.find(item => item.username.toLowerCase() === username.trim().toLowerCase());
      if (!reviewer) {
        this.setStatus("That reviewer username is not available.", true);
        return;
      }
      await this.requestJSON(this.reviewActionUrl("review/assign"), {
        method: "POST",
        body: JSON.stringify(this.reviewMutationBody({
          reviewer_user_id: reviewer.user_id,
          logical_name: this.currentArtifact,
        })),
      });
      await this.loadStudy({ preferredDatasetId: this.expectedCurrentDatasetId(), viewContext: this.captureDatasetViewContext() });
      this.setStatus(`Assigned this review to ${reviewer.display_name}.`);
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(`Could not assign review: ${error.message}`, true);
    }
  }

  async completeCurrentReview() {
    const coverage = this.editLockProfile?.coverage || {};
    const needsCheck = Number(coverage.needs_check_count || 0);
    const actor = this.editLockProfile?.actor || window.vibecleaningActor || {};
    let reason = "";
    if (actor.role === "editor" && this.editLockProfile?.review?.reviewer_user_id !== actor.user_id) {
      reason = window.prompt("Audit reason for completing this review on the reviewer's behalf:", "") || "";
      if (!reason) return;
    }
    const unresolved = Object.values(this.data?.stats || {}).reduce(
      (sum, stats) => sum + (Number(stats?.unresolvedSuspectedCount) || 0),
      0,
    );
    const warningParts = [];
    if (needsCheck) {
      warningParts.push(`${needsCheck} individual(s) marked Needs check`);
    }
    if (unresolved) {
      warningParts.push(`${unresolved} unresolved suspicious issue occurrence(s)`);
    }
    const warning = warningParts.length
      ? ` Complete with ${warningParts.join(" and ")}? The unresolved flags will remain attached.`
      : "";
    if (!window.confirm(`Mark this study review complete?${warning}`)) return;
    try {
      await this.requestJSON(this.reviewActionUrl("review/complete"), {
        method: "POST",
        body: JSON.stringify(this.reviewMutationBody({ reason })),
      });
      await this.loadStudy({ preferredDatasetId: this.expectedCurrentDatasetId(), viewContext: this.captureDatasetViewContext() });
      this.setStatus("Review completed. The study can now be reassigned.");
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(`Could not complete review: ${error.message}`, true);
    }
  }

  async cancelCurrentReview() {
    const reason = window.prompt("Audit reason for cancelling this active review:", "");
    if (!reason?.trim()) return;
    if (!window.confirm("Cancel this review? Its decisions remain in audit history.")) return;
    try {
      await this.requestJSON(this.reviewActionUrl("review/cancel"), {
        method: "POST",
        body: JSON.stringify(this.reviewMutationBody({ reason })),
      });
      await this.loadStudy({ preferredDatasetId: this.expectedCurrentDatasetId(), viewContext: this.captureDatasetViewContext() });
      this.setStatus("Review cancelled. The study can now be assigned again.");
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(`Could not cancel review: ${error.message}`, true);
    }
  }

  async startCurrentEditorControl() {
    const control = this.editLockProfile?.editor_control;
    const reason = window.prompt(
      control ? "Audit reason for taking over editor control:" : "Why are you intervening in this review?",
      "",
    );
    if (!reason?.trim()) return;
    try {
      const action = control ? "editor-control/takeover" : "editor-control/start";
      await this.requestJSON(this.reviewActionUrl(action), {
        method: "POST",
        body: JSON.stringify(this.reviewMutationBody({ reason })),
      });
      await this.loadEditLockProfile();
      this.setStatus(control ? "Editor control taken over." : "Editor control started.");
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(`Could not start editor control: ${error.message}`, true);
    }
  }

  async finishCurrentEditorControl() {
    if (!window.confirm("Release editor control and return editing to the assigned reviewer?")) return;
    try {
      await this.requestJSON(this.reviewActionUrl("editor-control/finish"), {
        method: "POST",
        body: JSON.stringify(this.reviewMutationBody()),
      });
      await this.loadEditLockProfile();
      this.setStatus("Editor control released.");
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(`Could not release editor control: ${error.message}`, true);
    }
  }

  connectStudyEvents() {
    if (!this.currentFamily || !this.currentStudy || typeof EventSource === "undefined") return;
    const actor = window.vibecleaningActor || this.editLockProfile?.actor || {};
    if (String(actor.role || "") !== "reviewer") {
      this.closeStudyEvents();
      return;
    }
    const key = `${this.currentFamily}/${this.currentStudy}`;
    if (this.studyEvents && this.studyEventsKey === key) return;
    this.studyEvents?.close();
    this.studyEventsKey = key;
    const events = new EventSource(this.reviewActionUrl("events"));
    this.studyEvents = events;
    events.addEventListener("study_state_changed", event => {
      if (events !== this.studyEvents) return;
      let update;
      try { update = JSON.parse(event.data); } catch { return; }
      if (String(update.reason || "") !== "editor_control_released") return;
      const actor = window.vibecleaningActor || this.editLockProfile?.actor || {};
      if (String(actor.role || "") !== "reviewer") return;
      const targetUserId = String(update.target_user_id || "");
      if (!targetUserId || targetUserId !== String(actor.user_id || "")) return;
      const remoteHead = String(update.current_dataset_id || "");
      if (!remoteHead) return;
      this.editorReleaseDatasetId = remoteHead;
      if (this.refs.releaseNotice) this.refs.releaseNotice.hidden = false;
      this.setStatus("The editor has finished making changes. Load the latest version when ready.");
    });
  }

  async loadReleasedEditorChanges() {
    const datasetId = this.editorReleaseDatasetId;
    if (!datasetId) return;
    if (this.refs.releaseNotice) this.refs.releaseNotice.hidden = true;
    this.editorReleaseDatasetId = "";
    await this.transitionToDataset(datasetId, {
      preserveAnnotationContext: true,
      reason: "editor_release",
    });
  }

  closeStudyEvents() {
    this.studyEvents?.close();
    this.studyEvents = null;
    this.studyEventsKey = "";
    this.editorReleaseDatasetId = "";
    if (this.refs?.releaseNotice) this.refs.releaseNotice.hidden = true;
  }

  renderEditLockProfile() {
    this.renderReviewControls();
    if (!this.refs?.editLockProfile) {
      return;
    }
    const profile = this.editLockProfile || {};
    const blockers = Array.isArray(profile.blockers) ? profile.blockers : [];
    const locked = Boolean(this.currentDatasetId) && profile.editable !== true;
    this.refs.editLockProfile.classList.toggle("hidden", !locked);
    if (!locked) {
      this.refs.editLockMessage.textContent = "";
      this.refs.editLockProfile.removeAttribute("title");
      this.refs.resumeHistory.hidden = true;
      return;
    }
    const message = blockers.map(item => String(item?.message || "")).filter(Boolean).join(" ");
    const owner = blockers.find(item => item?.owner)?.owner;
    const explanation = (
      (message || "Persistent edits are locked.")
      + (owner ? ` Locked by ${owner}.` : "")
    );
    this.refs.editLockMessage.textContent = explanation;
    this.refs.editLockProfile.title = (
      `${explanation} Analyses, reports, exports, filtering, and visualization remain available.`
    );
    this.refs.resumeHistory.hidden = profile.resume?.allowed !== true;
  }

  async loadEditLockProfile() {
    if (!this.currentFamily || !this.currentStudy || !this.currentDatasetId) {
      this.closeStudyEvents();
      this.editLockProfile = {
        editable: false,
        blockers: [],
        current_dataset_id: "",
        selected_dataset_id: "",
        resume: { allowed: false },
      };
      this.renderEditLockProfile();
      this.updateActionButtons();
      return;
    }
    this.editLockProfile = {
      editable: false,
      blockers: [{
        code: "loading",
        message: "Checking whether this version is editable.",
      }],
      current_dataset_id: this.graph?.current_dataset_id || "",
      selected_dataset_id: this.currentDatasetId,
      resume: { allowed: false },
    };
    this.renderEditLockProfile();
    this.connectStudyEvents();
    this.updateActionButtons();
    try {
      const params = new URLSearchParams({ dataset_id: this.currentDatasetId });
      const profile = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/edit-profile?${params.toString()}`,
        { cache: "no-store" },
      );
      if (profile.selected_dataset_id !== this.currentDatasetId) {
        return;
      }
      this.editLockProfile = profile;
    } catch (error) {
      this.editLockProfile = {
        ...this.editLockProfile,
        editable: false,
        blockers: [{
          code: "profile_unavailable",
          message: `Edit status could not be verified: ${error.message}`,
        }],
      };
    }
    this.renderEditLockProfile();
    this.renderIndividuals();
    this.renderAnomalyRanking();
    this.updateActionButtons();
  }

  rejectLockedEdit() {
    if (this.canPersistEdits()) {
      return false;
    }
    const message = (this.editLockProfile?.blockers || [])
      .map(item => item?.message)
      .filter(Boolean)
      .join(" ");
    this.setStatus(message || "This version is read-only.", true);
    return true;
  }

  async handleEditRequestError(error) {
    if (error?.payload?.edit_profile) {
      this.editLockProfile = error.payload.edit_profile;
      this.renderEditLockProfile();
      this.renderIndividuals();
      this.renderAnomalyRanking();
      this.updateActionButtons();
      return true;
    }
    if (error?.status === 409) {
      const viewContext = this.captureDatasetViewContext();
      await this.loadStudy({
        preferredDatasetId: this.currentDatasetId,
        viewContext,
      });
      return true;
    }
    return false;
  }

  showOverlay(message) {
    this.refs.overlay.classList.remove("hidden");
    this.refs.overlay.querySelector("p").textContent = message;
  }

  hideOverlay() {
    this.refs.overlay.classList.add("hidden");
  }

  captureDatasetViewContext() {
    if (!this.data) {
      return null;
    }
    let mapView = null;
    if (this.map) {
      try {
        const center = this.map.getCenter();
        mapView = {
          center: [center.lng, center.lat],
          zoom: this.map.getZoom(),
          bearing: this.map.getBearing(),
          pitch: this.map.getPitch(),
        };
      } catch {}
    }
    return {
      selectedIndividuals: this.getSelectedIndividuals(),
      selectedFixKeys: new Set(this.data.selectedFixKeys),
      currentTimeMs: this.currentTimeMs,
      mapView,
      pendingIssueContext: this.pendingIssueContext,
      pendingConfirmationGroups: this.pendingConfirmationGroups,
    };
  }

  captureAnnotationReloadContext() {
    const queue = this.individualReviewQueue;
    return {
      anomalyRanking: this.hasCompatibleIndividualQueueRanking()
        ? this.anomalyRanking
        : null,
      queue: {
        orderMode: queue.orderMode,
        pageIndex: queue.pageIndex,
        groupIndex: queue.groupIndex,
        activeIndividual: queue.activeIndividual,
        mapScope: queue.mapScope,
        appliedRankingAnalysisId: queue.appliedRankingAnalysisId,
        pendingRankingAnalysisId: queue.pendingRankingAnalysisId,
        rankingMethod: queue.rankingMethod,
      },
    };
  }

  restoreAnnotationReloadContext(viewContext = null) {
    const preserved = viewContext?.annotationReloadContext;
    if (!preserved) {
      return;
    }
    const ranking = preserved.anomalyRanking;
    if (
      ranking?.status === "completed"
      && ranking.analysisId
      && Array.isArray(ranking.rankedIndividuals)
      && ranking.rankedIndividuals.length
    ) {
      this.anomalyRanking = ranking;
    }
    const queueState = preserved.queue || {};
    const queue = this.individualReviewQueue;
    queue.orderMode = queueState.orderMode === "ranking" ? "ranking" : "dataset";
    queue.pageIndex = Math.max(0, Number(queueState.pageIndex) || 0);
    queue.groupIndex = Math.max(0, Number(queueState.groupIndex) || 0);
    queue.activeIndividual = String(queueState.activeIndividual || "");
    queue.mapScope = ["solo", "group"].includes(queueState.mapScope)
      ? queueState.mapScope
      : "group";
    queue.appliedRankingAnalysisId = String(
      queueState.appliedRankingAnalysisId || "",
    );
    queue.pendingRankingAnalysisId = String(
      queueState.pendingRankingAnalysisId || "",
    );
    queue.rankingMethod = String(
      queueState.rankingMethod || this.getRankingMethod(),
    );
  }

  initializeDatasetView(viewContext = null) {
    if (!this.data) {
      return new Set();
    }
    this.setTableSelection();
    this.mapRangeAwaitingEnd = false;
    const preservedFixKeys = viewContext?.selectedFixKeys instanceof Set
      ? new Set(viewContext.selectedFixKeys)
      : new Set();
    const availableIndividuals = new Set(this.data.individuals);
    const selectedIndividuals = viewContext
      ? (viewContext.selectedIndividuals || []).filter(individual => availableIndividuals.has(individual))
      : initialMovementVisibleIndividuals(this.data);
    this.data.selectedIndividuals = new Set(selectedIndividuals);
    this.data.selectedFixKeys = new Set(
      [...preservedFixKeys].filter(key => this.data.fixByKey.has(key)),
    );
    const preservedTimeMs = Number(viewContext?.currentTimeMs);
    this.currentTimeMs = Number.isFinite(preservedTimeMs)
      ? clamp(preservedTimeMs, this.data.minTimeMs, this.data.maxTimeMs)
      : this.data.minTimeMs;
    this.refs.slider.min = String(this.data.minTimeMs);
    this.refs.slider.max = String(this.data.maxTimeMs);
    this.refs.slider.value = String(this.currentTimeMs);
    if (viewContext?.pendingIssueContext) {
      this.pendingIssueContext = viewContext.pendingIssueContext;
    }
    if (Array.isArray(viewContext?.pendingConfirmationGroups)) {
      this.pendingConfirmationGroups = viewContext.pendingConfirmationGroups;
    }
    return preservedFixKeys;
  }

  restoreDatasetMapView(viewContext = null) {
    if (viewContext?.mapView && this.map) {
      this.map.jumpTo(viewContext.mapView);
      return;
    }
    this.resetView();
  }

  clearLoadedStudyState() {
    this.clearThresholdState();
    this.gpsSpikeTurnAngleDeg = DEFAULT_GPS_SPIKE_TURN_ANGLE_DEG;
    this.clearOsmContext({ render: false });
    this.clearCandidateQueryPreview({ render: false });
    this.clearAnomalyRanking({ render: false });
    this.clearBurstFeatureSpace({ render: false });
    this.activeFixPopup = null;
    this.pendingIssueContext = null;
    this.pendingConfirmationGroups = [];
    this.pendingDismissalGroups = [];
    this.tableSelection = {
      anchorFixKey: "",
      focusFixKey: "",
      selectedFixKeys: new Set(),
      contiguousRange: false,
      selectionMethod: "",
    };
    this.resetManualFlagTarget({ resetKind: false });
    this.hiddenBurstIds.clear();
    this.flagTargetKind = "none";
    this.mapRangeAwaitingEnd = false;
    this.temporalSliderEngaged = false;
    if (this.pendingMapSingleClickTimer !== null) {
      window.clearTimeout(this.pendingMapSingleClickTimer);
      this.pendingMapSingleClickTimer = null;
    }
    this.tableRenderState = {
      signature: "",
      rowLimit: TABLE_INITIAL_ROW_LIMIT,
    };
    this.data = null;
    this.currentArtifactEntry = null;
    this.currentTimeMs = 0;
    this.lastReportLinks = [];
    this.refs.outputLinks.innerHTML = "";
    this.refs.selectSuspicious.textContent = "Review suspicious fixes";
    this.refs.individuals.innerHTML = "";
    this.refs.selectedFixes.innerHTML = "";
    this.renderAnomalyRanking();
    this.renderBurstFeatureSpace();
    this.refs.individualHead.textContent = "Individuals and coverage";
    this.refs.fixHead.textContent = "Checked fixes";
    this.refs.slider.min = "0";
    this.refs.slider.max = "0";
    this.refs.slider.value = "0";
    this.syncAnomalyFeatureSetOptions({ save: false });
    this.updateTimeLabel();
    this.renderBurstCountIndicator();
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
    this.closeStudyEvents();
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

  selectableArtifacts(dataset = this.currentDataset) {
    const artifacts = Array.isArray(dataset?.artifacts) ? dataset.artifacts : [];
    return artifacts.filter(artifact => {
      const logicalName = String(artifact?.logical_name || "");
      const expectedSuffix = MOVEMENT_APP_CONFIG.rdsSource ? ".rds" : ".csv";
      if (!logicalName.toLowerCase().endsWith(expectedSuffix)) {
        return false;
      }
      if (MOVEMENT_APP_CONFIG.mode !== "slim_movement") {
        return true;
      }
      const lowerName = logicalName.toLowerCase();
      return !lowerName.endsWith("_osm_context.csv") && !lowerName.endsWith("_reviewed.csv");
    });
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
        const reviewerName = study.review?.reviewer?.display_name || "";
        option.textContent = reviewerName
          ? `${study.name} · active review: ${reviewerName}`
          : `${study.name} · unassigned/history`;
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

  async loadStudy({ preferredDatasetId = "", viewContext = null } = {}) {
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
      const requestedDatasetId = preferredDatasetId
        || datasetId
        || this.currentDatasetId
        || state?.current_dataset?.dataset_id
        || "";
      this.refreshDatasetOptions(requestedDatasetId);
      const selectedDatasetId = this.currentDatasetId;
      if (selectedDatasetId && selectedDatasetId !== datasetId) {
        this.currentDataset = null;
        await this.loadDataset(viewContext);
        return;
      }
      this.currentDataset = dataset;
      this.currentDatasetId = selectedDatasetId;
      this.currentArtifact = artifactName;
      await this.loadEditLockProfile();
      if (this.currentDataset && Array.isArray(this.currentDataset.artifacts)) {
        const artifacts = this.selectableArtifacts(this.currentDataset);
        this.refs.artifact.innerHTML = "";
        for (const artifact of artifacts) {
          const option = document.createElement("option");
          option.value = artifact.logical_name;
          option.textContent = artifact.logical_name;
          this.refs.artifact.appendChild(option);
        }
        this.refs.artifact.disabled = !artifacts.length;
        if (artifactName) {
          this.refs.artifact.value = artifactName;
        }
      }
      this.saveUiState();
      if (!this.currentDataset || !summary || !artifactName) {
        await this.loadDataset(viewContext);
        return;
      }
      this.currentArtifactEntry = (this.currentDataset.artifacts || []).find(
        artifact => artifact.logical_name === artifactName,
      ) || null;
      this.clearLoadedStudyState();
      this.data = buildDatasetFromSummary(summary, this.uiState.colorBy);
      this.restoreAnnotationReloadContext(viewContext);
      this.syncAnomalyFeatureSetOptions({ save: false });
      const preservedFixKeys = this.initializeDatasetView(viewContext);
      this.initializeIndividualQueueDatasetSelection();
      const selectedIndividuals = this.getSelectedIndividuals();
      this.populateColorByOptions();
      this.anomalyRanking = {
        ...this.makeEmptyAnomalyRanking(),
        status: "checking",
      };
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderLegend();
      this.renderThresholdPane();
      this.updateTimeLabel();
      this.hideOverlay();
      void this.restoreSavedAnalyses();
      await this.rebuildMap(false);
      this.restoreDatasetMapView(viewContext);
      this.updateActionButtons();
      if (selectedIndividuals.length) {
        this.setStatus(
          this.data.detailState === "loaded"
            ? `Loaded ${formatCount(this.data.detailReturnedFixCount || this.data.detailFixes.length)} editable fixes for ${formatCount(selectedIndividuals.length)} visible individuals.`
            : `Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Loading editable fixes for the visible individuals...`,
        );
      } else {
        this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Select individuals to load fixes on demand.`);
      }
      void this.loadDetailForCurrentSelection({ preservedFixKeys });
      if (this.refs.showConfirmed.checked) {
        void this.loadConfirmedFixes();
      }
      void this.loadSuspiciousFixes({ focus: false });
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

  async loadDataset(viewContext = this.captureDatasetViewContext()) {
    this.cancelSelectionRequests("dataset");
    this.loadRequestId += 1;
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const datasetLoadId = ++this.datasetLoadId;
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
      await this.loadEditLockProfile();
      this.updateActionButtons();
    } catch (error) {
      if (this.isAbortError(error)) {
        return;
      }
      this.setStatus(error.message, true);
      this.showOverlay(`Could not load dataset ${this.currentDatasetId}.`);
      return;
    }
    const artifacts = this.selectableArtifacts(this.currentDataset);
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
    await this.loadArtifact(viewContext);
  }

  async loadArtifact(viewContext = this.captureDatasetViewContext()) {
    this.cancelSelectionRequests("artifact");
    this.clearAnomalyRanking();
    this.clearBurstFeatureSpace();
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
    this.renderBurstCountIndicator("Loading bursts...");
    try {
      const controller = this.beginRequest("overview");
      const overviewParams = new URLSearchParams({
        logical_name: artifactName,
        burst_gap_mode: this.getBurstGapMode(),
        burst_gap_seconds: String(this.getBurstGapSeconds()),
        burst_gap_quantile: String(this.getBurstGapQuantile()),
      });
      const summary = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/dataset/${encodeURIComponent(datasetId)}/overview?${overviewParams.toString()}`,
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
      const nextData = buildDatasetFromSummary(summary, this.uiState.colorBy);
      const availableIndividuals = new Set(nextData.individuals);
      const transitionIndividuals = viewContext
        ? (viewContext.selectedIndividuals || []).filter(individual => availableIndividuals.has(individual))
        : initialMovementVisibleIndividuals(nextData);
      const transitionRequestsWholeRdsStudy = (
        MOVEMENT_APP_CONFIG.rdsSource
        && transitionIndividuals.length === nextData.individuals.length
      );
      if (nextData.overviewTruncated && transitionIndividuals.length && !transitionRequestsWholeRdsStudy) {
        const detailPayload = await this.fetchJSON(
          this.buildFixesRequestUrl({
            familyName,
            studyName,
            datasetId,
            artifactName,
            individuals: transitionIndividuals,
            data: nextData,
          }),
          { signal: controller.signal },
        );
        nextData.detailState = "loaded";
        const payloadIndividuals = Array.isArray(detailPayload.detail_scope?.individuals)
          ? detailPayload.detail_scope.individuals.map(value => String(value)).filter(Boolean)
          : [];
        nextData.detailIndividuals = payloadIndividuals.length
          ? payloadIndividuals
          : [...transitionIndividuals];
        nextData.detailLimit = detailPayload.detail_scope?.limit ?? null;
        nextData.detailMatchingFixCount = Number(detailPayload.matching_fix_count) || 0;
        nextData.detailReturnedFixCount = Number(detailPayload.returned_fix_count) || 0;
        nextData.detailTruncated = Boolean(detailPayload.truncated);
        nextData.detailFixes = parseMovementFixes(detailPayload.fixes || []);
        nextData.detailSegments = parseMovementSegments(detailPayload.segments || []);
        nextData.detailAutoBursts = parseMovementAutoBursts(detailPayload.auto_bursts || []);
        refreshMovementFixCollections(nextData);
      }
      this.data = nextData;
      this.restoreAnnotationReloadContext(viewContext);
      this.syncAnomalyFeatureSetOptions({ save: false });
      this.renderBurstCountIndicator();
      const preservedFixKeys = this.initializeDatasetView(viewContext);
      this.initializeIndividualQueueDatasetSelection();
      const selectedIndividuals = this.getSelectedIndividuals();
      this.populateColorByOptions();
      this.anomalyRanking = {
        ...this.makeEmptyAnomalyRanking(),
        status: "checking",
      };
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderLegend();
      this.renderThresholdPane();
      this.updateTimeLabel();
      this.hideOverlay();
      void this.restoreSavedAnalyses();
      await this.rebuildMap(false);
      this.restoreDatasetMapView(viewContext);
      this.updateActionButtons();
      if (selectedIndividuals.length) {
        this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Loading editable fixes for the visible individuals...`);
      } else {
        this.setStatus(`Loaded overview for ${formatCount(this.data.totalRows)} fixes across ${formatCount(this.data.individuals.length)} individuals from ${this.currentArtifact}. Select individuals to load fixes on demand.`);
      }
      void this.loadDetailForCurrentSelection({ preservedFixKeys });
      if (this.refs.showConfirmed.checked) {
        void this.loadConfirmedFixes();
      }
      void this.loadSuspiciousFixes({ focus: false });
    } catch (error) {
      if (this.isAbortError(error) || requestId !== this.loadRequestId) {
        return;
      }
      this.setStatus(error.message, true);
      if (!this.data) {
        this.showOverlay(`Could not render ${this.currentArtifact}.`);
      }
    }
  }

  populateColorByOptions() {
    this.refs.colorBy.innerHTML = "";
    if (!this.data) {
      return;
    }
    const colorFields = this.data.colorFields.filter(field => (
      MOVEMENT_APP_CONFIG.osmDerivedFeatures || !String(field?.key || "").toLowerCase().startsWith("osm:")
    ));
    for (const field of colorFields) {
      const option = document.createElement("option");
      option.value = field.key;
      option.textContent = `${field.label} (${field.source})`;
      this.refs.colorBy.appendChild(option);
    }
    const preferred = colorFields.some(field => field.key === this.uiState.colorBy)
      ? this.uiState.colorBy
      : colorFields[0]?.key || "step_length_m";
    this.refs.colorBy.value = preferred;
    this.saveUiState();
  }

  initializeIndividualQueueDatasetSelection() {
    if (!this.data || this.individualReviewQueue.mode !== "queue") {
      return;
    }
    const available = new Set(this.data.individuals);
    if (!available.has(this.individualReviewQueue.activeIndividual)) {
      this.individualReviewQueue.activeIndividual = "";
    }
    this.data.selectedIndividuals = new Set(this.getIndividualQueueMapIndividuals());
  }

  captureCurrentMapView() {
    if (!this.map) {
      return null;
    }
    try {
      const center = this.map.getCenter();
      return {
        center: [center.lng, center.lat],
        zoom: this.map.getZoom(),
        bearing: this.map.getBearing(),
        pitch: this.map.getPitch(),
      };
    } catch {
      return null;
    }
  }

  getIndividualReviewState(individual) {
    const priorDecision = (
      this.editLockProfile?.coverage?.prior_decisions_by_individual?.[individual]
      || null
    );
    const saved = this.getSavedIndividualReviewState(individual, priorDecision);
    const draft = this.individualReviewQueue.stagedDecisions.get(individual);
    if (draft) {
      return {
        ...saved,
        reviewed: Boolean(draft.review_decision),
        reviewDecision: draft.review_decision,
        reviewOk: draft.review_decision === "ok",
        needsCheck: draft.needs_check === true,
        comment: draft.comment || "",
        staged: true,
      };
    }
    return saved;
  }

  getSavedIndividualReviewState(individual, priorDecision = null) {
    priorDecision = priorDecision || (
      this.editLockProfile?.coverage?.prior_decisions_by_individual?.[individual]
      || null
    );
    const stats = this.data?.stats?.[individual] || {};
    return {
      reviewed: stats.reviewed === true,
      reviewDecision: stats.reviewDecision || "",
      reviewOk: stats.reviewDecision === "ok",
      needsCheck: stats.needsCheck === true,
      priorDecision,
      priorNeedsCheck: priorDecision?.needs_check === true,
      comment: stats.reviewComment || "",
      staged: false,
    };
  }

  individualReviewDraftDiffers(individual) {
    const draft = this.individualReviewQueue.stagedDecisions.get(individual);
    if (!draft) return false;
    const saved = this.getSavedIndividualReviewState(individual);
    return (
      String(draft.review_decision || "") !== String(saved.reviewDecision || "")
      || Boolean(draft.needs_check) !== Boolean(saved.needsCheck)
      || String(draft.comment || "").trim() !== String(saved.comment || "").trim()
    );
  }

  discardRedundantIndividualReviewDraft(individual) {
    if (this.individualReviewDraftDiffers(individual)) return;
    this.individualReviewQueue.stagedDecisions.delete(individual);
    this.individualReviewQueue.commentDrafts.delete(individual);
  }

  hasUnsavedIndividualReviewDrafts() {
    return [...this.individualReviewQueue.stagedDecisions.keys()]
      .some(individual => this.individualReviewDraftDiffers(individual));
  }

  confirmDiscardIndividualReviewDrafts() {
    if (!this.hasUnsavedIndividualReviewDrafts()) return true;
    if (!window.confirm("Discard unsaved individual review decision(s)?")) return false;
    this.individualReviewQueue.stagedDecisions.clear();
    this.individualReviewQueue.commentDrafts.clear();
    this.individualReviewQueue.commentEditingIndividual = "";
    this.renderIndividuals();
    return true;
  }

  hasCompatibleIndividualQueueRanking() {
    return (
      this.anomalyRanking?.status === "completed"
      && Boolean(this.anomalyRanking.analysisId)
      && Array.isArray(this.anomalyRanking.rankedIndividuals)
      && this.anomalyRanking.rankedIndividuals.length > 0
    );
  }

  getIndividualQueueOrder() {
    const individuals = this.data?.individuals || [];
    const visibleIndividuals = individuals.filter(individual => {
      const state = this.getIndividualReviewState(individual);
      if (this.individualReviewQueue.filterMode === "unresolved") {
        return Number(this.data?.stats?.[individual]?.unresolvedSuspectedCount) > 0;
      }
      return this.individualReviewQueue.filterMode !== "needs_check"
        || state.needsCheck
        || state.priorNeedsCheck;
    });
    const datasetIndex = new Map(this.data?.individuals?.map((individual, index) => [individual, index]) || []);
    const rankingIndex = new Map(
      (this.anomalyRanking?.rankedIndividuals || [])
        .map((row, index) => [String(row?.individual || ""), Number(row?.rank) || index + 1]),
    );
    const useRanking = (
      this.individualReviewQueue.orderMode === "ranking"
      && this.hasCompatibleIndividualQueueRanking()
      && this.individualReviewQueue.appliedRankingAnalysisId === this.anomalyRanking.analysisId
      && this.individualReviewQueue.rankingMethod === this.anomalyRanking.rankingMethod
    );
    return [...visibleIndividuals].sort((left, right) => {
      if (useRanking) {
        const leftRank = rankingIndex.get(left) ?? Number.MAX_SAFE_INTEGER;
        const rightRank = rankingIndex.get(right) ?? Number.MAX_SAFE_INTEGER;
        if (leftRank !== rightRank) {
          return leftRank - rightRank;
        }
      }
      return (datasetIndex.get(left) ?? Number.MAX_SAFE_INTEGER)
        - (datasetIndex.get(right) ?? Number.MAX_SAFE_INTEGER)
        || left.localeCompare(right);
    });
  }

  getIndividualQueuePosition() {
    const ordered = this.getIndividualQueueOrder();
    const pageCount = Math.max(1, Math.ceil(ordered.length / INDIVIDUAL_QUEUE_PAGE_SIZE));
    this.individualReviewQueue.pageIndex = clamp(
      this.individualReviewQueue.pageIndex,
      0,
      pageCount - 1,
    );
    const pageStart = this.individualReviewQueue.pageIndex * INDIVIDUAL_QUEUE_PAGE_SIZE;
    const page = ordered.slice(pageStart, pageStart + INDIVIDUAL_QUEUE_PAGE_SIZE);
    const groupCount = Math.max(1, Math.ceil(page.length / INDIVIDUAL_QUEUE_GROUP_SIZE));
    this.individualReviewQueue.groupIndex = clamp(
      this.individualReviewQueue.groupIndex,
      0,
      groupCount - 1,
    );
    const groupStart = this.individualReviewQueue.groupIndex * INDIVIDUAL_QUEUE_GROUP_SIZE;
    const group = page.slice(groupStart, groupStart + INDIVIDUAL_QUEUE_GROUP_SIZE);
    if (!page.includes(this.individualReviewQueue.activeIndividual)) {
      this.individualReviewQueue.activeIndividual = group[0] || page[0] || "";
    } else {
      const activePageIndex = page.indexOf(this.individualReviewQueue.activeIndividual);
      this.individualReviewQueue.groupIndex = Math.floor(
        activePageIndex / INDIVIDUAL_QUEUE_GROUP_SIZE,
      );
    }
    const activeIndex = ordered.indexOf(this.individualReviewQueue.activeIndividual);
    return {
      ordered,
      page,
      group: page.slice(
        this.individualReviewQueue.groupIndex * INDIVIDUAL_QUEUE_GROUP_SIZE,
        (this.individualReviewQueue.groupIndex + 1) * INDIVIDUAL_QUEUE_GROUP_SIZE,
      ),
      pageCount,
      groupCount,
      activeIndex,
    };
  }

  resetManualFlagTarget({ resetKind = true } = {}) {
    this.manualFlagTarget = {
      individual: "",
      burstIds: new Set(),
      selectionMethods: new Set(),
      origin: "manual",
      sourceAnalysisId: "",
    };
    if (resetKind && ["individual", "bursts"].includes(this.flagTargetKind)) {
      this.flagTargetKind = "none";
    }
  }

  flagTargetBursts() {
    if (!this.data || this.flagTargetKind !== "bursts") return [];
    return [...this.manualFlagTarget.burstIds]
      .map(burstId => this.data.autoBurstById?.get(burstId))
      .filter(Boolean)
      .sort((left, right) => (
        left.startTimeMs - right.startTimeMs
        || left.burstIdx - right.burstIdx
        || left.burstId.localeCompare(right.burstId)
      ));
  }

  selectEntireIndividualFlagTarget(individual) {
    individual = String(individual || "");
    if (!this.data?.individuals?.includes(individual)) return;
    this.resetManualFlagTarget({ resetKind: false });
    this.data.selectedFixKeys = new Set();
    this.setTableSelection();
    this.mapRangeAwaitingEnd = false;
    this.manualFlagTarget.individual = individual;
    this.manualFlagTarget.selectionMethods.add("queue_individual_control");
    this.flagTargetKind = "individual";
    this.renderIndividuals();
    this.renderLayers();
    this.updateActionButtons();
    this.setStatus(`Selected the entire individual ${individual} as the flag target.`);
  }

  setBurstFlagTargetIncluded(
    burstId,
    included,
    {
      selectionMethod = "map_burst_click",
      origin = "manual",
      sourceAnalysisId = "",
      replace = false,
    } = {},
  ) {
    const burst = this.data?.autoBurstById?.get(String(burstId || ""));
    if (!burst) return;
    const activeQueueIndividual = this.individualReviewQueue.mode === "queue"
      ? this.individualReviewQueue.activeIndividual
      : "";
    if (activeQueueIndividual && burst.individual !== activeQueueIndividual) {
      this.setStatus("Choose bursts belonging to the active queue individual.", true);
      this.renderIndividuals();
      return;
    }
    if (replace || this.flagTargetKind !== "bursts") {
      this.resetManualFlagTarget({ resetKind: false });
      this.data.selectedFixKeys = new Set();
      this.setTableSelection();
      this.mapRangeAwaitingEnd = false;
    }
    this.flagTargetKind = "bursts";
    this.manualFlagTarget.individual = activeQueueIndividual || burst.individual;
    this.manualFlagTarget.origin = origin;
    this.manualFlagTarget.sourceAnalysisId = String(sourceAnalysisId || "");
    this.manualFlagTarget.selectionMethods.add(selectionMethod);
    if (included) {
      this.manualFlagTarget.burstIds.add(burst.burstId);
    } else {
      this.manualFlagTarget.burstIds.delete(burst.burstId);
    }
    if (!this.manualFlagTarget.burstIds.size) {
      this.resetManualFlagTarget();
    }
    if (this.individualReviewQueue.mode === "queue") {
      this.renderIndividuals();
    }
    this.renderLayers();
    this.updateActionButtons();
    const count = this.manualFlagTarget.burstIds.size;
    this.setStatus(count
      ? `Selected ${formatCount(count)} burst(s) as the flag target.`
      : "Cleared the burst flag target.");
  }

  setBurstVisible(burstId, visible) {
    const burst = this.data?.autoBurstById?.get(String(burstId || ""));
    if (!burst) return;
    const activeQueueIndividual = this.individualReviewQueue.mode === "queue"
      ? this.individualReviewQueue.activeIndividual
      : "";
    if (activeQueueIndividual && burst.individual !== activeQueueIndividual) {
      this.setStatus("Change visibility only for bursts belonging to the active queue individual.", true);
      return;
    }
    if (visible) {
      this.hiddenBurstIds.delete(burst.burstId);
    } else {
      this.hiddenBurstIds.add(burst.burstId);
    }
    if (this.individualReviewQueue.mode === "queue") {
      this.renderIndividuals();
    }
    if (this.refs?.sideSheetTabs?.dataset.activeSheet === "table") {
      this.renderTableSheet();
    }
    this.renderLayers();
    this.setStatus(
      visible
        ? `Restored burst ${burst.burstId} to the map.`
        : `Hid the ordinary points and track steps for burst ${burst.burstId}.`,
    );
  }

  clearFlagTargetForQueueIndividualChange(previousIndividual, nextIndividual) {
    if (!previousIndividual || previousIndividual === nextIndividual) return;
    this.hiddenBurstIds.clear();
    this.focusedRankingBurst = null;
    this.resetManualFlagTarget();
    if (["fixes", "segment"].includes(this.flagTargetKind)) {
      this.data.selectedFixKeys = new Set();
      this.setTableSelection();
      this.mapRangeAwaitingEnd = false;
      this.flagTargetKind = "none";
    }
  }

  queueFlagTargetControlsHtml(individual) {
    const disabled = this.canPersistEdits() ? "" : " disabled";
    const bursts = (this.data?.autoBursts || [])
      .filter(burst => burst.individual === individual)
      .sort((left, right) => (
        left.startTimeMs - right.startTimeMs
        || left.burstIdx - right.burstIdx
        || left.burstId.localeCompare(right.burstId)
      ));
    const burstDetailsLoading = (
      this.data?.detailState === "loading"
      && (this.data?.detailIndividuals || []).includes(individual)
    );
    const entireSelected = (
      this.flagTargetKind === "individual"
      && this.manualFlagTarget.individual === individual
    );
    const selectedBurstIds = this.flagTargetKind === "bursts"
      ? this.manualFlagTarget.burstIds
      : new Set();
    const currentTarget = entireSelected
      ? "Entire individual selected"
      : selectedBurstIds.size
        ? `${formatCount(selectedBurstIds.size)} burst(s) selected`
        : this.flagTargetKind === "segment"
          ? "Track section selected"
          : this.flagTargetKind === "fixes" && this.getSelectedFixes().length
            ? `${formatCount(this.getSelectedFixes().length)} fix(es) checked`
            : "Choose a target";
    return `
      <div class="movement-queue-flag-target">
        <div class="movement-queue-flag-target-head">
          <strong>Flag target</strong>
          <span>${escapeHtml(currentTarget)}</span>
        </div>
        <div class="movement-queue-card-actions">
          <button
            type="button"
            data-queue-flag-individual
            data-individual="${escapeHtml(individual)}"
            class="${entireSelected ? "is-active" : ""}"
            ${disabled}
          >Entire individual</button>
          <span class="movement-subtle">Visible controls ordinary map points. Flag controls the next issue step.</span>
        </div>
        ${bursts.length ? `
          <div class="movement-queue-flag-bursts">
            ${bursts.map(burst => {
              const selected = selectedBurstIds.has(burst.burstId);
              const visible = !this.hiddenBurstIds.has(burst.burstId);
              return `
                <div class="movement-queue-flag-burst${visible ? " is-visible" : " is-hidden"}${selected ? " is-selected" : ""}" data-queue-burst-controls>
                  <label class="movement-queue-burst-show">
                    <input
                      type="checkbox"
                      data-queue-burst-visible="${escapeHtml(burst.burstId)}"
                      ${visible ? "checked" : ""}
                    >
                    <span>
                      <strong>Visible ${escapeHtml(`burst ${formatCount(burst.burstIdx + 1)}`)}</strong>
                      • ${escapeHtml(burst.setName)}
                      • ${escapeHtml(formatTimestamp(burst.startTimeMs))}
                      • ${escapeHtml(`${formatCount(burst.fixCount)} fixes`)}
                    </span>
                  </label>
                  <label class="movement-queue-burst-flag">
                    <input
                      type="checkbox"
                      data-queue-flag-burst="${escapeHtml(burst.burstId)}"
                      ${selected ? "checked" : ""}
                      ${disabled}
                    >
                    <span>Flag</span>
                  </label>
                </div>
              `;
            }).join("")}
          </div>
        ` : burstDetailsLoading
          ? '<span class="movement-subtle">Loading bursts for this individual…</span>'
          : '<span class="movement-subtle">No bursts are available under the current burst definition.</span>'}
      </div>
    `;
  }

  async setIndividualViewMode(mode) {
    const nextMode = mode === "queue" ? "queue" : "browse";
    const queue = this.individualReviewQueue;
    if (!this.data) {
      return;
    }
    if (queue.mode === nextMode) {
      if (nextMode === "queue") {
        this.setSideSheet("individuals");
        this.renderIndividuals();
      }
      return;
    }
    if (nextMode === "queue") {
      queue.browseContext = this.captureDatasetViewContext();
      queue.browseSideSheet = this.refs.sideSheetTabs?.dataset.activeSheet || "individuals";
      queue.mode = "queue";
      queue.activeIndividual = "";
      this.hiddenBurstIds.clear();
      this.resetManualFlagTarget();
      this.flagTargetKind = "none";
      queue.mapScope = "group";
      this.setSideSheet("individuals", { save: false });
      this.renderIndividuals();
      if (this.individualQueueListHeightPx !== null) {
        this.applyIndividualListHeight(this.individualQueueListHeightPx, { save: false });
      }
      await this.applyIndividualQueueMapScope({ zoom: !queue.queueMapView });
      if (queue.queueMapView && this.map) {
        this.map.jumpTo(queue.queueMapView);
      }
    } else {
      queue.queueMapView = this.captureCurrentMapView();
      queue.mode = "browse";
      this.hiddenBurstIds.clear();
      this.resetManualFlagTarget();
      this.flagTargetKind = this.data.selectedFixKeys.size ? "fixes" : "none";
      const context = queue.browseContext;
      if (context) {
        const available = new Set(this.data.individuals);
        this.data.selectedIndividuals = new Set(
          (context.selectedIndividuals || []).filter(individual => available.has(individual)),
        );
        this.data.selectedFixKeys = new Set(context.selectedFixKeys || []);
        this.currentTimeMs = clamp(
          Number(context.currentTimeMs) || this.data.minTimeMs,
          this.data.minTimeMs,
          this.data.maxTimeMs,
        );
        this.refs.slider.value = String(this.currentTimeMs);
        this.updateTimeLabel();
      }
      this.renderIndividuals();
      if (this.individualListHeightPx !== null) {
        this.applyIndividualListHeight(this.individualListHeightPx, { save: false });
      }
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      await this.loadDetailForCurrentSelection();
      this.setSideSheet(queue.browseSideSheet || "individuals", { save: false });
      if (context?.mapView && this.map) {
        this.map.jumpTo(context.mapView);
      }
    }
    this.saveUiState();
  }

  getIndividualQueueMapIndividuals() {
    const position = this.getIndividualQueuePosition();
    if (this.individualReviewQueue.mapScope === "all") {
      return [...this.data.individuals];
    }
    if (this.individualReviewQueue.mapScope === "solo") {
      return this.individualReviewQueue.activeIndividual
        ? [this.individualReviewQueue.activeIndividual]
        : [];
    }
    return position.group;
  }

  async applyIndividualQueueMapScope({ zoom = true } = {}) {
    if (!this.data || this.individualReviewQueue.mode !== "queue") {
      return;
    }
    const individuals = this.getIndividualQueueMapIndividuals();
    this.data.selectedIndividuals = new Set(individuals);
    this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(
      this.data.selectedFixKeys,
      individuals,
    );
    this.renderIndividuals();
    this.renderSelectedFixes();
    this.renderThresholdPane();
    this.renderLayers();
    this.updateActionButtons();
    await this.loadDetailForCurrentSelection();
    if (zoom) {
      this.zoomToIndividualQueueActive();
    }
  }

  async setIndividualQueueMapScope(scope) {
    if (!["solo", "group"].includes(scope)) {
      return;
    }
    this.individualReviewQueue.mapScope = scope;
    await this.applyIndividualQueueMapScope();
  }

  zoomToIndividualQueueActive() {
    const individual = this.individualReviewQueue.activeIndividual;
    if (!individual || !this.data) {
      return;
    }
    const path = this.data.fixes
      .filter(fix => fix.individual === individual)
      .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey))
      .map(fix => fix.position);
    if (path.length) {
      this.zoomToPath(path);
      return;
    }
    const fallback = Object.values(this.data.seriesByIndividual[individual] || {})
      .flatMap(series => series.positions || []);
    this.zoomToPath(fallback);
  }

  async focusIndividualQueueItem(individual, { zoom = true } = {}) {
    if (!this.data || !individual) {
      return;
    }
    const previousIndividual = this.individualReviewQueue.activeIndividual;
    const previousGroup = this.getIndividualQueuePosition().group.join("\u0000");
    const ordered = this.getIndividualQueueOrder();
    const orderedIndex = ordered.indexOf(individual);
    if (orderedIndex < 0) {
      return;
    }
    this.individualReviewQueue.pageIndex = Math.floor(
      orderedIndex / INDIVIDUAL_QUEUE_PAGE_SIZE,
    );
    const pageOffset = orderedIndex % INDIVIDUAL_QUEUE_PAGE_SIZE;
    this.individualReviewQueue.groupIndex = Math.floor(
      pageOffset / INDIVIDUAL_QUEUE_GROUP_SIZE,
    );
    this.individualReviewQueue.activeIndividual = individual;
    this.clearFlagTargetForQueueIndividualChange(previousIndividual, individual);
    const position = this.getIndividualQueuePosition();
    const nextGroup = position.group.join("\u0000");
    if (
      this.individualReviewQueue.mapScope === "solo"
      || (this.individualReviewQueue.mapScope === "group" && previousGroup !== nextGroup)
    ) {
      await this.applyIndividualQueueMapScope({ zoom });
      return;
    }
    this.renderIndividuals();
    this.renderLayers();
    if (zoom) {
      this.zoomToIndividualQueueActive();
    }
  }

  async navigateIndividualQueue(action) {
    if (!this.data || this.individualReviewQueue.mode !== "queue") {
      return;
    }
    let position = this.getIndividualQueuePosition();
    if (action === "previous-page" || action === "next-page") {
      const direction = action === "next-page" ? 1 : -1;
      const targetPageIndex = clamp(
        this.individualReviewQueue.pageIndex + direction,
        0,
        position.pageCount - 1,
      );
      const targetIndividual = position.ordered[
        targetPageIndex * INDIVIDUAL_QUEUE_PAGE_SIZE
      ] || "";
      if (targetIndividual && this.data?.individuals.includes(targetIndividual)) {
        await this.focusIndividualQueueItem(targetIndividual);
      } else {
        position = this.getIndividualQueuePosition();
        this.individualReviewQueue.pageIndex = clamp(
          targetPageIndex,
          0,
          position.pageCount - 1,
        );
        this.individualReviewQueue.groupIndex = 0;
        this.individualReviewQueue.activeIndividual = "";
        await this.applyIndividualQueueMapScope();
      }
      this.saveUiState();
      return;
    }
    if (action === "previous-group" || action === "next-group") {
      const direction = action === "next-group" ? 1 : -1;
      const nextGroupIndex = clamp(
        this.individualReviewQueue.groupIndex + direction,
        0,
        position.groupCount - 1,
      );
      const target = position.page[nextGroupIndex * INDIVIDUAL_QUEUE_GROUP_SIZE]
        || position.page[0];
      await this.focusIndividualQueueItem(target);
      return;
    }
    const direction = action === "next-individual" ? 1 : -1;
    const pageIndex = position.page.indexOf(this.individualReviewQueue.activeIndividual);
    const nextPageIndex = clamp(pageIndex + direction, 0, Math.max(0, position.page.length - 1));
    await this.focusIndividualQueueItem(position.page[nextPageIndex]);
  }

  stageIndividualReviewDecision(individual, reviewDecision) {
    if (this.rejectLockedEdit()) {
      return;
    }
    individual = individual || this.individualReviewQueue.activeIndividual;
    if (!individual) {
      return;
    }
    if (!["ok", "fix_keep", "remove"].includes(reviewDecision)) return;
    const existingState = this.getIndividualReviewState(individual);
    const comment = (
      this.individualReviewQueue.commentDrafts.has(individual)
        ? this.individualReviewQueue.commentDrafts.get(individual)
        : existingState.comment
    ).trim();
    this.individualReviewQueue.stagedDecisions.set(individual, {
      individual,
      review_decision: reviewDecision,
      needs_check: existingState.needsCheck === true,
      comment,
    });
    this.discardRedundantIndividualReviewDraft(individual);
    this.individualReviewQueue.skippedIndividuals.delete(individual);
    this.renderIndividuals();
  }

  stageIndividualNeedsCheck(individual, needsCheck) {
    if (this.rejectLockedEdit()) return;
    individual = individual || this.individualReviewQueue.activeIndividual;
    if (!individual) return;
    const existingState = this.getIndividualReviewState(individual);
    const comment = (
      this.individualReviewQueue.commentDrafts.has(individual)
        ? this.individualReviewQueue.commentDrafts.get(individual)
        : existingState.comment
    ).trim();
    this.individualReviewQueue.stagedDecisions.set(individual, {
      individual,
      review_decision: existingState.reviewDecision || "",
      needs_check: needsCheck === true,
      comment,
    });
    this.discardRedundantIndividualReviewDraft(individual);
    this.renderIndividuals();
  }

  updateIndividualReviewCommentDraft(individual, comment) {
    if (!individual) return;
    this.individualReviewQueue.commentDrafts.set(individual, comment);
    const existingState = this.getIndividualReviewState(individual);
    this.individualReviewQueue.stagedDecisions.set(individual, {
      individual,
      review_decision: existingState.reviewDecision || "",
      needs_check: existingState.needsCheck === true,
      comment,
    });
    this.discardRedundantIndividualReviewDraft(individual);
    this.updateIndividualDecisionSaveButton();
  }

  async skipIndividualQueueItem(individual) {
    individual = individual || this.individualReviewQueue.activeIndividual;
    if (!individual) {
      return;
    }
    const position = this.getIndividualQueuePosition();
    const currentIndex = position.page.indexOf(individual);
    const nextIndividual = position.page[currentIndex + 1] || "";
    this.individualReviewQueue.skippedIndividuals.add(individual);
    this.renderIndividuals();
    if (nextIndividual && nextIndividual !== individual) {
      await this.focusIndividualQueueItem(nextIndividual);
    }
  }

  toggleIndividualQueueComment(individual) {
    if (!individual) {
      return;
    }
    const queue = this.individualReviewQueue;
    if (queue.commentEditingIndividual === individual) {
      queue.commentEditingIndividual = "";
    } else {
      queue.commentEditingIndividual = individual;
      if (!queue.commentDrafts.has(individual)) {
        queue.commentDrafts.set(
          individual,
          this.getIndividualReviewState(individual).comment || "",
        );
      }
    }
    this.renderIndividuals();
    if (queue.commentEditingIndividual === individual) {
      window.requestAnimationFrame(() => {
        this.refs.individuals
          .querySelector(`input[data-queue-comment-input][data-individual="${cssEscape(individual)}"]`)
          ?.focus();
      });
    }
  }

  async viewIndividualQueueTable(individual) {
    if (!individual || !this.data) {
      return;
    }
    await this.focusIndividualQueueItem(individual, { zoom: false });
    this.refs.tableFilter.value = individual;
    this.saveUiState();
    this.setSideSheet("table");
  }

  async saveActiveIndividualReviewDecision() {
    const queue = this.individualReviewQueue;
    const individual = queue.activeIndividual;
    const decision = queue.stagedDecisions.get(individual);
    if (!individual || !decision || !decision.review_decision) return false;
    if (!this.individualReviewDraftDiffers(individual)) return false;
    if (this.rejectLockedEdit()) {
      return false;
    }
    if (queue.saving || !this.currentArtifact) {
      return false;
    }
    queue.saving = true;
    this.renderIndividuals();
    const position = this.getIndividualQueuePosition();
    const currentIndex = position.page.indexOf(individual);
    const nextIndividual = position.page[currentIndex + 1] || "";
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/review-individual`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            expected_current_dataset_id: this.expectedCurrentDatasetId(),
            expected_review_revision: this.expectedReviewRevision(),
            logical_name: this.currentArtifact,
            source_bundle_signature: this.data?.sourceSignature || "",
            decision: {
              individual,
              review_decision: decision.review_decision,
              needs_check: decision.needs_check === true,
              comment: String(decision.comment || "").trim(),
            },
            user: this.getUser() || "reviewer",
          }),
        },
      );
      queue.stagedDecisions.delete(individual);
      queue.commentDrafts.delete(individual);
      if (queue.commentEditingIndividual === individual) {
        queue.commentEditingIndividual = "";
      }
      await this.loadStudyAtDataset(
        result.dataset.dataset_id,
        { preserveAnnotationContext: true, result },
      );
      if (nextIndividual && nextIndividual !== individual) {
        await this.focusIndividualQueueItem(nextIndividual, { zoom: false });
      }
      this.setStatus(`Saved the individual review decision for ${individual}.`);
      return true;
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(`Could not save the individual review decision: ${error.message}`, true);
      return false;
    } finally {
      queue.saving = false;
      this.renderIndividuals();
    }
  }

  async changeIndividualQueueOrder(orderValue) {
    const rankingMethods = new Set(this.rankingMethodOptions().map(([value]) => value));
    const requestedMethod = rankingMethods.has(orderValue) ? orderValue : "";
    if (!requestedMethod) {
      this.individualReviewQueue.orderMode = "dataset";
      this.individualReviewQueue.appliedRankingAnalysisId = "";
      this.individualReviewQueue.pendingRankingAnalysisId = "";
    } else {
      const methodChanged = requestedMethod !== this.getRankingMethod();
      this.individualReviewQueue.rankingMethod = requestedMethod;
      if (methodChanged) {
        this.refs.rankingMethod.value = requestedMethod;
        this.syncRankingMethodControl();
        this.saveUiState();
        this.clearAnomalyRanking({ render: false });
        await this.restoreSavedAnalyses();
      }
      if (["available", "restore_error"].includes(this.anomalyRanking?.status)) {
        await this.loadSavedAnomalyRanking();
      }
      this.individualReviewQueue.orderMode = "ranking";
      if (
        this.hasCompatibleIndividualQueueRanking()
        && this.anomalyRanking.rankingMethod === requestedMethod
      ) {
        this.individualReviewQueue.appliedRankingAnalysisId = this.anomalyRanking.analysisId;
        this.individualReviewQueue.pendingRankingAnalysisId = "";
      } else {
        this.individualReviewQueue.appliedRankingAnalysisId = "";
      }
    }
    this.individualReviewQueue.pageIndex = 0;
    this.individualReviewQueue.groupIndex = 0;
    this.individualReviewQueue.activeIndividual = "";
    this.saveUiState();
    await this.applyIndividualQueueMapScope();
  }

  noteCompletedIndividualQueueRanking() {
    if (!this.hasCompatibleIndividualQueueRanking()) {
      return;
    }
    const queue = this.individualReviewQueue;
    if (queue.appliedRankingAnalysisId !== this.anomalyRanking.analysisId) {
      if (
        queue.orderMode === "ranking"
        && queue.rankingMethod === this.anomalyRanking.rankingMethod
      ) {
        queue.appliedRankingAnalysisId = this.anomalyRanking.analysisId;
        queue.pendingRankingAnalysisId = "";
      } else {
        queue.pendingRankingAnalysisId = this.anomalyRanking.analysisId;
      }
    }
  }

  async applyCompletedIndividualQueueRanking() {
    if (!this.hasCompatibleIndividualQueueRanking()) {
      return;
    }
    this.individualReviewQueue.orderMode = "ranking";
    this.individualReviewQueue.rankingMethod = this.anomalyRanking.rankingMethod;
    this.individualReviewQueue.appliedRankingAnalysisId = this.anomalyRanking.analysisId;
    this.individualReviewQueue.pendingRankingAnalysisId = "";
    this.individualReviewQueue.pageIndex = 0;
    this.individualReviewQueue.groupIndex = 0;
    this.individualReviewQueue.activeIndividual = "";
    this.saveUiState();
    await this.applyIndividualQueueMapScope();
  }

  renderIndividualQueueRankingState() {
    const target = this.refs.individualQueueRankingState;
    if (!target) {
      return;
    }
    target.classList.remove("error");
    const rankingMethod = String(
      this.individualReviewQueue.rankingMethod || this.getRankingMethod(),
    );
    const rankingLabel = this.rankingMethodLabel(rankingMethod);
    if (this.anomalyRanking?.status === "checking") {
      target.innerHTML = (
        `<span class="movement-queue-ranking-copy">Checking for a saved ${escapeHtml(rankingLabel)} ranking…</span>`
        + '<button type="button" disabled>Checking…</button>'
      );
      return;
    }
    if (this.anomalyRanking?.status === "restoring") {
      target.innerHTML = (
        `<span class="movement-queue-ranking-copy">Loading the saved ${escapeHtml(rankingLabel)} ranking.</span>`
        + '<button type="button" disabled>Loading…</button>'
      );
      return;
    }
    if (this.anomalyRanking?.status === "loading") {
      target.innerHTML = (
        `<span class="movement-queue-ranking-copy">${escapeHtml(rankingLabel)} ranking in progress.</span>`
        + '<button type="button" disabled>Running…</button>'
      );
      return;
    }
    if (this.anomalyRanking?.status === "history_error") {
      target.classList.add("error");
      target.innerHTML = (
        `<span class="movement-queue-ranking-copy" title="${escapeHtml(this.anomalyRanking.restoreError || "")}">Could not check saved burst rankings.</span>`
        + '<button type="button" data-queue-action="check-ranking">Try again</button>'
      );
      return;
    }
    if (["available", "restore_error"].includes(this.anomalyRanking?.status)) {
      const metadata = [
        this.anomalyRanking.createdAt
          ? `Saved ranking ${formatDateTime(this.anomalyRanking.createdAt)}`
          : `${rankingLabel} ranking available`,
        this.burstGapLabel(),
      ].filter(Boolean).join(" • ");
      target.innerHTML = (
        `<span class="movement-queue-ranking-copy" title="${escapeHtml(metadata)}">${escapeHtml(metadata)}</span>`
        + `<button type="button" data-queue-action="load-ranking">${this.anomalyRanking.status === "restore_error" ? "Retry loading" : "Load ranking"}</button>`
      );
      return;
    }
    if (!this.hasCompatibleIndividualQueueRanking()) {
      target.classList.add("error");
      target.innerHTML = (
        `<span class="movement-queue-ranking-copy" title="No compatible ${escapeHtml(rankingLabel)} ranking is available for this dataset version and burst definition.">`
        + `No compatible ${escapeHtml(rankingLabel)} ranking is available.</span>`
        + '<button type="button" data-queue-action="run-ranking">Run burst ranking</button>'
      );
      return;
    }
    const burstSettings = this.anomalyRanking.burstGap
      ? formatBurstGapMetadata(parseMovementBurstGap({ burst_gap: this.anomalyRanking.burstGap }))
      : this.burstGapLabel();
    const metadata = [
      this.anomalyRanking.createdAt
        ? `${rankingLabel} • ${formatDateTime(this.anomalyRanking.createdAt)}`
        : `${rankingLabel} available`,
      burstSettings,
    ].filter(Boolean).join(" • ");
    const needsApply = (
      this.individualReviewQueue.appliedRankingAnalysisId
      !== this.anomalyRanking.analysisId
    );
    target.innerHTML = (
      `<span class="movement-queue-ranking-copy" title="${escapeHtml(metadata)}">${escapeHtml(metadata)}</span>`
      + (needsApply
        ? '<button type="button" data-queue-action="apply-ranking">Apply completed ranking</button>'
        : "")
    );
  }

  renderIndividuals() {
    this.refs.individuals.innerHTML = "";
    this.refs.sideSheetIndividuals.classList.toggle(
      "queue-mode",
      this.individualReviewQueue.mode === "queue",
    );
    this.refs.sideSheetTabs.classList.toggle(
      "hidden",
      this.individualReviewQueue.mode === "queue",
    );
    this.refs.individualViewBrowse.classList.toggle(
      "is-active",
      this.individualReviewQueue.mode === "browse",
    );
    this.refs.individualViewQueue.classList.toggle(
      "is-active",
      this.individualReviewQueue.mode === "queue",
    );
    this.refs.individualQueueControls.classList.toggle(
      "hidden",
      this.individualReviewQueue.mode !== "queue",
    );
    this.refs.individualSearchControl.hidden = this.individualReviewQueue.mode === "queue";
    this.refs.individualResize.setAttribute(
      "aria-label",
      this.individualReviewQueue.mode === "queue"
        ? "Resize review controls and individual list"
        : "Resize individual and checked-fix lists",
    );
    if (!this.data) {
      return;
    }
    if (this.individualReviewQueue.mode === "queue") {
      this.renderIndividualReviewQueue();
      return;
    }
    const filteredIndividuals = this.getFilteredIndividuals();
    const totalIndividuals = this.data.individuals.length;
    this.refs.individualHead.textContent = filteredIndividuals.length === totalIndividuals
      ? `Individuals and coverage (${formatCount(totalIndividuals)})`
      : `Individuals and coverage (${formatCount(filteredIndividuals.length)} of ${formatCount(totalIndividuals)})`;
    if (!filteredIndividuals.length) {
      const empty = document.createElement("div");
      empty.className = "movement-empty";
      empty.textContent = "No individual IDs match the current search.";
      this.refs.individuals.appendChild(empty);
      return;
    }
    for (const individual of filteredIndividuals) {
      const stats = this.data.stats[individual];
      const coverage = this.data.coverageByIndividual[individual] || {};
      const isSelected = this.data.selectedIndividuals.has(individual);
      const card = document.createElement("div");
      const unresolvedCount = Number(stats.unresolvedSuspectedCount) || 0;
      const priorReview = this.getIndividualReviewState(individual).priorDecision || null;
      const priorDecision = String(priorReview?.review_decision || "");
      card.className = `movement-card interactive${unresolvedCount ? " has-unresolved-issues" : ""}`;
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
        statChip(`unresolved ${formatCount(unresolvedCount)}`),
        statChip(`confirmed ${formatCount(stats.confirmedCount)}`),
      );
      card.appendChild(statsRow);
      if (unresolvedCount) {
        const origins = Array.isArray(stats.unresolvedIssueOrigins) ? stats.unresolvedIssueOrigins : [];
        const notice = document.createElement("div");
        notice.className = "movement-fix-note";
        notice.textContent = `Unresolved issues: ${formatCount(unresolvedCount)}${origins.length ? ` • ${origins.join(", ")}` : ""}`;
        card.appendChild(notice);
      }
      if (priorDecision) {
        const badge = document.createElement("div");
        badge.className = `movement-prior-decision-badge ${reviewDecisionClass(priorDecision, priorReview?.needs_check)}`;
        badge.textContent = `Prior review: ${reviewDecisionLabel(priorDecision)}`;
        if (priorReview?.needs_check) badge.textContent += " • Needs check";
        card.appendChild(badge);
      }

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

  renderIndividualReviewQueue() {
    const position = this.getIndividualQueuePosition();
    const queue = this.individualReviewQueue;
    this.syncIndividualQueueRankingOptions();
    this.refs.individualHead.textContent = (
      `Individual review queue (${formatCount(position.ordered.length)})`
    );
    this.renderIndividualQueueRankingState();
    const activeNumber = position.activeIndex >= 0 ? position.activeIndex + 1 : 0;
    this.refs.individualQueueProgress.textContent = position.ordered.length
      ? (
        `Individual ${formatCount(activeNumber)} of ${formatCount(position.ordered.length)}`
        + ` • page ${formatCount(queue.pageIndex + 1)} of ${formatCount(position.pageCount)}`
        + ` • ${formatCount(position.group.length)} detailed track(s) in this group`
        + ` • ${formatCount(queue.stagedDecisions.size)} unsaved`
      )
      : "No individuals are available.";
    const editsLocked = !this.canPersistEdits();
    this.updateIndividualDecisionSaveButton(editsLocked);
    const activePageIndex = position.page.indexOf(queue.activeIndividual);
    const navDisabled = {
      "previous-page": queue.pageIndex <= 0,
      "next-page": queue.pageIndex >= position.pageCount - 1,
      "previous-group": queue.groupIndex <= 0,
      "next-group": queue.groupIndex >= position.groupCount - 1,
      "previous-individual": activePageIndex <= 0,
      "next-individual": activePageIndex < 0 || activePageIndex >= position.page.length - 1,
    };
    for (const button of this.refs.individualQueueControls.querySelectorAll("button[data-queue-nav]")) {
      button.disabled = queue.saving || Boolean(navDisabled[button.dataset.queueNav]);
    }
    for (const button of this.refs.individualQueueControls.querySelectorAll("button[data-queue-scope]")) {
      button.classList.toggle("is-active", button.dataset.queueScope === queue.mapScope);
      button.disabled = queue.saving || !position.ordered.length;
    }
    if (!position.page.length) {
      this.refs.individuals.innerHTML = '<div class="movement-empty">No individuals are available.</div>';
      return;
    }
    for (const individual of position.page) {
      const stats = this.data.stats[individual] || {};
      const reviewState = this.getIndividualReviewState(individual);
      const isActive = individual === queue.activeIndividual;
      const isEditingComment = queue.commentEditingIndividual === individual;
      const comment = queue.commentDrafts.has(individual)
        ? queue.commentDrafts.get(individual)
        : reviewState.comment || "";
      const card = document.createElement("div");
      const unresolvedCount = Number(stats.unresolvedSuspectedCount) || 0;
      const origins = Array.isArray(stats.unresolvedIssueOrigins) ? stats.unresolvedIssueOrigins : [];
      const priorDecision = String(reviewState.priorDecision?.review_decision || "");
      const priorNeedsCheck = reviewState.priorDecision?.needs_check === true;
      const priorLabel = priorDecision
        ? `Prior review: ${reviewDecisionLabel(priorDecision)}`
        : "";
      const priorClass = reviewDecisionClass(priorDecision, priorNeedsCheck);
      card.className = `movement-card queue-card interactive${isActive ? " queue-active" : ""}${unresolvedCount ? " has-unresolved-issues" : ""}`;
      const stateLabel = reviewState.reviewed
        ? reviewDecisionLabel(reviewState.reviewDecision)
        : priorDecision && priorDecision !== "ok"
          ? `Needs review—prior ${reviewDecisionLabel(priorDecision)}`
          : queue.skippedIndividuals.has(individual)
            ? "Skipped"
            : "Unreviewed";
      const stateClass = reviewState.reviewed
        ? ` ${reviewDecisionClass(reviewState.reviewDecision)}`
        : "";
      const selectedDecision = String(reviewState.reviewDecision || "");
      card.innerHTML = `
        <div class="movement-row">
          <div class="movement-row-left">
            <div class="movement-title">${escapeHtml(individual)}</div>
            <span class="movement-review-state${stateClass}">${escapeHtml(stateLabel)}${reviewState.needsCheck ? " • Needs check" : ""}${reviewState.staged ? " • unsaved" : ""}</span>
          </div>
          <div class="movement-queue-card-actions">
            <button type="button" data-queue-comment data-individual="${escapeHtml(individual)}">${comment ? "Note ✓" : "Note"}</button>
            <button type="button" data-queue-table data-individual="${escapeHtml(individual)}">Table</button>
          </div>
        </div>
        ${priorLabel ? `<div class="movement-prior-decision-badge ${priorClass}">${escapeHtml(priorLabel)}${priorNeedsCheck ? " • Needs check" : ""}</div>` : ""}
        <div class="movement-queue-card-meta">
          ${escapeHtml(this.data.speciesByIndividual[individual] || "")}
          ${this.data.speciesByIndividual[individual] ? " • " : ""}
          ${escapeHtml(formatCount(stats.rowCount))} fixes
          • ${escapeHtml(formatCount(unresolvedCount))} unresolved
          • ${escapeHtml(formatCount(stats.confirmedCount))} confirmed
        </div>
        ${unresolvedCount ? `<div class="movement-fix-note"><strong>Unresolved issues:</strong> ${escapeHtml(formatCount(unresolvedCount))}${origins.length ? ` • ${escapeHtml(origins.join(", "))}` : ""}</div>` : ""}
        ${isActive ? `
          <div class="movement-queue-card-actions">
            <span class="movement-review-choice">
              <button type="button" class="${selectedDecision === "ok" ? "is-selected" : ""}" data-review-decision="ok" data-individual="${escapeHtml(individual)}" aria-describedby="movement-review-help-ok"${editsLocked ? " disabled" : ""}>OK</button>
              <span class="movement-review-help" id="movement-review-help-ok" role="tooltip">The individual looks acceptable overall. Fix-level flags remain separate and may still require attention.</span>
            </span>
            <span class="movement-review-choice">
              <button type="button" class="${selectedDecision === "fix_keep" ? "is-selected" : ""}" data-review-decision="fix_keep" data-individual="${escapeHtml(individual)}" aria-describedby="movement-review-help-fix-keep"${editsLocked ? " disabled" : ""}>Fix &amp; Keep</button>
              <span class="movement-review-help" id="movement-review-help-fix-keep" role="tooltip">The individual has cleaning issues, but should remain after the affected fixes, bursts, or track sections are corrected.</span>
            </span>
            <span class="movement-review-choice">
              <button type="button" class="${selectedDecision === "remove" ? "is-selected" : ""}" data-review-decision="remove" data-individual="${escapeHtml(individual)}" aria-describedby="movement-review-help-remove"${editsLocked ? " disabled" : ""}>Remove</button>
              <span class="movement-review-help" id="movement-review-help-remove" role="tooltip">The individual should be removed from the cleaned dataset. This records a review decision only and does not exclude any data.</span>
            </span>
            <span class="movement-review-choice">
              <label class="movement-review-needs-check">
                <input type="checkbox" data-review-needs-check data-individual="${escapeHtml(individual)}" aria-describedby="movement-review-help-needs-check"${reviewState.needsCheck ? " checked" : ""}${editsLocked ? " disabled" : ""}>
                <span>Needs check</span>
              </label>
              <span class="movement-review-help" id="movement-review-help-needs-check" role="tooltip">Request another reviewer’s opinion in addition to your OK, Fix &amp; Keep, or Remove decision.</span>
            </span>
            <button type="button" data-queue-skip data-individual="${escapeHtml(individual)}">Skip</button>
          </div>
          ${this.queueFlagTargetControlsHtml(individual)}
        ` : ""}
        ${isEditingComment ? `
          <input
            class="movement-queue-card-comment"
            data-queue-comment-input
            data-individual="${escapeHtml(individual)}"
            value="${escapeHtml(comment)}"
            placeholder="Optional note for this individual"
          >
        ` : ""}
      `;
      card.addEventListener("click", event => {
        if (
          event.target.closest("button, input, label, select, textarea, [data-queue-burst-controls]")
          || individual === this.individualReviewQueue.activeIndividual
        ) {
          return;
        }
        void this.focusIndividualQueueItem(individual);
      });
      this.refs.individuals.appendChild(card);
    }
  }

  updateIndividualDecisionSaveButton(editsLocked = !this.canPersistEdits()) {
    const queue = this.individualReviewQueue;
    const activeDraft = queue.stagedDecisions.get(queue.activeIndividual);
    this.refs.individualQueueSave.disabled = (
      editsLocked
      || queue.saving
      || !activeDraft?.review_decision
      || !this.individualReviewDraftDiffers(queue.activeIndividual)
    );
    this.refs.individualQueueSave.textContent = queue.saving
      ? "Saving..."
      : "Save decision";
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
      card.className = `movement-card${fix.review.status === "suspected" ? " is-suspected" : ""}`;

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
      const cardActions = document.createElement("div");
      cardActions.className = "movement-row-left";
      if (fix.review.status === "suspected") {
        const dismissButton = document.createElement("button");
        dismissButton.type = "button";
        dismissButton.className = "movement-fix-dismiss";
        dismissButton.textContent = "Not suspicious";
        dismissButton.disabled = !this.canPersistEdits();
        dismissButton.addEventListener("click", () => this.openDismissModal([fix]));
        cardActions.appendChild(dismissButton);
      }
      cardActions.appendChild(removeButton);
      header.append(left, cardActions);
      card.appendChild(header);

      const meta = document.createElement("div");
      meta.className = "movement-fix-meta";
      const colorField = this.data.colorFieldByKey.get(this.refs.colorBy.value);
      const colorValue = formatColorValue(
        movementColorFieldValue(fix, colorField),
        colorField?.kind || "numeric",
      );
      meta.textContent = [
        `fix ${fix.fixKey}`,
        `color ${this.refs.colorBy.value}: ${colorValue}`,
        `step ${formatMaybeNumber(fix.attributes.step_length_m, "m")}`,
        `speed ${formatMaybeNumber(fix.attributes.speed_mps, "m/s")}`,
        `turn ${formatMaybeNumber(fix.attributes.turn_angle_deg, "°")}`,
      ].join(" • ");
      card.appendChild(meta);

      if (fix.review.issueType || fix.review.issueNote) {
        const note = document.createElement("div");
        note.className = "movement-fix-note";
        const issueTypes = reportIssueTypes(fix);
        note.textContent = `${issueTypes.join(", ") || "Issue"}: ${fix.review.issueNote || fix.review.ownerQuestion || ""}`;
        card.appendChild(note);
      }
      const unresolvedIssues = (fix.review.effectiveIssues || [])
        .filter(issue => issue.status === "suspected");
      if (unresolvedIssues.length) {
        const provenance = document.createElement("div");
        provenance.className = "movement-fix-note";
        provenance.innerHTML = `<strong>${escapeHtml(formatCount(unresolvedIssues.length))} unresolved suspicion(s)</strong><br>${unresolvedIssues.map(issue => {
          const origin = [
            issue.issueType || "Unspecified issue",
            issue.origin || "manual",
            issue.sourceAnalysisId ? `analysis ${issue.sourceAnalysisId}` : "",
            issue.stepId ? `step ${issue.stepId}` : "",
          ].filter(Boolean).join(" • ");
          return escapeHtml(origin);
        }).join("<br>")}`;
        card.appendChild(provenance);
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

  getVisibleDetailFixes() {
    if (!this.data) {
      return [];
    }
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    const visibleSetNames = this.getVisibleSetNames();
    return (this.data.fixes || []).filter(
      fix => visibleIndividuals.has(fix.individual) && visibleSetNames.has(fix.setName),
    );
  }

  getVisibleSegments() {
    if (!this.data) {
      return [];
    }
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    const visibleSetNames = this.getVisibleSetNames();
    return (this.data.segments || []).filter(
      segment => visibleIndividuals.has(segment.individual) && visibleSetNames.has(segment.setName),
    );
  }

  getVisibleAutoBursts({ requireOverlay = true } = {}) {
    if (!this.data || (requireOverlay && !this.refs.showBursts.checked)) {
      return [];
    }
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    const visibleSetNames = this.getVisibleSetNames();
    return (this.data.autoBursts || []).filter(
      burst => (
        visibleIndividuals.has(burst.individual)
        && visibleSetNames.has(burst.setName)
        && (
          !requireOverlay
          || !this.hiddenBurstIds.has(burst.burstId)
        )
      ),
    );
  }

  getFilteredTableSegments() {
    const filterText = String(this.refs.tableFilter.value || "").trim().toLowerCase();
    const segments = this.getVisibleSegments().slice();
    const filtered = filterText
      ? segments.filter(segment => {
        const haystack = [
          segment.individual,
          segment.setName,
          segment.segmentId,
          segment.status,
          segment.issueType,
          segment.startFixKey,
          segment.endFixKey,
          segment.issueNote,
          segment.ownerQuestion,
        ].join(" ").toLowerCase();
        return haystack.includes(filterText);
      })
      : segments;
    const direction = this.refs.tableSortDirection.dataset.direction === "desc" ? -1 : 1;
    const sortKey = this.refs.tableSort.value || "track_time";
    filtered.sort((left, right) => {
      if (sortKey === "status") {
        return direction * (
          String(left.status || "").localeCompare(String(right.status || ""))
          || left.startTimeMs - right.startTimeMs
        );
      }
      if (sortKey === "issue_type") {
        return direction * (
          String(left.issueType || "").localeCompare(String(right.issueType || ""))
          || left.startTimeMs - right.startTimeMs
        );
      }
      if (sortKey === "time_desc") {
        return direction * ((right.startTimeMs - left.startTimeMs) || left.segmentId.localeCompare(right.segmentId));
      }
      if (sortKey === "time_asc") {
        return direction * ((left.startTimeMs - right.startTimeMs) || left.segmentId.localeCompare(right.segmentId));
      }
      return direction * (
        left.individual.localeCompare(right.individual)
        || left.setName.localeCompare(right.setName)
        || left.startTimeMs - right.startTimeMs
        || left.segmentId.localeCompare(right.segmentId)
      );
    });
    return filtered;
  }

  getFilteredTableAutoBursts() {
    const filterText = String(this.refs.tableFilter.value || "").trim().toLowerCase();
    const bursts = this.getVisibleAutoBursts({ requireOverlay: false }).slice();
    const filtered = filterText
      ? bursts.filter(burst => {
        const haystack = [
          burst.individual,
          burst.setName,
          burst.burstId,
          burst.startFixKey,
          burst.endFixKey,
        ].join(" ").toLowerCase();
        return haystack.includes(filterText);
      })
      : bursts;
    const direction = this.refs.tableSortDirection.dataset.direction === "desc" ? -1 : 1;
    const sortKey = this.refs.tableSort.value || "track_time";
    filtered.sort((left, right) => {
      if (sortKey === "time_desc") {
        return direction * ((right.startTimeMs - left.startTimeMs) || left.burstId.localeCompare(right.burstId));
      }
      if (sortKey === "time_asc") {
        return direction * ((left.startTimeMs - right.startTimeMs) || left.burstId.localeCompare(right.burstId));
      }
      return direction * (
        left.individual.localeCompare(right.individual)
        || left.setName.localeCompare(right.setName)
        || left.startTimeMs - right.startTimeMs
        || left.burstId.localeCompare(right.burstId)
      );
    });
    return filtered;
  }

  buildTableRenderSignature(mode, totalRows) {
    const selectedIndividuals = this.getSelectedIndividuals().join("|");
    const direction = this.refs.tableSortDirection.dataset.direction || "asc";
    return [
      this.currentDatasetId,
      this.currentArtifact,
      mode,
      totalRows,
      this.refs.tableFilter.value || "",
      this.refs.tableSort.value || "track_time",
      direction,
      this.refs.showTrain.checked ? "train" : "",
      this.refs.showTest.checked ? "test" : "",
      this.refs.showBursts.checked ? "bursts" : "",
      this.getBurstGapSeconds(),
      selectedIndividuals,
      this.data?.detailState || "idle",
      this.data?.detailReturnedFixCount || 0,
    ].join("::");
  }

  getRenderedTableRows(rows, mode) {
    const totalRows = Array.isArray(rows) ? rows.length : 0;
    const signature = this.buildTableRenderSignature(mode, totalRows);
    if (this.tableRenderState.signature !== signature) {
      this.tableRenderState.signature = signature;
      this.tableRenderState.rowLimit = TABLE_INITIAL_ROW_LIMIT;
    }
    const renderedCount = mode === "fixes"
      ? Math.min(totalRows, this.tableRenderState.rowLimit)
      : totalRows;
    return {
      rows: rows.slice(0, renderedCount),
      renderedCount,
      totalRows,
      hasMore: renderedCount < totalRows,
    };
  }

  handleTableWrapScroll() {
    if (!this.data || this.refs.tableMode.value !== "fixes") {
      return;
    }
    const wrap = this.refs.tableWrap;
    if (!wrap) {
      return;
    }
    const thresholdPx = 240;
    const distanceFromBottom = wrap.scrollHeight - wrap.scrollTop - wrap.clientHeight;
    if (distanceFromBottom > thresholdPx) {
      return;
    }
    const rows = this.getFilteredTableFixRows();
    if (this.tableRenderState.rowLimit >= rows.length) {
      return;
    }
    this.tableRenderState.rowLimit = Math.min(rows.length, this.tableRenderState.rowLimit + TABLE_ROW_INCREMENT);
    this.renderTableSheet();
  }

  getFilteredTableFixRows() {
    const filterText = String(this.refs.tableFilter.value || "").trim().toLowerCase();
    const rows = this.getVisibleDetailFixes();
    const filtered = filterText
      ? rows.filter(fix => {
        const segmentText = (fix.segments || []).map(segment => (
          `${segment.segmentId} ${segment.issueType} ${segment.status}`
        )).join(" ").toLowerCase();
        const haystack = [
          fix.individual,
          fix.setName,
          fix.fixKey,
          fix.review?.status || "",
          fix.review?.issueType || "",
          segmentText,
        ].join(" ").toLowerCase();
        return haystack.includes(filterText);
      })
      : rows;
    const direction = this.refs.tableSortDirection.dataset.direction === "desc" ? -1 : 1;
    const sortKey = this.refs.tableSort.value || "track_time";
    filtered.sort((left, right) => {
      if (sortKey === "status") {
        return direction * (
          String(left.review?.status || "").localeCompare(String(right.review?.status || ""))
          || left.timeMs - right.timeMs
        );
      }
      if (sortKey === "issue_type") {
        return direction * (
          String(left.review?.issueType || "").localeCompare(String(right.review?.issueType || ""))
          || left.timeMs - right.timeMs
        );
      }
      if (sortKey === "time_desc") {
        return direction * ((right.timeMs - left.timeMs) || left.fixKey.localeCompare(right.fixKey));
      }
      if (sortKey === "time_asc") {
        return direction * ((left.timeMs - right.timeMs) || left.fixKey.localeCompare(right.fixKey));
      }
      return direction * (
        left.individual.localeCompare(right.individual)
        || left.setName.localeCompare(right.setName)
        || left.timeMs - right.timeMs
        || left.fixKey.localeCompare(right.fixKey)
      );
    });
    return filtered;
  }

  resolveSegmentSelection(anchorFixKey, targetFixKey) {
    if (!anchorFixKey || !targetFixKey) {
      return null;
    }
    const anchorPosition = this.data?.eligibleTrackPositionByFixKey?.get(anchorFixKey);
    const targetPosition = this.data?.eligibleTrackPositionByFixKey?.get(targetFixKey);
    if (!anchorPosition || !targetPosition || anchorPosition.trackKey !== targetPosition.trackKey) {
      return null;
    }
    const track = this.data?.eligibleFixesByTrack?.get(anchorPosition.trackKey) || [];
    const anchor = track[anchorPosition.index];
    const target = track[targetPosition.index];
    if (!anchor || !target) {
      return null;
    }
    const startIndex = Math.min(anchorPosition.index, targetPosition.index);
    const endIndex = Math.max(anchorPosition.index, targetPosition.index);
    const fixes = track.slice(startIndex, endIndex + 1);
    return {
      anchorFixKey,
      startFixKey: fixes[0]?.fixKey || anchorFixKey,
      endFixKey: fixes[fixes.length - 1]?.fixKey || targetFixKey,
      selectedFixKeys: new Set(fixes.map(fix => fix.fixKey)),
      fixes,
      individual: anchor.individual,
      setName: anchor.setName,
      trackKey: anchorPosition.trackKey,
      startIndex,
      endIndex,
      selectionMethod: this.tableSelection.selectionMethod || "",
    };
  }

  setTableSelection({
    anchorFixKey = "",
    focusFixKey = "",
    selectedFixKeys = [],
    contiguousRange = false,
    selectionMethod = "",
  } = {}) {
    const normalizedKeys = selectedFixKeys instanceof Set
      ? new Set(selectedFixKeys)
      : new Set(Array.isArray(selectedFixKeys) ? selectedFixKeys : []);
    const anchor = contiguousRange
      ? anchorFixKey
      : normalizedKeys.has(anchorFixKey) ? anchorFixKey : (normalizedKeys.size ? [...normalizedKeys][0] : "");
    const focus = contiguousRange
      ? focusFixKey || anchor
      : normalizedKeys.has(focusFixKey) ? focusFixKey : (normalizedKeys.has(anchor) ? anchor : "");
    this.tableSelection = {
      anchorFixKey: anchor,
      focusFixKey: focus,
      selectedFixKeys: normalizedKeys,
      contiguousRange: Boolean(contiguousRange && anchor && focus),
      selectionMethod: String(selectionMethod || ""),
    };
  }

  applyMapRangeEndpoint(fixKey) {
    if (!this.data?.eligibleTrackPositionByFixKey?.has(fixKey)) {
      this.setStatus("That fix is not part of the current analytical track.", true);
      return;
    }
    if (this.flagTargetKind !== "segment") {
      this.resetManualFlagTarget();
      this.data.selectedFixKeys = new Set();
    }
    if (!this.mapRangeAwaitingEnd || !this.tableSelection.anchorFixKey) {
      this.setTableSelection({
        anchorFixKey: fixKey,
        focusFixKey: fixKey,
        contiguousRange: true,
        selectionMethod: "map_double_click",
      });
      this.flagTargetKind = "segment";
      this.mapRangeAwaitingEnd = true;
      this.setStatus("Range start selected. Double-click the end fix on the same track.");
      return;
    }
    const selection = this.resolveSegmentSelection(this.tableSelection.anchorFixKey, fixKey);
    if (!selection) {
      this.setTableSelection({
        anchorFixKey: fixKey,
        focusFixKey: fixKey,
        contiguousRange: true,
        selectionMethod: "map_double_click",
      });
      this.flagTargetKind = "segment";
      this.mapRangeAwaitingEnd = true;
      this.setStatus("The track changed, so this fix is now the new range start.", true);
      return;
    }
    this.setTableSelection({
      anchorFixKey: this.tableSelection.anchorFixKey,
      focusFixKey: fixKey,
      contiguousRange: true,
      selectionMethod: "map_double_click",
    });
    this.flagTargetKind = "segment";
    this.mapRangeAwaitingEnd = false;
    this.setStatus(`Selected ${formatCount(selection.fixes.length)} fixes as a track segment. Use “Flag selected segment” or double-click another start.`);
  }

  applyTableSelectionInteraction(fixKey, { additive = false, range = false } = {}) {
    if (!this.data?.fixByKey?.has(fixKey)) {
      return;
    }
    if (range && this.tableSelection.anchorFixKey) {
      const selection = this.resolveSegmentSelection(this.tableSelection.anchorFixKey, fixKey);
      if (!selection) {
        this.setStatus("Segment ranges must stay within one visible track.", true);
        return;
      }
      this.setTableSelection({
        anchorFixKey: selection.anchorFixKey,
        focusFixKey: fixKey,
        contiguousRange: true,
        selectionMethod: "table_shift_click",
      });
      return;
    }
    if (additive) {
      const nextKeys = new Set(this.tableSelection.selectedFixKeys || []);
      if (nextKeys.has(fixKey)) {
        nextKeys.delete(fixKey);
      } else {
        nextKeys.add(fixKey);
      }
      this.setTableSelection({
        anchorFixKey: this.tableSelection.anchorFixKey || fixKey,
        focusFixKey: fixKey,
        selectedFixKeys: nextKeys,
      });
      return;
    }
    this.setTableSelection({
      anchorFixKey: fixKey,
      focusFixKey: fixKey,
      selectedFixKeys: [fixKey],
    });
  }

  clearTableSelection() {
    this.setTableSelection();
    this.mapRangeAwaitingEnd = false;
    if (this.flagTargetKind === "segment") {
      this.flagTargetKind = this.getActiveThresholdMatchKeys().size
        ? "filter"
        : this.data?.selectedFixKeys?.size ? "fixes" : "none";
    }
    this.renderTableSheet();
    this.renderLayers();
    this.updateActionButtons();
  }

  getCurrentSegmentSelection() {
    if (!this.tableSelection.anchorFixKey || !this.tableSelection.focusFixKey) {
      return null;
    }
    const selected = this.resolveSegmentSelection(
      this.tableSelection.anchorFixKey,
      this.tableSelection.focusFixKey,
    );
    if (!selected) {
      return null;
    }
    if (!this.tableSelection.contiguousRange) {
      if (selected.selectedFixKeys.size !== this.tableSelection.selectedFixKeys.size) {
        return null;
      }
      for (const fixKey of selected.selectedFixKeys) {
        if (!this.tableSelection.selectedFixKeys.has(fixKey)) {
          return null;
        }
      }
    }
    return selected;
  }

  hasTableSelection() {
    return Boolean(
      this.tableSelection.contiguousRange
        ? this.tableSelection.anchorFixKey
        : this.tableSelection.selectedFixKeys.size,
    );
  }

  isFixInTableSelection(fixKey, selection = this.getCurrentSegmentSelection()) {
    if (selection && this.tableSelection.contiguousRange) {
      const position = this.data?.eligibleTrackPositionByFixKey?.get(fixKey);
      return Boolean(
        position
        && position.trackKey === selection.trackKey
        && position.index >= selection.startIndex
        && position.index <= selection.endIndex,
      );
    }
    return this.tableSelection.selectedFixKeys.has(fixKey);
  }

  getVisibleTableSelectionFixes() {
    const visibleSetNames = this.getVisibleSetNames();
    const visibleIndividuals = new Set(this.getSelectedIndividuals());
    const selection = this.getCurrentSegmentSelection();
    const fixes = selection && this.tableSelection.contiguousRange
      ? selection.fixes
      : [...(this.tableSelection.selectedFixKeys || new Set())]
        .map(fixKey => this.data?.fixByKey?.get(fixKey));
    return fixes
      .filter(fix => Boolean(fix) && visibleIndividuals.has(fix.individual) && visibleSetNames.has(fix.setName));
  }

  zoomToPath(path) {
    if (!this.map || !Array.isArray(path) || !path.length) {
      return;
    }
    const bounds = buildWindowBounds(path.map((position, index) => ({
      fixKey: `path_${index}`,
      position,
    })), { tight: false });
    if (!bounds) {
      return;
    }
    this.map.fitBounds(bounds, { padding: 44, duration: 0, maxZoom: 15 });
  }

  handleTableWrapClick(event) {
    const fixCheckbox = event.target.closest("input[data-table-check-fix]");
    if (fixCheckbox) {
      this.setCheckedFixIncluded(
        fixCheckbox.dataset.tableCheckFix || "",
        fixCheckbox.checked,
        "table_check",
      );
      return;
    }
    const burstVisibilityCheckbox = event.target.closest("input[data-table-burst-visible]");
    if (burstVisibilityCheckbox) {
      this.setBurstVisible(
        burstVisibilityCheckbox.dataset.tableBurstVisible || "",
        burstVisibilityCheckbox.checked,
      );
      return;
    }
    const burstCheckbox = event.target.closest("input[data-table-check-burst]");
    if (burstCheckbox) {
      this.setBurstFlagTargetIncluded(
        burstCheckbox.dataset.tableCheckBurst || "",
        burstCheckbox.checked,
        { selectionMethod: "table_burst_check" },
      );
      return;
    }
    const actionButton = event.target.closest("button[data-action]");
    if (actionButton) {
      const action = actionButton.dataset.action || "";
      if (action === "zoom-fix") {
        const fix = this.data?.fixByKey.get(actionButton.dataset.fixKey || "");
        if (fix) {
          this.zoomToPath([fix.position]);
        }
      } else if (action === "zoom-segment") {
        const segment = this.data?.segmentById?.get(actionButton.dataset.segmentId || "");
        if (segment) {
          this.zoomToPath(segment.path);
        }
      } else if (action === "zoom-auto-burst") {
        const burst = this.data?.autoBurstById?.get(actionButton.dataset.burstId || "");
        if (burst) {
          this.zoomToPath(burst.path);
        }
      }
      return;
    }
    const row = event.target.closest("tr[data-fix-key], tr[data-segment-id], tr[data-burst-id]");
    if (!row) {
      return;
    }
    if (row.dataset.segmentId && this.refs.tableMode.value === "segments") {
      const segment = this.data?.segmentById?.get(row.dataset.segmentId || "");
      if (segment) {
        this.zoomToPath(segment.path);
      }
      return;
    }
    if (row.dataset.burstId && this.refs.tableMode.value === "auto_bursts") {
      const burstId = row.dataset.burstId || "";
      this.setBurstVisible(burstId, this.hiddenBurstIds.has(burstId));
      return;
    }
    if (this.refs.tableMode.value !== "fixes") {
      return;
    }
    const fixKey = row.dataset.fixKey || "";
    if (!fixKey) {
      return;
    }
    if (this.flagTargetKind !== "segment") {
      this.resetManualFlagTarget();
      this.data.selectedFixKeys = new Set();
    }
    this.flagTargetKind = "segment";
    this.applyTableSelectionInteraction(fixKey, {
      additive: event.metaKey || event.ctrlKey,
      range: event.shiftKey,
    });
    this.renderTableSheet();
    this.renderLayers();
    this.updateActionButtons();
  }

  renderTableSheet() {
    if (!this.refs.tableWrap || !this.refs.tableMeta) {
      return;
    }
    if (!this.data) {
      this.refs.tableMeta.textContent = "Load a study to inspect fix rows and flagged segments.";
      this.refs.tableWrap.innerHTML = '<div class="movement-table-empty">No table data yet.</div>';
      return;
    }
    const mode = this.refs.tableMode.value || "fixes";
    const flagCheckboxDisabled = this.canPersistEdits() ? "" : " disabled";
    const hasDetail = this.hasLoadedDetailSelection() || this.data.overviewHasAllFixes;
    const rows = mode === "fixes" ? this.getFilteredTableFixRows() : [];
    const selection = this.getCurrentSegmentSelection();
    this.refs.segmentClear.disabled = !this.hasTableSelection();
    const segmentActionDisabled = (
      !this.canPersistEdits()
      || mode !== "fixes"
      || !hasDetail
      || !selection
      || selection.fixes.length < 2
    );
    this.refs.segmentConfirmed.disabled = segmentActionDisabled || !this.canConfirmFixes(selection?.fixes || []);

    if (mode === "segments") {
      const segments = this.getFilteredTableSegments();
      this.refs.tableMeta.textContent = `${formatCount(segments.length)} flagged segments in the current visible scope. Click a row to zoom to the full segment extent.`;
      if (!segments.length) {
        this.refs.tableWrap.innerHTML = '<div class="movement-table-empty">No flagged segments are visible for the current selection.</div>';
        return;
      }
      this.refs.tableWrap.innerHTML = `
        <table class="movement-table">
          <thead>
            <tr>
              <th>Individual</th>
              <th>Track</th>
              <th>Status</th>
              <th>Issue type</th>
              <th>Fixes</th>
              <th>Start</th>
              <th>End</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            ${segments.map(segment => `
              <tr class="is-segment-row" data-segment-id="${escapeHtml(segment.segmentId)}">
                <td>${escapeHtml(segment.individual)}</td>
                <td>${escapeHtml(segment.setName)}</td>
                <td>${escapeHtml(segment.status || "unreviewed")}</td>
                <td>${escapeHtml(segment.issueType || "Unspecified issue")}</td>
                <td class="movement-table-cell-mono">${escapeHtml(String(segment.fixCount))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatTimestamp(segment.startTimeMs))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatTimestamp(segment.endTimeMs))}</td>
                <td class="movement-table-cell-actions"><button type="button" data-action="zoom-segment" data-segment-id="${escapeHtml(segment.segmentId)}">Zoom</button></td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
      return;
    }

    if (mode === "auto_bursts") {
      const bursts = this.getFilteredTableAutoBursts();
      const overlayNote = this.refs.showBursts.checked
        ? ""
        : " The burst-color overlay is hidden, but visibility checks still control ordinary points and tracks.";
      this.refs.tableMeta.textContent = `${formatCount(bursts.length)} automatic bursts in the current visible scope at ${this.burstGapLabel() || `${formatCount(this.getBurstGapSeconds())} s`}. Bursts start visible; uncheck Visible to hide their ordinary points and track steps. Flag remains independent. Zoom is always explicit.${overlayNote}`;
      if (!bursts.length) {
        this.refs.tableWrap.innerHTML = '<div class="movement-table-empty">No automatic bursts are visible for the current selection.</div>';
        return;
      }
      this.refs.tableWrap.innerHTML = `
        <table class="movement-table">
          <thead>
            <tr>
              <th>Visible</th>
              <th>Flag</th>
              <th>Color</th>
              <th>Individual</th>
              <th>Track</th>
              <th>Burst</th>
              <th>Fixes</th>
              <th>Start</th>
              <th>End</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            ${bursts.map(burst => `
              <tr class="is-auto-burst-row" data-burst-id="${escapeHtml(burst.burstId)}">
                <td><input type="checkbox" data-table-burst-visible="${escapeHtml(burst.burstId)}"${this.hiddenBurstIds.has(burst.burstId) ? "" : " checked"}></td>
                <td><input type="checkbox" data-table-check-burst="${escapeHtml(burst.burstId)}"${this.flagTargetKind === "bursts" && this.manualFlagTarget.burstIds.has(burst.burstId) ? " checked" : ""}${flagCheckboxDisabled}></td>
                <td><span class="movement-burst-swatch" style="background: ${escapeHtml(rgbaCss(burstPathColor(this.data?.individualPalette, burst, 215)))}"></span></td>
                <td>${escapeHtml(burst.individual)}</td>
                <td>${escapeHtml(burst.setName)}</td>
                <td class="movement-table-cell-mono">${escapeHtml(String(burst.burstIdx))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(String(burst.fixCount))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatTimestamp(burst.startTimeMs))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatTimestamp(burst.endTimeMs))}</td>
                <td class="movement-table-cell-actions">
                  <button type="button" data-action="zoom-auto-burst" data-burst-id="${escapeHtml(burst.burstId)}">Zoom</button>
                </td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
      return;
    }

    if (!rows.length && !hasDetail) {
      const message = this.data.detailState === "error"
        ? "Visible fix rows could not be loaded for this selection."
        : "Loading visible fix rows for the current selection...";
      this.refs.tableMeta.textContent = "Fix rows depend on the visible-scope detail load.";
      this.refs.tableWrap.innerHTML = `<div class="movement-table-empty">${escapeHtml(message)}</div>`;
      return;
    }

    const selectedRowKeys = this.tableSelection.selectedFixKeys || new Set();
    const anchorFixKey = this.tableSelection.anchorFixKey || "";
    const renderedTable = this.getRenderedTableRows(rows, mode);
    const renderedRows = renderedTable.rows;
    const truncationNote = this.data.detailTruncated
      ? ` Visible scope is truncated to ${formatCount(this.data.detailReturnedFixCount)} of ${formatCount(this.data.detailMatchingFixCount)} rows because of the ${formatCount(this.data.detailLimit)}-fix cap.`
      : "";
    const detailSourceNote = this.data.detailState === "error" && rows.length
        ? " Showing overview rows because editable detail failed to load; segment actions stay disabled."
        : !hasDetail && rows.length
          ? " Showing overview rows while editable detail finishes loading; segment actions stay disabled until then."
        : "";
    let selectionSummary = "Click a row to set a segment anchor, then Shift-click another row on the same track to select the full inclusive range.";
    if (anchorFixKey) {
      selectionSummary = `Anchor set on ${anchorFixKey}. Shift-click another row on the same track to create a segment range.`;
    }
    const selectedCount = selection?.fixes?.length || selectedRowKeys.size;
    if (selectedCount === 1) {
      selectionSummary = `${anchorFixKey} selected. Shift-click another row on the same track to create a segment range.`;
    }
    if (selectedCount > 1) {
      selectionSummary = `${formatCount(selectedCount)} table rows selected. Shift-click within one track to turn the selection into a contiguous segment range.`;
    }
    if (selection && selection.fixes.length >= 2) {
      selectionSummary = `${selection.individual} • ${selection.setName} • ${formatCount(selection.fixes.length)} fixes from ${formatTimestamp(selection.fixes[0].timeMs)} to ${formatTimestamp(selection.fixes[selection.fixes.length - 1].timeMs)}`;
    }
    const renderNote = renderedTable.hasMore
      ? ` Rendering ${formatCount(renderedTable.renderedCount)} of ${formatCount(renderedTable.totalRows)} rows; scroll to load more.`
      : "";
    this.refs.tableMeta.textContent = `${formatCount(rows.length)} visible fix rows. ${selectionSummary}${truncationNote}${detailSourceNote}${renderNote}`;
    if (!rows.length) {
      this.refs.tableWrap.innerHTML = '<div class="movement-table-empty">No fix rows match the current table filters.</div>';
      return;
    }
    this.refs.tableWrap.innerHTML = `
      <table class="movement-table">
        <thead>
          <tr>
            <th>Flag</th>
            <th>Individual</th>
            <th>Track</th>
            <th>Timestamp</th>
            <th>Status</th>
            <th>Segment</th>
            <th>Issue</th>
            <th>Step (m)</th>
            <th>Speed (m/s)</th>
            <th>Turn (°)</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          ${renderedRows.map(fix => {
            const issueType = fix.review?.issueType || "Unreviewed";
            const segmentLabel = (fix.segments || []).length
              ? `${fix.segments[0].status || "flagged"} • ${fix.segments[0].issueType || "segment"}`
              : "";
            const rowClasses = [
              anchorFixKey === fix.fixKey ? "is-anchor" : "",
              this.isFixInTableSelection(fix.fixKey, selection) ? "is-selected-range" : "",
              this.data.selectedFixKeys.has(fix.fixKey) ? "is-checked-fix" : "",
            ].filter(Boolean).join(" ");
            return `
              <tr class="${rowClasses}" data-fix-key="${escapeHtml(fix.fixKey)}">
                <td><input type="checkbox" data-table-check-fix="${escapeHtml(fix.fixKey)}"${this.data.selectedFixKeys.has(fix.fixKey) ? " checked" : ""}${flagCheckboxDisabled}></td>
                <td>${escapeHtml(fix.individual)}</td>
                <td>${escapeHtml(fix.setName)}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatTimestamp(fix.timeMs))}</td>
                <td>${escapeHtml(fix.review?.status || "unreviewed")}</td>
                <td>${escapeHtml(segmentLabel || "—")}</td>
                <td>${escapeHtml(issueType)}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatMaybeNumber(fix.attributes?.step_length_m, "m"))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatMaybeNumber(fix.attributes?.speed_mps, "m/s"))}</td>
                <td class="movement-table-cell-mono">${escapeHtml(formatMaybeNumber(fix.attributes?.turn_angle_deg, "°"))}</td>
                <td class="movement-table-cell-actions"><button type="button" data-action="zoom-fix" data-fix-key="${escapeHtml(fix.fixKey)}">Zoom</button></td>
              </tr>
            `;
          }).join("")}
          ${renderedTable.hasMore ? `
            <tr class="movement-table-more-row">
              <td colspan="11" class="movement-table-more-cell">Scroll to load more rows.</td>
            </tr>
          ` : ""}
        </tbody>
      </table>
    `;
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
    if (!field) {
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
    if (field.key === INDIVIDUAL_COLOR_FIELD_KEY) {
      const visibleIndividuals = this.getSelectedIndividuals();
      const shownIndividuals = visibleIndividuals.slice(0, INDIVIDUAL_LEGEND_MAX_ITEMS);
      const remainingCount = Math.max(0, visibleIndividuals.length - shownIndividuals.length);
      const note = visibleIndividuals.length
        ? remainingCount > 0
          ? `Points use each individual's track color. Showing ${formatCount(shownIndividuals.length)} of ${formatCount(visibleIndividuals.length)} visible individuals.`
          : `Points use each individual's track color across ${formatCount(visibleIndividuals.length)} visible individuals.`
        : "Points use each individual's track color.";
      body = `
        <div class="movement-legend-note">${escapeHtml(note)}</div>
        <div class="movement-legend-items">
          ${shownIndividuals.map(individual => legendItem(
            individual,
            [...(this.data.individualPalette[individual] || [124, 210, 255]), POINT_ALPHA],
          )).join("")}
        </div>
      `;
    } else {
      const style = this.data.colorStyles.get(field.key);
      if (!style) {
        legendEl.innerHTML = "";
        legendEl.classList.add("hidden");
        return;
      }
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
    }

    const sourceFlaggedCount = (this.data.fixes || []).filter(isSourceOnlyFlaggedFix).length;
    const sourceFlagNote = sourceFlaggedCount
      ? `<div class="movement-legend-note">Thin faded sections were flagged in the source data (${escapeHtml(formatCount(sourceFlaggedCount))} loaded fixes); they remain analytically included until confirmed in Vibecleaning.</div>`
      : "";
    legendEl.innerHTML = `${header}${body}${sourceFlagNote}`;
    legendEl.classList.remove("hidden");
  }

  async rebuildMap(forceStyleReload) {
    if (!this.assetsLoaded || !window.maplibregl || !window.deck) {
      return;
    }
    const preset = BASEMAP_PRESETS[this.refs.basemap.value] || BASEMAP_PRESETS.Blank;
    const style = preset.style;
    this.updateMapAttribution();
    if (!this.map || forceStyleReload) {
      const currentView = this.map
        ? {
          center: [this.map.getCenter().lng, this.map.getCenter().lat],
          zoom: this.map.getZoom(),
        }
        : {
          center: this.data
            ? [this.data.initialView.longitude, this.data.initialView.latitude]
            : [0, 20],
          zoom: this.data ? this.data.initialView.zoom : 1.3,
        };
      this.destroyMapInstance();
      this.createMapInstance({ style, ...currentView });
      return;
    }
    this.renderLayers();
  }

  createMapInstance({ style, center, zoom }) {
    this.mapErrorMessage = "";
    this.mapLoaded = false;
    this.map = new maplibregl.Map({
      container: this.refs.map,
      style,
      center,
      zoom,
      attributionControl: false,
    });
    this.map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    this.map.addControl(new maplibregl.ScaleControl({ unit: "metric", maxWidth: 120 }), "top-right");
    this.map.doubleClickZoom?.disable?.();
    this.map.on("load", () => {
      this.mapLoaded = true;
      this.renderLayers();
      this.updateActionButtons();
    });
    this.map.on("error", (event) => {
      const message = event?.error?.message || "Map request failed";
      if (message === this.mapErrorMessage) {
        return;
      }
      this.mapErrorMessage = message;
      this.setStatus(`Map warning: ${message}`, true);
    });
    this.ensureOverlayAttached();
    this.map.on("click", event => this.handleMapClick(event));
    this.map.on("dblclick", event => this.handleMapDoubleClick(event));
    this.map.on("contextmenu", event => this.handleMapContextMenu(event));
  }

  destroyMapInstance() {
    this.mapLoaded = false;
    this.activeFixPopup = null;
    if (this.pendingMapSingleClickTimer !== null) {
      window.clearTimeout(this.pendingMapSingleClickTimer);
      this.pendingMapSingleClickTimer = null;
    }
    if (this.overlay) {
      try {
        this.overlay.finalize();
      } catch {}
    }
    this.overlay = null;
    if (this.map) {
      try {
        this.map.remove();
      } catch {}
    }
    this.map = null;
  }

  ensureOverlayAttached() {
    if (!this.map || !window.deck) {
      return;
    }
    if (!this.overlay) {
      this.overlay = new deck.MapboxOverlay({ interleaved: false, layers: [] });
    }
    try {
      this.map.addControl(this.overlay);
    } catch {}
  }

  detachOverlay() {
    if (!this.map || !this.overlay) {
      return;
    }
    try {
      this.map.removeControl(this.overlay);
    } catch {}
  }

  waitForStyleReload() {
    if (!this.map) {
      return;
    }
    let finished = false;
    const finishReload = () => {
      if (!this.map || finished) {
        return;
      }
      finished = true;
      this.map.off("styledata", maybeFinishReload);
      this.mapLoaded = true;
      this.ensureOverlayAttached();
      this.renderLayers();
    };
    const maybeFinishReload = () => {
      if (this.map?.isStyleLoaded?.()) {
        finishReload();
      }
    };
    this.map.once("style.load", finishReload);
    this.map.on("styledata", maybeFinishReload);
  }

  updateMapAttribution() {
    if (!this.refs?.mapAttribution) {
      return;
    }
    const preset = BASEMAP_PRESETS[this.refs.basemap.value || "Blank"] || BASEMAP_PRESETS.Blank;
    if (preset.attributionHtml) {
      this.refs.mapAttribution.innerHTML = preset.attributionHtml;
      this.refs.mapAttribution.classList.remove("hidden");
      return;
    }
    this.refs.mapAttribution.innerHTML = "";
    this.refs.mapAttribution.classList.add("hidden");
  }

  scheduleTemporalFocusRender() {
    if (this.temporalFocusRenderFrame !== null) return;
    this.temporalFocusRenderFrame = window.requestAnimationFrame(() => {
      this.temporalFocusRenderFrame = null;
      this.renderLayers({ temporalOnly: true });
    });
  }

  getVisibleMovementPoints(visibleIndividuals, visibleSetNames) {
    const cacheKey = [
      [...visibleIndividuals].sort().join("|"),
      [...visibleSetNames].sort().join("|"),
    ].join("::");
    const cache = this.data.visiblePointCache;
    if (cache.has(cacheKey)) return cache.get(cacheKey);
    const points = [];
    for (const [trackKey, fixes] of this.data.eligibleFixesByTrack || []) {
      const first = fixes[0];
      if (
        !first
        || !visibleIndividuals.has(first.individual)
        || !visibleSetNames.has(first.setName)
      ) {
        continue;
      }
      for (const fix of fixes) {
        points.push({
          fix,
          fixKey: fix.fixKey,
          individual: fix.individual,
          setName: fix.setName,
          trackKey,
          position: fix.position,
        });
      }
    }
    cache.set(cacheKey, points);
    return points;
  }

  getVisibleTrackSteps(visibleIndividuals, visibleSetNames) {
    const cacheKey = [
      [...visibleIndividuals].sort().join("|"),
      [...visibleSetNames].sort().join("|"),
    ].join("::");
    const cache = this.data.visibleTrackStepCache;
    if (cache.has(cacheKey)) return cache.get(cacheKey);
    const steps = (this.data.trackStepSegments || []).filter(step => (
      visibleIndividuals.has(step.individual)
      && visibleSetNames.has(step.setName)
    ));
    cache.set(cacheKey, steps);
    return steps;
  }

  buildTemporalFocalData(visibleIndividuals, visibleSetNames) {
    const binary = this.data?.binaryMovement;
    if (binary) {
      const points = [];
      for (const [code, [start, end]] of binary.individualRanges || []) {
        const individual = String(binary.header.individuals?.[code] || "");
        if (!visibleIndividuals.has(individual)) continue;
        let low = start;
        let high = end;
        while (low < high) {
          const middle = Math.floor((low + high) / 2);
          if (Number(binary.arrays.time_ms[middle]) < this.currentTimeMs) low = middle + 1;
          else high = middle;
        }
        const candidates = [Math.max(start, low - 1), Math.min(end - 1, low)];
        const focalIndex = candidates.reduce((best, candidate) => (
          Math.abs(Number(binary.arrays.time_ms[candidate]) - this.currentTimeMs)
            < Math.abs(Number(binary.arrays.time_ms[best]) - this.currentTimeMs)
            ? candidate : best
        ), candidates[0]);
        for (let index = Math.max(start, focalIndex - 1); index <= Math.min(end - 1, focalIndex + 1); index += 1) {
          if (Number(binary.arrays.review_status[index]) === 2) continue;
          const burstId = `${individual}:${binary.header.implicit_set || "train"}:source_${Number(binary.arrays.burst_values[index])}`;
          if (this.hiddenBurstIds.has(burstId)) continue;
          const fix = this.binaryFixAt(index, { remember: false });
          if (!fix) continue;
          points.push({
            fixKey: fix.fixKey,
            individual,
            position: fix.position,
            color: this.binaryColorForIndex(binary, index, this.getCurrentColorField()),
            focal: index === focalIndex,
          });
        }
      }
      return { points };
    }
    const points = [];
    for (const fixes of this.data.eligibleFixesByTrack?.values() || []) {
      const first = fixes[0];
      if (
        !first
        || !visibleIndividuals.has(first.individual)
        || !visibleSetNames.has(first.setName)
      ) {
        continue;
      }
      const focusIndex = nearestTrackFixIndex(fixes, this.currentTimeMs);
      if (focusIndex < 0) continue;
      const startIndex = Math.max(0, focusIndex - 1);
      const endIndex = Math.min(fixes.length - 1, focusIndex + 1);
      for (let index = startIndex; index <= endIndex; index += 1) {
        const fix = fixes[index];
        points.push({
          fixKey: fix.fixKey,
          individual: fix.individual,
          position: fix.position,
          color: this.colorForFix(fix),
          focal: index === focusIndex,
        });
      }
    }
    return { points };
  }

  binaryColorForIndex(binary, index, field) {
    const arrays = binary.arrays;
    const individual = String(binary.header.individuals?.[Number(arrays.individual_codes[index])] || "");
    if (!field || field.key === INDIVIDUAL_COLOR_FIELD_KEY) {
      return [...(this.data.individualPalette[individual] || [124, 210, 255]), POINT_ALPHA];
    }
    if (field.kind === "boolean") {
      const value = field.key === "is_outlier" ? Number(arrays.is_outlier[index]) : -1;
      if (value === 1) return [246, 92, 110, POINT_ALPHA];
      if (value === 0) return [96, 201, 170, POINT_ALPHA];
      return [120, 136, 153, 120];
    }
    if (field.kind === "numeric") {
      const sourceKey = field.key === GPS_SPIKE_COLOR_FIELD_KEY ? "step_length_m" : field.key;
      const values = arrays[sourceKey];
      const range = this.data.colorStyles.get(field.key)?.range
        || this.data.colorStyles.get(sourceKey)?.range
        || { min: 0, max: 1 };
      return interpolateNumericColor(values ? Number(values[index]) : null, range, POINT_ALPHA);
    }
    return [120, 136, 153, 150];
  }

  buildBinaryRenderAttributes(visibleIndividuals, showPoints) {
    const binary = this.data?.binaryMovement;
    if (!binary) return null;
    const arrays = binary.arrays;
    const rowCount = Number(binary.header.row_count) || 0;
    const lineCount = Number(binary.header.line_count) || 0;
    const field = this.getCurrentColorField();
    const selectedCodes = new Set();
    (binary.header.individuals || []).forEach((individual, code) => {
      if (visibleIndividuals.has(String(individual))) selectedCodes.add(code);
    });
    const hiddenBurstIds = this.hiddenBurstIds;
    const activeQueueIndividual = this.queueActiveIndividual();
    const pointColors = new Uint8Array(rowCount * 4);
    const pointFilter = new Uint8Array(rowCount);
    const suspectedFilter = new Uint8Array(rowCount);
    const confirmedFilter = new Uint8Array(rowCount);
    const thresholdFilter = new Uint8Array(rowCount);
    const thresholdActive = this.thresholdState.fieldKey === field?.key;
    const thresholdValue = thresholdActive ? finiteOrNull(this.thresholdState.value) : null;
    const thresholdLevels = new Set(thresholdActive ? this.thresholdState.selectedLevels || [] : []);
    for (let index = 0; index < rowCount; index += 1) {
      const code = Number(arrays.individual_codes[index]);
      const individual = String(binary.header.individuals?.[code] || "");
      const burstId = `${individual}:${binary.header.implicit_set || "train"}:source_${Number(arrays.burst_values[index])}`;
      const selected = selectedCodes.has(code) && !hiddenBurstIds.has(burstId);
      const status = Number(arrays.review_status[index]);
      const visible = selected && status !== 2;
      pointFilter[index] = showPoints && visible ? 1 : 0;
      suspectedFilter[index] = showPoints && visible && status === 1 ? 1 : 0;
      confirmedFilter[index] = showPoints && selected && status === 2 && this.refs.showConfirmed.checked ? 1 : 0;
      if (showPoints && visible && thresholdActive) {
        if (field.kind === "boolean" && field.key === "is_outlier") {
          thresholdFilter[index] = thresholdLevels.has(Number(arrays.is_outlier[index]) ? "True" : "False") ? 1 : 0;
        } else if (field.kind === "numeric" && thresholdValue !== null) {
          const sourceKey = field.key === GPS_SPIKE_COLOR_FIELD_KEY ? "step_length_m" : field.key;
          const value = Number(arrays[sourceKey]?.[index]);
          const validGpsTurn = field.key !== GPS_SPIKE_COLOR_FIELD_KEY
            || (Number.isFinite(Number(arrays.turn_angle_deg[index])) && Math.abs(Number(arrays.turn_angle_deg[index])) >= this.gpsSpikeTurnAngleDeg);
          const matches = this.thresholdState.reverse === true ? value < thresholdValue : value > thresholdValue;
          thresholdFilter[index] = Number.isFinite(value) && validGpsTurn && matches ? 1 : 0;
        }
      }
      const color = this.binaryColorForIndex(binary, index, field);
      const opacity = activeQueueIndividual && individual !== activeQueueIndividual ? 0.25 : 1;
      const offset = index * 4;
      pointColors[offset] = color[0];
      pointColors[offset + 1] = color[1];
      pointColors[offset + 2] = color[2];
      pointColors[offset + 3] = Math.round(color[3] * opacity);
    }
    const lineColors = new Uint8Array(lineCount * 4);
    const lineFilter = new Uint8Array(lineCount);
    const burstFilter = new Uint8Array(lineCount);
    for (let index = 0; index < lineCount; index += 1) {
      const sourceIndex = Number(arrays.line_source_indexes[index]);
      const targetIndex = Number(arrays.line_target_indexes[index]);
      const code = Number(arrays.individual_codes[targetIndex]);
      const individual = String(binary.header.individuals?.[code] || "");
      const sourceBurst = Number(arrays.burst_values[sourceIndex]);
      const targetBurst = Number(arrays.burst_values[targetIndex]);
      const burstId = `${individual}:${binary.header.implicit_set || "train"}:source_${targetBurst}`;
      const visible = selectedCodes.has(code) && !hiddenBurstIds.has(burstId);
      lineFilter[index] = visible ? 1 : 0;
      burstFilter[index] = visible && sourceBurst === targetBurst ? 1 : 0;
      const colorOffset = index * 4;
      const pointOffset = targetIndex * 4;
      lineColors[colorOffset] = pointColors[pointOffset];
      lineColors[colorOffset + 1] = pointColors[pointOffset + 1];
      lineColors[colorOffset + 2] = pointColors[pointOffset + 2];
      lineColors[colorOffset + 3] = Math.round(185 * (activeQueueIndividual && individual !== activeQueueIndividual ? 0.25 : 1));
    }
    return { pointColors, pointFilter, suspectedFilter, confirmedFilter, thresholdFilter, lineColors, lineFilter, burstFilter };
  }

  binaryDeckLayers(visibleIndividuals, showPoints) {
    const binary = this.data?.binaryMovement;
    if (!binary || !window.deck?.DataFilterExtension) return [];
    const attributes = this.buildBinaryRenderAttributes(visibleIndividuals, showPoints);
    const pointData = {
      length: Number(binary.header.row_count) || 0,
      attributes: {
        getPosition: { value: binary.arrays.positions, size: 2 },
        getFillColor: { value: attributes.pointColors, size: 4 },
        getFilterValue: { value: attributes.pointFilter, size: 1 },
      },
    };
    const lineBaseAttributes = {
      getSourcePosition: { value: binary.lineSourcePositions, size: 2 },
      getTargetPosition: { value: binary.lineTargetPositions, size: 2 },
    };
    const filterExtension = new deck.DataFilterExtension({ filterSize: 1 });
    const layers = [];
    if (this.refs.showBursts.checked) {
      layers.push(new deck.LineLayer({
        id: "movement-binary-burst-casing",
        data: {
          length: Number(binary.header.line_count) || 0,
          attributes: {
            ...lineBaseAttributes,
            getFilterValue: { value: attributes.burstFilter, size: 1 },
          },
        },
        getColor: BURST_CASING_RGB,
        getWidth: 9,
        widthUnits: "meters",
        widthMinPixels: 2,
        filterRange: [1, 1],
        extensions: [filterExtension],
        pickable: false,
      }));
    }
    layers.push(new deck.LineLayer({
      id: "movement-binary-paths",
      data: {
        length: Number(binary.header.line_count) || 0,
        attributes: {
          ...lineBaseAttributes,
          getColor: { value: attributes.lineColors, size: 4 },
          getFilterValue: { value: attributes.lineFilter, size: 1 },
        },
      },
      getWidth: 3,
      widthUnits: "meters",
      widthMinPixels: 2,
      filterRange: [1, 1],
      extensions: [filterExtension],
      pickable: false,
    }));
    if (showPoints) {
      layers.push(new deck.ScatterplotLayer({
        id: "movement-binary-points",
        data: pointData,
        getRadius: 68,
        radiusMinPixels: 3,
        radiusMaxPixels: 8,
        filterRange: [1, 1],
        extensions: [filterExtension],
        pickable: true,
      }));
      layers.push(new deck.ScatterplotLayer({
        id: "movement-binary-threshold-points",
        data: {
          length: pointData.length,
          attributes: {
            getPosition: pointData.attributes.getPosition,
            getFilterValue: { value: attributes.thresholdFilter, size: 1 },
          },
        },
        getLineColor: [255, 236, 148, 255],
        filled: false,
        stroked: true,
        lineWidthMinPixels: 2.5,
        getRadius: 108,
        radiusMinPixels: 6,
        radiusMaxPixels: 12,
        filterRange: [1, 1],
        extensions: [filterExtension],
        pickable: true,
      }));
      layers.push(new deck.ScatterplotLayer({
        id: "movement-binary-suspected-outline",
        data: {
          length: pointData.length,
          attributes: {
            getPosition: pointData.attributes.getPosition,
            getFillColor: pointData.attributes.getFillColor,
            getFilterValue: { value: attributes.suspectedFilter, size: 1 },
          },
        },
        getLineColor: [255, 204, 40, 255],
        filled: true,
        stroked: true,
        lineWidthMinPixels: 3,
        getRadius: 135,
        radiusMinPixels: 8,
        radiusMaxPixels: 17,
        filterRange: [1, 1],
        extensions: [filterExtension],
        pickable: true,
      }));
      if (this.refs.showConfirmed.checked) {
        layers.unshift(new deck.ScatterplotLayer({
          id: "movement-binary-confirmed-exclusions",
          data: {
            length: pointData.length,
            attributes: {
              getPosition: pointData.attributes.getPosition,
              getFilterValue: { value: attributes.confirmedFilter, size: 1 },
            },
          },
          getFillColor: [92, 101, 110, 24],
          getLineColor: [92, 101, 110, 105],
          filled: true,
          stroked: true,
          lineWidthMinPixels: 1,
          getRadius: 52,
          radiusMinPixels: 3,
          radiusMaxPixels: 6,
          filterRange: [1, 1],
          extensions: [filterExtension],
          pickable: true,
        }));
      }
    }
    return layers;
  }

  renderLayers({ temporalOnly = false } = {}) {
    this.renderBurstCountIndicator();
    this.syncFixPopupVisibility();
    if (!this.data || !this.overlay || !this.mapLoaded) {
      if (this.overlay) {
        try {
          this.overlay.setProps({ layers: [] });
        } catch {}
      }
      this.renderFixPopup();
      return;
    }

    const visibleIndividuals = new Set(this.data.selectedIndividuals);
    const visibleSetNames = this.getVisibleSetNames();
    const showPoints = this.refs.showPoints.checked;
    const hiddenBurstFixKeys = new Set(
      [...this.hiddenBurstIds].flatMap(
        burstId => this.data.autoBurstById?.get(burstId)?.fixKeys || [],
      ),
    );
    const allPathData = this.getVisibleTrackSteps(visibleIndividuals, visibleSetNames);
    const pathData = allPathData
      .filter(step => (
        !hiddenBurstFixKeys.has(step.sourceFix?.fixKey)
        && !hiddenBurstFixKeys.has(step.destinationFix?.fixKey)
      ));
    const pointData = showPoints
      ? this.getVisibleMovementPoints(visibleIndividuals, visibleSetNames)
        .filter(point => !hiddenBurstFixKeys.has(point.fix?.fixKey))
      : [];
    const temporalFocalData = showPoints && this.temporalSliderEngaged
      ? this.buildTemporalFocalData(visibleIndividuals, visibleSetNames)
      : { points: [] };
    if (hiddenBurstFixKeys.size) {
      temporalFocalData.points = temporalFocalData.points.filter(
        point => !hiddenBurstFixKeys.has(point.fixKey),
      );
    }
    const thresholdPointData = [];
    const selectedThresholdPointData = [];
    const candidatePointData = [];
    const selectedCandidatePointData = [];
    const selectedPointData = [];
    const suspectedPointData = [];
    const confirmedPointData = [];
    const showSuspectedOutlines = this.data.suspiciousState === "loaded";
    if (this.refs.showConfirmed.checked) {
      const seenConfirmed = new Set();
      for (const fix of this.data.confirmedPointFixes || []) {
        if (
          fix.review?.status !== "confirmed"
          || !visibleIndividuals.has(fix.individual)
          || !visibleSetNames.has(fix.setName)
        ) {
          continue;
        }
        if (seenConfirmed.has(fix.fixKey)) {
          continue;
        }
        seenConfirmed.add(fix.fixKey);
        confirmedPointData.push({
          fixKey: fix.fixKey,
          individual: fix.individual,
          setName: fix.setName,
          position: fix.position,
        });
      }
    }
    const visibleSegments = this.getVisibleSegments();
    const visibleFlaggedSteps = (this.data.flaggedStepOverlays || []).filter(step => (
      visibleIndividuals.has(step.individual)
      && visibleSetNames.has(step.setName)
      && !hiddenBurstFixKeys.has(step.fixKey)
      && (step.status !== "confirmed" || this.refs.showConfirmed.checked)
    ));
    const visibleAutoBursts = this.getVisibleAutoBursts();
    const drawableAutoBursts = visibleAutoBursts.filter(burst => burst.path.length >= 2);
    const visibleAutoBurstPaths = drawableAutoBursts
      .map(burst => this.data.autoBurstRenderCache.get(burst.burstId)?.pathItem)
      .filter(Boolean);
    const manualFlagOutlinePaths = [];
    if (this.flagTargetKind === "individual" && this.manualFlagTarget.individual) {
      for (const step of allPathData) {
        if (step.individual === this.manualFlagTarget.individual) {
          manualFlagOutlinePaths.push(step);
        }
      }
    } else if (this.flagTargetKind === "bursts") {
      for (const burst of this.flagTargetBursts()) {
        if (
          visibleIndividuals.has(burst.individual)
          && visibleSetNames.has(burst.setName)
          && burst.path.length >= 2
        ) {
          manualFlagOutlinePaths.push({
            path: burst.path,
            individual: burst.individual,
            setName: burst.setName,
          });
        }
      }
    }
    const visibleTableSelection = this.getVisibleTableSelectionFixes();
    const visibleTableSelectionKeys = new Set(visibleTableSelection.map(fix => fix.fixKey));
    const tableSelectedPointData = visibleTableSelection
      .filter((fix, index, fixes) => index === 0 || index === fixes.length - 1)
      .map(fix => ({
        fixKey: fix.fixKey,
        position: fix.position,
        color: [255, 204, 40, 235],
      }));
    const segmentSelection = this.hasTableSelection()
      ? this.getCurrentSegmentSelection()
      : null;
    const tableSelectionPath = segmentSelection?.fixes
      ?.filter(fix => visibleTableSelectionKeys.has(fix.fixKey))
      .map(fix => fix.position) || [];
    const thresholdMatchKeys = showPoints
      ? temporalOnly ? this.lastThresholdMatchKeys : this.getActiveThresholdMatchKeys()
      : new Set();
    const candidateMatchKeys = showPoints
      ? temporalOnly ? this.lastCandidateMatchKeys : this.getCandidateQueryMatchKeys()
      : new Set();
    if (!temporalOnly) {
      this.lastThresholdMatchKeys = thresholdMatchKeys;
      this.lastCandidateMatchKeys = candidateMatchKeys;
    }
    const focusedRankingBurstFixes = this.getFocusedRankingBurstFixes();
    const focusedRankingBurstPoints = focusedRankingBurstFixes.map(fix => ({
      fixKey: fix.fixKey,
      position: fix.position,
    }));
    const hasFocusedRankingBurst = focusedRankingBurstFixes.length > 0;
    const focusedBurstId = hasFocusedRankingBurst
      ? String(this.focusedRankingBurst?.burstId || "")
      : "";
    // Layers with identity-stable data skip attribute recomputation unless an
    // update trigger changes. Queue dimming and burst focus both depend on
    // state outside the data array, so they must be declared as triggers or
    // their colors freeze when the active individual or focused burst changes.
    const queueDimKey = this.individualReviewQueue.mode === "queue"
      ? `queue:${this.queueActiveIndividual()}`
      : "browse";
    if (showSuspectedOutlines) {
      const seenSuspected = new Set();
      for (const fix of this.data.fixes || []) {
        if (
          fix.review?.status !== "suspected"
          || !visibleIndividuals.has(fix.individual)
          || !visibleSetNames.has(fix.setName)
          || hiddenBurstFixKeys.has(fix.fixKey)
          || seenSuspected.has(fix.fixKey)
        ) {
          continue;
        }
        seenSuspected.add(fix.fixKey);
        suspectedPointData.push({
          fixKey: fix.fixKey,
          individual: fix.individual,
          setName: fix.setName,
          position: fix.position,
          fix,
        });
      }
    }

    if (showPoints) {
      const visibleFix = fixKey => {
        const fix = this.data.fixByKey.get(fixKey);
        return fix
          && !fix.analyticallyExcluded
          && fix.review?.status !== "confirmed"
          && visibleIndividuals.has(fix.individual)
          && visibleSetNames.has(fix.setName)
          ? fix
          : null;
      };
      for (const fixKey of this.data.selectedFixKeys) {
        const fix = visibleFix(fixKey);
        if (!fix) continue;
        const point = {
          fixKey: fix.fixKey,
          individual: fix.individual,
          setName: fix.setName,
          position: fix.position,
          color: this.colorForFix(fix),
        };
        selectedPointData.push({ ...point, status: fix.review.status || "unreviewed" });
      }
      for (const fixKey of thresholdMatchKeys) {
        const fix = visibleFix(fixKey);
        if (!fix) continue;
        const target = this.data.selectedFixKeys.has(fixKey)
          ? selectedThresholdPointData
          : thresholdPointData;
        target.push({ fixKey, individual: fix.individual, position: fix.position });
      }
      for (const fixKey of candidateMatchKeys) {
        const fix = visibleFix(fixKey);
        if (!fix) continue;
        const target = this.data.selectedFixKeys.has(fixKey)
          ? selectedCandidatePointData
          : candidatePointData;
        target.push({ fixKey, individual: fix.individual, position: fix.position });
      }
    }

    const layers = [];
    if (confirmedPointData.length) {
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-confirmed-exclusions",
          data: confirmedPointData,
          getPosition: item => item.position,
          getFillColor: item => this.queueMapColor(
            [92, 101, 110, 24],
            item.individual,
          ),
          getLineColor: item => this.queueMapColor(
            [92, 101, 110, 105],
            item.individual,
          ),
          filled: true,
          stroked: true,
          lineWidthMinPixels: 1,
          getRadius: 52,
          radiusMinPixels: 3,
          radiusMaxPixels: 6,
          pickable: true,
        }),
      );
    }
    if (visibleAutoBurstPaths.length) {
      layers.push(
        new deck.PathLayer({
          id: "movement-burst-casing",
          data: visibleAutoBurstPaths,
          dataComparator: sameArrayItems,
          getPath: item => item.path,
          getColor: item => this.burstCasingColor(item, focusedBurstId),
          getWidth: item => this.burstCasingWidth(item, focusedBurstId),
          widthMinPixels: 2,
          updateTriggers: {
            getColor: [focusedBurstId, queueDimKey],
            getWidth: focusedBurstId,
          },
          pickable: false,
        }),
      );
      layers.push(
        new deck.PathLayer({
          id: "movement-bursts",
          data: visibleAutoBurstPaths,
          dataComparator: sameArrayItems,
          getPath: item => item.path,
          getColor: item => this.burstFillColor(item, focusedBurstId),
          getWidth: item => this.burstFillWidth(item),
          widthMinPixels: 1,
          updateTriggers: {
            getColor: [focusedBurstId, queueDimKey],
          },
          pickable: true,
        }),
      );
    }

    if (manualFlagOutlinePaths.length) {
      layers.push(
        new deck.PathLayer({
          id: "movement-manual-flag-target-outline",
          data: manualFlagOutlinePaths,
          dataComparator: sameArrayItems,
          getPath: item => item.path,
          getColor: [255, 204, 40, 210],
          getWidth: 9,
          widthMinPixels: 5,
          pickable: false,
        }),
      );
    }

    if (tableSelectionPath.length >= 2) {
      layers.push(
        new deck.PathLayer({
          id: "movement-table-selection-path",
          data: [{ path: tableSelectionPath }],
          getPath: item => item.path,
          getColor: [255, 204, 40, 210],
          getWidth: 9,
          widthMinPixels: 5,
          pickable: false,
        }),
      );
    }

    if (this.data.binaryMovement) {
      layers.push(...this.binaryDeckLayers(visibleIndividuals, showPoints));
    }

    // Each derived step value belongs to its destination fix, so the inbound
    // segment uses that fix's selected-variable color. Drawing it above the
    // optional burst casing keeps speed and other step fields legible.
    if (!this.data.binaryMovement) {
      layers.push(new deck.PathLayer({
        id: "movement-paths",
        data: pathData,
        dataComparator: sameArrayItems,
        getPath: item => item.path,
        getColor: item => {
          const color = this.colorForFix(item.destinationFix);
          return this.queueMapColor(
            [color[0], color[1], color[2], item.sourceFlagged ? 52 : 185],
            item.individual,
          );
        },
        getWidth: item => item.sourceFlagged ? 1.5 : 3,
        widthMinPixels: 2,
        updateTriggers: {
          getColor: [this.refs.colorBy.value, queueDimKey],
        },
        pickable: false,
      }));
    }

    if (visibleFlaggedSteps.length) {
      layers.push(
        new deck.PathLayer({
          id: "movement-flagged-fix-steps",
          data: visibleFlaggedSteps,
          dataComparator: sameArrayItems,
          getPath: item => item.path,
          getColor: item => item.status === "confirmed"
            ? [92, 101, 110, 115]
            : [255, 204, 40, 235],
          getWidth: item => item.status === "confirmed" ? 3 : 5,
          widthMinPixels: 2,
          pickable: false,
        }),
      );
    }

    if (visibleSegments.length) {
      layers.push(
        new deck.PathLayer({
          id: "movement-segment-outline",
          data: visibleSegments,
          getPath: segment => segment.path,
          getColor: segment => this.queueMapColor(
            segment.status === "confirmed"
              ? [255, 255, 255, 30]
              : [255, 255, 255, 120],
            segment.individual,
          ),
          getWidth: segment => segment.status === "confirmed" ? 3.5 : 7,
          widthMinPixels: 2,
          pickable: false,
        }),
      );
      layers.push(
        new deck.PathLayer({
          id: "movement-segments",
          data: visibleSegments,
          getPath: segment => segment.path,
          getColor: segment => this.queueMapColor(
            segment.status === "confirmed"
              ? [92, 101, 110, 90]
              : [245, 181, 54, 210],
            segment.individual,
          ),
          getWidth: segment => segment.status === "confirmed" ? 2 : 4.5,
          widthMinPixels: 1,
          pickable: false,
        }),
      );
    }

    if (showPoints) {
      if (!this.data.binaryMovement) {
        layers.push(new deck.ScatterplotLayer({
          id: "movement-points",
          data: pointData,
          dataComparator: sameArrayItems,
          getPosition: item => item.position,
          getFillColor: item => this.queueMapColor(
            this.colorForFix(item.fix),
            item.individual,
          ),
          getRadius: 68,
          radiusMinPixels: 3,
          radiusMaxPixels: 8,
          updateTriggers: {
            getFillColor: [this.refs.colorBy.value, queueDimKey],
          },
          pickable: true,
        }));
      }
      if (temporalFocalData.points.length) {
        layers.push(
          new deck.ScatterplotLayer({
            id: "movement-temporal-focal-halos",
            data: temporalFocalData.points,
            getPosition: item => item.position,
            getLineColor: item => item.focal
              ? [255, 238, 153, 245]
              : [255, 255, 255, 190],
            filled: false,
            stroked: true,
            lineWidthMinPixels: 4,
            getRadius: item => item.focal ? 260 : 190,
            radiusMinPixels: 12,
            radiusMaxPixels: 34,
            pickable: false,
          }),
        );
        layers.push(
          new deck.ScatterplotLayer({
            id: "movement-temporal-focal-points",
            data: temporalFocalData.points,
            getPosition: item => item.position,
            getFillColor: item => item.color,
            getLineColor: item => item.focal
              ? [255, 255, 255, 250]
              : [255, 255, 255, 105],
            filled: true,
            stroked: true,
            lineWidthMinPixels: 2,
            getRadius: item => item.focal ? 180 : 130,
            radiusMinPixels: 9,
            radiusMaxPixels: 24,
            pickable: true,
          }),
        );
      }
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-suspected-outline",
          data: suspectedPointData,
          getPosition: item => item.position,
          getFillColor: item => this.queueMapColor(
            this.colorForFix(item.fix),
            item.individual,
          ),
          getLineColor: item => this.queueMapColor(
            [255, 204, 40, 255],
            item.individual,
          ),
          filled: true,
          stroked: true,
          lineWidthMinPixels: 3,
          getRadius: 135,
          radiusMinPixels: 8,
          radiusMaxPixels: 17,
          updateTriggers: {
            getFillColor: [this.refs.colorBy.value, queueDimKey],
            getLineColor: queueDimKey,
          },
          pickable: true,
        }),
      );
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-threshold-points",
          data: thresholdPointData,
          getPosition: item => item.position,
          getLineColor: item => this.queueMapColor(
            [255, 236, 148, 255],
            item.individual,
          ),
          filled: false,
          stroked: true,
          lineWidthMinPixels: 2.5,
          getRadius: 108,
          radiusMinPixels: 6,
          radiusMaxPixels: 12,
          pickable: true,
        }),
      );
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-selected-threshold-points",
          data: selectedThresholdPointData,
          getPosition: item => item.position,
          getLineColor: item => this.queueMapColor(
            [255, 236, 148, 255],
            item.individual,
          ),
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
          id: "movement-candidate-query-points",
          data: candidatePointData,
          getPosition: item => item.position,
          getLineColor: item => this.queueMapColor(
            [72, 222, 255, 255],
            item.individual,
          ),
          filled: false,
          stroked: true,
          lineWidthMinPixels: 2.5,
          getRadius: 132,
          radiusMinPixels: 8,
          radiusMaxPixels: 15,
          pickable: false,
        }),
      );
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-selected-candidate-query-points",
          data: selectedCandidatePointData,
          getPosition: item => item.position,
          getLineColor: item => this.queueMapColor(
            [72, 222, 255, 255],
            item.individual,
          ),
          filled: false,
          stroked: true,
          lineWidthMinPixels: 3,
          getRadius: 182,
          radiusMinPixels: 11,
          radiusMaxPixels: 21,
          pickable: false,
        }),
      );
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-selected-points",
          data: selectedPointData,
          getPosition: item => item.position,
          getFillColor: item => this.queueMapColor(
            hasFocusedRankingBurst
              ? this.mutedRankingContextColor(item.color, 54)
              : item.color,
            item.individual,
          ),
          getLineColor: item => this.queueMapColor(
            hasFocusedRankingBurst
              ? [255, 204, 40, 115]
              : [255, 204, 40, 255],
            item.individual,
          ),
          stroked: true,
          lineWidthMinPixels: 1.5,
          getRadius: 130,
          radiusMinPixels: 7,
          radiusMaxPixels: 14,
          pickable: true,
        }),
      );
    }

    if (tableSelectedPointData.length) {
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-table-selected-points",
          data: tableSelectedPointData,
          getPosition: item => item.position,
          getFillColor: [0, 0, 0, 0],
          getLineColor: item => item.color,
          filled: false,
          stroked: true,
          lineWidthMinPixels: 3,
          getRadius: 190,
          radiusMinPixels: 10,
          radiusMaxPixels: 22,
          pickable: false,
        }),
      );
    }

    if (focusedRankingBurstPoints.length) {
      layers.push(
        new deck.ScatterplotLayer({
          id: "movement-burst-focus-ring",
          data: focusedRankingBurstPoints,
          getPosition: item => item.position,
          getLineColor: BURST_FOCUS_RING_COLOR,
          filled: false,
          stroked: true,
          lineWidthMinPixels: 2,
          getRadius: 146,
          radiusMinPixels: 7,
          radiusMaxPixels: 16,
          pickable: false,
        }),
      );
    }

    layers.push(...this.getOsmDeckLayers());

    try {
      this.overlay.setProps({
        layers,
        useDevicePixels: (this.data.binaryMovement?.header?.row_count || pointData.length + selectedPointData.length) <= LARGE_MAP_POINT_THRESHOLD,
      });
    } catch (error) {
      this.setStatus(`Map warning: ${error.message}`, true);
    }
    this.renderFixPopup();
  }

  isSourceOnlyFlaggedBurst(burst) {
    return isSourceOnlyFlaggedBurstFromData(this.data, burst);
  }

  colorForFix(fix) {
    const field = this.data.colorFieldByKey.get(this.refs.colorBy.value) || this.data.colorFields[0];
    if (!field) {
      return [124, 210, 255, POINT_ALPHA];
    }
    if (field.key === INDIVIDUAL_COLOR_FIELD_KEY) {
      return splitColor(
        this.data.individualPalette[fix.individual] || [124, 210, 255],
        fix.setName,
        POINT_ALPHA,
      );
    }
    const style = this.data.colorStyles.get(field.key);
    const value = movementColorFieldValue(fix, field);
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
    this.setCheckedFixIncluded(
      fixKey,
      !this.data.selectedFixKeys.has(fixKey),
      "checked_fix_card",
    );
  }

  setCheckedFixIncluded(fixKey, included, selectionMethod = "map_check") {
    const fix = this.data?.fixByKey?.get(String(fixKey || ""));
    if (!fix) return;
    if (this.individualReviewQueue.mode === "queue") {
      const activeIndividual = this.individualReviewQueue.activeIndividual;
      if (activeIndividual && fix.individual !== activeIndividual) {
        this.setStatus("Choose fixes belonging to the active queue individual.", true);
        this.renderTableSheet();
        return;
      }
    }
    if (this.flagTargetKind !== "fixes") {
      this.resetManualFlagTarget();
      this.data.selectedFixKeys = new Set();
      this.setTableSelection();
      this.mapRangeAwaitingEnd = false;
    }
    this.flagTargetKind = "fixes";
    this.manualFlagTarget.selectionMethods.add(selectionMethod);
    if (included) {
      this.data.selectedFixKeys.add(fix.fixKey);
    } else {
      this.data.selectedFixKeys.delete(fix.fixKey);
    }
    if (!this.data.selectedFixKeys.size) this.flagTargetKind = "none";
    this.renderThresholdPane();
    this.renderSelectedFixes();
    if (this.individualReviewQueue.mode === "queue") {
      this.renderIndividuals();
    }
    this.renderLayers();
    this.updateActionButtons();
  }

  getMapPickedFeatureSpaceBurst(event) {
    if (
      !this.overlay
      || !this.data
      || this.refs?.sideSheetTabs?.dataset.activeSheet !== "feature_space"
      || !(this.burstFeatureSpace?.points || []).length
    ) {
      return null;
    }
    const point = event?.point;
    if (!point || !Number.isFinite(point.x) || !Number.isFinite(point.y)) {
      return null;
    }
    const pickedObjects = this.overlay.pickMultipleObjects({
      x: Number(point.x),
      y: Number(point.y),
      radius: 8,
      depth: 20,
    }) || [];
    for (const picked of pickedObjects) {
      const burstId = String(picked?.object?.burst?.burstId || "");
      if (burstId && this.getBurstFeatureSpacePoint(burstId)) {
        return this.data.autoBurstById?.get(burstId) || picked.object.burst;
      }
    }
    return null;
  }

  focusMapBurst(burstId) {
    const burst = this.data?.autoBurstById?.get(String(burstId || ""));
    if (!burst) {
      return false;
    }
    if (this.focusedRankingBurst?.burstId === burst.burstId) {
      this.focusedRankingBurst = null;
      this.renderLayers();
      this.setStatus(`Cleared focus on burst ${burst.burstId}.`);
      return true;
    }
    this.setFocusedRankingBurst({
      burst_id: burst.burstId,
      individual: burst.individual,
      set_name: burst.setName,
      start_time_ms: burst.startTimeMs,
      end_time_ms: burst.endTimeMs,
      n_fixes: burst.fixCount,
      fix_keys: burst.fixKeys,
    });
    this.renderLayers();
    this.setStatus(`Focused burst ${burst.burstId}.`);
    return true;
  }

  selectMapBurstInFeatureSpace(burst) {
    if (!burst?.burstId) {
      return false;
    }
    const point = this.selectBurstFeatureSpacePoint(burst.burstId, { render: false });
    if (!point) {
      return false;
    }
    this.setFocusedRankingBurst({
      burst_id: burst.burstId,
      individual: burst.individual,
      set_name: burst.setName,
      start_time_ms: burst.startTimeMs,
      end_time_ms: burst.endTimeMs,
      n_fixes: burst.fixCount,
      fix_keys: burst.fixKeys,
    });
    this.setSideSheet("feature_space");
    this.renderBurstFeatureSpace();
    this.renderLayers();
    this.setStatus(`Selected burst ${burst.burstId} in feature space.`);
    return true;
  }

  handleMapClick(event) {
    if (!this.overlay || !this.data) {
      return;
    }
    const pickedBurst = this.getMapPickedFeatureSpaceBurst(event);
    if (pickedBurst && this.selectMapBurstInFeatureSpace(pickedBurst)) {
      return;
    }
    const picked = this.getMapPickedObject(event);
    if (!picked?.object?.fixKey) {
      return;
    }
    const fixKey = picked.object.fixKey;
    if (this.pendingMapSingleClickTimer !== null) {
      window.clearTimeout(this.pendingMapSingleClickTimer);
    }
    this.pendingMapSingleClickTimer = window.setTimeout(() => {
      this.pendingMapSingleClickTimer = null;
      this.applyMapSingleFixClick(fixKey);
    }, 220);
  }

  getMapPickedObject(event) {
    const point = event?.point;
    const picked = point && Number.isFinite(point.x) && Number.isFinite(point.y)
      ? this.overlay?.pickObject({
        x: Number(point.x),
        y: Number(point.y),
        radius: 6,
      })
      : null;
    if (
      picked
      && String(picked.layer?.id || "").startsWith("movement-binary-")
      && Number.isInteger(picked.index)
    ) {
      return { ...picked, object: this.binaryFixAt(picked.index) };
    }
    return picked;
  }

  applyMapSingleFixClick(fixKey) {
    if (!this.data?.fixByKey?.has(fixKey)) return;
    const included = !this.data.selectedFixKeys.has(fixKey);
    this.setCheckedFixIncluded(fixKey, included, "map_check");
  }

  handleMapDoubleClick(event) {
    event?.preventDefault?.();
    event?.originalEvent?.preventDefault?.();
    if (this.pendingMapSingleClickTimer !== null) {
      window.clearTimeout(this.pendingMapSingleClickTimer);
      this.pendingMapSingleClickTimer = null;
    }
    if (!this.overlay || !this.data) return;
    const picked = this.getMapPickedObject(event);
    const fixKey = String(picked?.object?.fixKey || "");
    if (!fixKey) return;
    this.applyMapRangeEndpoint(fixKey);
    if (this.refs?.sideSheetTabs?.dataset.activeSheet === "table") {
      this.renderTableSheet();
    }
    this.renderLayers();
    this.updateActionButtons();
  }

  handleMapContextMenu(event) {
    event?.preventDefault?.();
    event?.originalEvent?.preventDefault?.();
    if (!this.overlay || !this.data) {
      this.closeFixPopup();
      return;
    }
    const point = event?.point;
    const picked = point && Number.isFinite(point.x) && Number.isFinite(point.y)
      ? this.overlay.pickObject({
        x: Number(point.x),
        y: Number(point.y),
        radius: 6,
      })
      : null;
    if (!picked?.object?.fixKey) {
      this.closeFixPopup();
      return;
    }
    this.openFixPopup(picked.object, {
      x: Number(point.x),
      y: Number(point.y),
    });
  }

  openFixPopup(point, info) {
    const fixKey = String(point?.fixKey || "");
    if (!fixKey) {
      this.closeFixPopup();
      return;
    }
    this.activeFixPopup = {
      fixKey,
      screenX: Number.isFinite(info?.x) ? Number(info.x) : this.refs.map.clientWidth / 2,
      screenY: Number.isFinite(info?.y) ? Number(info.y) : this.refs.map.clientHeight / 2,
    };
    this.renderFixPopup();
  }

  closeFixPopup() {
    this.activeFixPopup = null;
    this.renderFixPopup();
  }

  getPopupFix() {
    if (!this.data || !this.activeFixPopup?.fixKey) {
      return null;
    }
    return this.data.fixByKey.get(this.activeFixPopup.fixKey) || null;
  }

  isFixVisibleOnMap(fix) {
    if (!fix || !this.data) {
      return false;
    }
    if (fix.analyticallyExcluded || fix.review?.status === "confirmed") {
      return this.refs.showConfirmed.checked;
    }
    const visibleIndividuals = this.data.selectedIndividuals instanceof Set
      ? this.data.selectedIndividuals
      : new Set();
    const inVisibleTrack = visibleIndividuals.has(fix.individual)
      && this.getVisibleSetNames().has(fix.setName);
    if (isSourceOnlyFlaggedFix(fix)) {
      return inVisibleTrack;
    }
    return this.refs.showPoints.checked && inVisibleTrack;
  }

  syncFixPopupVisibility() {
    const fix = this.getPopupFix();
    if (!fix || !this.isFixVisibleOnMap(fix)) {
      this.activeFixPopup = null;
    }
  }

  renderFixPopup() {
    const popupEl = this.refs.fixPopup;
    if (!popupEl) {
      return;
    }
    const fix = this.getPopupFix();
    if (!this.activeFixPopup || !fix || !this.isFixVisibleOnMap(fix)) {
      popupEl.innerHTML = "";
      popupEl.classList.add("hidden");
      popupEl.style.left = "";
      popupEl.style.top = "";
      return;
    }

    const popupFields = this.buildPopupFields(fix);
    popupEl.innerHTML = `
      <div class="movement-fix-popup-head">
        <div>
          <div class="movement-fix-popup-title">Fix details</div>
          <div class="movement-fix-popup-subtitle">${escapeHtml(fix.individual)}</div>
        </div>
        <button type="button" class="movement-fix-popup-close" data-role="fix-popup-close" aria-label="Close fix details">X</button>
      </div>
      <div class="movement-fix-popup-fields">
        ${popupFields.map(field => `
          <div class="movement-fix-popup-row">
            <div class="movement-fix-popup-label">${escapeHtml(field.label)}</div>
            <div class="movement-fix-popup-value">${escapeHtml(field.value)}</div>
          </div>
        `).join("")}
      </div>
    `;
    popupEl.classList.remove("hidden");

    const mapWidth = this.refs.map.clientWidth || popupEl.offsetWidth || 0;
    const mapHeight = this.refs.map.clientHeight || popupEl.offsetHeight || 0;
    const popupWidth = popupEl.offsetWidth || 0;
    const popupHeight = popupEl.offsetHeight || 0;
    const preferredLeft = this.activeFixPopup.screenX + FIX_POPUP_OFFSET_PX;
    const preferredRight = this.activeFixPopup.screenX - popupWidth - FIX_POPUP_OFFSET_PX;
    const preferredBelow = this.activeFixPopup.screenY + FIX_POPUP_OFFSET_PX;
    const preferredAbove = this.activeFixPopup.screenY - popupHeight - FIX_POPUP_OFFSET_PX;
    const left = preferredLeft + popupWidth <= (mapWidth - FIX_POPUP_EDGE_PADDING_PX)
      ? preferredLeft
      : preferredRight >= FIX_POPUP_EDGE_PADDING_PX
        ? preferredRight
        : clamp(preferredLeft, FIX_POPUP_EDGE_PADDING_PX, Math.max(FIX_POPUP_EDGE_PADDING_PX, mapWidth - popupWidth - FIX_POPUP_EDGE_PADDING_PX));
    const top = preferredBelow + popupHeight <= (mapHeight - FIX_POPUP_EDGE_PADDING_PX)
      ? preferredBelow
      : preferredAbove >= FIX_POPUP_EDGE_PADDING_PX
        ? preferredAbove
        : clamp(preferredBelow, FIX_POPUP_EDGE_PADDING_PX, Math.max(FIX_POPUP_EDGE_PADDING_PX, mapHeight - popupHeight - FIX_POPUP_EDGE_PADDING_PX));
    popupEl.style.left = `${left}px`;
    popupEl.style.top = `${top}px`;
  }

  buildPopupFields(fix) {
    const rows = [];
    const seenKeys = new Set();
    const addRow = (id, label, value, { allowMissing = false } = {}) => {
      if (!allowMissing && (value === null || value === undefined || value === "")) {
        return;
      }
      if (seenKeys.has(id)) {
        return;
      }
      rows.push({ label, value: value === null || value === undefined || value === "" ? "missing" : String(value) });
      seenKeys.add(id);
    };
    addRow("individual", "Individual", fix.individual, { allowMissing: true });
    addRow("timestamp", "Timestamp", formatTimestamp(fix.timeMs), { allowMissing: true });
    addRow(
      "source_flags",
      "Source flags",
      Array.isArray(fix.sourceFlags) ? fix.sourceFlags.join(", ") : "",
    );

    for (const fieldKey of FIX_POPUP_DEFAULT_FIELDS) {
      const resolved = this.resolvePopupFieldValue(fix, fieldKey);
      if (!resolved) {
        continue;
      }
      addRow(resolved.id, resolved.label, resolved.value, { allowMissing: resolved.allowMissing === true });
    }

    const colorField = this.getCurrentColorField();
    if (colorField) {
      addRow(
        colorField.key,
        colorField.label,
        formatColorValue(movementColorFieldValue(fix, colorField), colorField.kind),
        { allowMissing: true },
      );
    }

    return rows;
  }

  resolvePopupFieldValue(fix, fieldKey) {
    switch (fieldKey) {
      case "set":
        return { id: "set", label: "Set", value: fix.setName };
      case "fix_key":
        return { id: "fix_key", label: "Fix key", value: fix.fixKey };
      case "step_length_m":
        return fix.attributes?.step_length_m === undefined
          ? null
          : { id: "step_length_m", label: "Step length", value: formatMaybeNumber(fix.attributes.step_length_m, "m") };
      case "speed_mps":
        return fix.attributes?.speed_mps === undefined
          ? null
          : { id: "speed_mps", label: "Speed", value: formatMaybeNumber(fix.attributes.speed_mps, "m/s") };
      case "time_delta_s":
        return fix.attributes?.time_delta_s === undefined
          ? null
          : { id: "time_delta_s", label: "Time delta", value: formatMaybeNumber(fix.attributes.time_delta_s, "s") };
      case "turn_angle_deg":
        return fix.attributes?.turn_angle_deg === undefined
          ? null
          : { id: "turn_angle_deg", label: "Turn angle", value: formatMaybeNumber(fix.attributes.turn_angle_deg, "°") };
      case "review.status": {
        const status = String(fix.review?.status || "").trim();
        return status ? { id: "review.status", label: "Review", value: status } : null;
      }
      case "review.issue_type": {
        const issueTypes = this.getPopupIssueTypes(fix);
        return issueTypes.length ? { id: "review.issue_type", label: "Issue type", value: issueTypes.join(", ") } : null;
      }
      default:
        return null;
    }
  }

  getPopupIssueTypes(fix) {
    const issues = Array.isArray(fix?.review?.issues) ? fix.review.issues : [];
    const typedIssues = uniqueNonEmpty(issues.map(issue => issue.issueType || ""));
    if (typedIssues.length) {
      return typedIssues;
    }
    const legacyType = String(fix?.review?.issueType || "").trim();
    return legacyType ? [legacyType] : [];
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
      fix => (
        !fix.analyticallyExcluded
        && fix.review?.status !== "confirmed"
        && visibleIndividuals.has(fix.individual)
        && visibleSetNames.has(fix.setName)
      ),
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
    if (this.flagTargetKind === "filter") {
      this.flagTargetKind = this.data?.selectedFixKeys?.size ? "fixes" : "none";
    }
  }

  syncFlagTargetToThreshold() {
    if (this.getActiveThresholdMatchKeys().size) {
      this.resetManualFlagTarget({ resetKind: false });
      this.flagTargetKind = "filter";
    } else if (this.flagTargetKind === "filter") {
      this.flagTargetKind = this.data?.selectedFixKeys?.size ? "fixes" : "none";
    }
    this.updateActionButtons();
  }

  getBinaryThresholdContext(field) {
    const binary = this.data?.binaryMovement;
    if (!binary) return null;
    const arrays = binary.arrays;
    const selectedIndividuals = new Set(this.getSelectedIndividuals());
    const visibleIndexes = [];
    for (let index = 0; index < Number(binary.header.row_count); index += 1) {
      const individual = String(binary.header.individuals?.[Number(arrays.individual_codes[index])] || "");
      if (selectedIndividuals.has(individual) && Number(arrays.review_status[index]) !== 2) {
        visibleIndexes.push(index);
      }
    }
    const emptyKeys = new Set();
    if (!field || field.key === INDIVIDUAL_COLOR_FIELD_KEY) {
      return {
        field,
        visibleFixes: { length: visibleIndexes.length },
        numericFixes: { length: 0 },
        thresholdValue: null,
        histogram: null,
        reverse: false,
        histogramMode: "full",
        histogramInputMin: null,
        histogramInputMax: null,
        selectedLevels: [],
        levelOptions: [],
        disabledReason: field
          ? "Threshold selection is unavailable when coloring by individual ID. Use the Individuals panel to filter tracks instead."
          : "",
        matchKeys: emptyKeys,
        uncheckedMatchKeys: new Set(),
        matchCount: 0,
      };
    }
    const reverse = this.thresholdState.fieldKey === field.key && this.thresholdState.reverse === true;
    const selectedLevels = this.thresholdState.fieldKey === field.key
      ? uniqueNonEmpty(this.thresholdState.selectedLevels || [])
      : [];
    const matchIndexes = [];
    let matchCount = 0;
    if (field.kind !== "numeric") {
      let trueCount = 0;
      let falseCount = 0;
      for (const index of visibleIndexes) {
        if (Number(arrays.is_outlier[index])) trueCount += 1;
        else falseCount += 1;
      }
      const selected = new Set(selectedLevels);
      for (const index of visibleIndexes) {
        const level = Number(arrays.is_outlier[index]) ? "True" : "False";
        if (!selected.has(level)) continue;
        matchCount += 1;
        if (matchIndexes.length < MAX_SELECTED_FIXES_SHOWN) matchIndexes.push(index);
      }
      const fixes = matchIndexes.map(index => this.binaryFixAt(index)).filter(Boolean);
      const matchKeys = new Set(fixes.map(fix => fix.fixKey));
      return {
        field,
        visibleFixes: { length: visibleIndexes.length },
        numericFixes: { length: 0 },
        thresholdValue: null,
        histogram: null,
        reverse,
        histogramMode: "full",
        histogramInputMin: null,
        histogramInputMax: null,
        selectedLevels,
        levelOptions: [
          { level: "False", count: falseCount },
          { level: "True", count: trueCount },
        ].sort((left, right) => right.count - left.count),
        matchKeys,
        uncheckedMatchKeys: new Set([...matchKeys].filter(key => !this.data.selectedFixKeys.has(key))),
        matchCount,
        previewTruncated: matchCount > matchKeys.size,
      };
    }
    const sourceKey = field.key === GPS_SPIKE_COLOR_FIELD_KEY ? "step_length_m" : field.key;
    const values = arrays[sourceKey];
    const gpsSpikeMode = field.key === GPS_SPIKE_COLOR_FIELD_KEY;
    const numericValues = [];
    const eligibleIndexes = [];
    for (const index of visibleIndexes) {
      const value = values ? Number(values[index]) : NaN;
      const turnAngle = Number(arrays.turn_angle_deg[index]);
      if (!Number.isFinite(value) || (gpsSpikeMode && (!Number.isFinite(turnAngle) || Math.abs(turnAngle) < this.gpsSpikeTurnAngleDeg))) continue;
      numericValues.push(value);
      eligibleIndexes.push(index);
    }
    const styleRange = this.data.colorStyles.get(field.key)?.range
      || this.data.colorStyles.get(sourceKey)?.range
      || null;
    const histogramMode = this.thresholdState.fieldKey === field.key && this.thresholdState.histogramMode === "clipped"
      ? "clipped" : "full";
    const manualMin = this.thresholdState.fieldKey === field.key ? finiteOrNull(this.thresholdState.histogramMin) : null;
    const manualMax = this.thresholdState.fieldKey === field.key ? finiteOrNull(this.thresholdState.histogramMax) : null;
    const histogram = numericValues.length
      ? computeHistogramBins(numericValues, 24, {
          mode: histogramMode === "clipped" || manualMin !== null || manualMax !== null ? "clipped" : "full",
          clippedMin: manualMin ?? styleRange?.min ?? null,
          clippedMax: manualMax ?? styleRange?.max ?? null,
        })
      : null;
    const rawThreshold = this.thresholdState.fieldKey === field.key
      ? finiteOrNull(this.thresholdState.value) : null;
    const thresholdValue = rawThreshold === null || !histogram
      ? null : clampThresholdValue(rawThreshold, histogram.min, histogram.max);
    if (thresholdValue !== null) {
      eligibleIndexes.forEach((index, position) => {
        const matches = reverse ? numericValues[position] < thresholdValue : numericValues[position] > thresholdValue;
        if (!matches) return;
        matchCount += 1;
        if (matchIndexes.length < MAX_SELECTED_FIXES_SHOWN) matchIndexes.push(index);
      });
    }
    const fixes = matchIndexes.map(index => this.binaryFixAt(index)).filter(Boolean);
    const matchKeys = new Set(fixes.map(fix => fix.fixKey));
    return {
      field,
      visibleFixes: { length: visibleIndexes.length },
      numericFixes: { length: numericValues.length },
      histogram,
      thresholdValue,
      reverse,
      histogramMode: histogram?.mode === "clipped" ? "clipped" : "full",
      histogramInputMin: manualMin ?? (histogram?.mode === "clipped" ? histogram?.min : histogram?.observedMin),
      histogramInputMax: manualMax ?? (histogram?.mode === "clipped" ? histogram?.max : histogram?.observedMax),
      selectedLevels: [],
      levelOptions: [],
      matchKeys,
      uncheckedMatchKeys: new Set([...matchKeys].filter(key => !this.data.selectedFixKeys.has(key))),
      matchCount,
      previewTruncated: matchCount > matchKeys.size,
      gpsSpikeMode,
      turnAngleThreshold: this.gpsSpikeTurnAngleDeg,
    };
  }

  getThresholdContext() {
    if (!this.data) {
      return null;
    }
    const field = this.getCurrentColorField();
    if (this.data.binaryMovement) {
      return this.getBinaryThresholdContext(field);
    }
    const visibleFixes = this.getVisibleReviewFixes();
    const gpsSpikeMode = field?.key === GPS_SPIKE_COLOR_FIELD_KEY;
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
    if (field.key === INDIVIDUAL_COLOR_FIELD_KEY) {
      return {
        field,
        visibleFixes,
        numericFixes: [],
        thresholdValue: null,
        histogram: null,
        reverse: false,
        histogramMode: "full",
        histogramInputMin: null,
        histogramInputMax: null,
        selectedLevels: [],
        levelOptions: [],
        disabledReason: "Threshold selection is unavailable when coloring by individual ID. Use the Individuals panel to filter tracks instead.",
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
        const level = discreteFieldLevelLabel(field, movementColorFieldValue(fix, field));
        levelCounts.set(level, (levelCounts.get(level) || 0) + 1);
      }
      const levelOptions = Array.from(levelCounts.entries())
        .map(([level, count]) => ({ level, count }))
        .sort((left, right) => right.count - left.count || left.level.localeCompare(right.level, undefined, { sensitivity: "base" }));
      const selectedLevelSet = new Set(selectedLevels);
      const matchItems = selectedLevelSet.size
        ? visibleFixes.filter(fix => selectedLevelSet.has(discreteFieldLevelLabel(field, movementColorFieldValue(fix, field))))
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
        value: finiteOrNull(movementColorFieldValue(fix, field)),
        turnAngle: finiteOrNull(fix.attributes?.turn_angle_deg),
      }))
      .filter(item => (
        typeof item.value === "number"
        && (
          !gpsSpikeMode
          || (
            typeof item.turnAngle === "number"
            && Math.abs(item.turnAngle) >= this.gpsSpikeTurnAngleDeg
          )
        )
      ));
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
        gpsSpikeMode,
        turnAngleThreshold: this.gpsSpikeTurnAngleDeg,
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
      gpsSpikeMode,
      turnAngleThreshold: this.gpsSpikeTurnAngleDeg,
    };
  }

  getActiveThresholdMatchKeys() {
    if (!this.data) {
      return new Set();
    }
    const field = this.getCurrentColorField();
    if (!field || field.key === INDIVIDUAL_COLOR_FIELD_KEY || this.thresholdState.fieldKey !== field.key) {
      return new Set();
    }
    if (field.kind === "numeric") {
      if (!Number.isFinite(this.thresholdState.value)) {
        return new Set();
      }
      if (
        field.key === GPS_SPIKE_COLOR_FIELD_KEY
        && this.thresholdState.value <= 0
      ) {
        return new Set();
      }
    } else {
      const selectedLevels = Array.isArray(this.thresholdState.selectedLevels)
        ? this.thresholdState.selectedLevels
        : [];
      if (!selectedLevels.length) {
        return new Set();
      }
    }
    return this.getThresholdContext()?.matchKeys || new Set();
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
    const disabledReason = context?.disabledReason || "";
    const matchCount = Number(context?.matchCount) || context?.matchKeys?.size || 0;
    const uncheckedCount = context?.uncheckedMatchKeys?.size || 0;
    const histogram = context?.histogram;
    const histogramMode = context?.histogramMode === "clipped" ? "clipped" : "full";
    const histogramInputMin = finiteOrNull(context?.histogramInputMin);
    const histogramInputMax = finiteOrNull(context?.histogramInputMax);
    const gpsSpikeMode = context?.gpsSpikeMode === true;
    const gpsSpikeControl = gpsSpikeMode
      ? `
        <label class="movement-threshold-range-label">
          <span>Minimum absolute turn angle (°)</span>
          <input
            class="movement-threshold-range-input"
            type="number"
            min="0"
            max="180"
            step="any"
            data-action="set-gps-spike-turn-angle"
            value="${escapeHtml(String(this.gpsSpikeTurnAngleDeg))}"
          >
        </label>
        <div class="movement-threshold-note">All fixes remain colored by step length. The histogram includes only fixes with |turn angle| ≥ ${escapeHtml(formatColorValue(this.gpsSpikeTurnAngleDeg, "numeric"))}°.</div>
      `
      : "";

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
    } else if (disabledReason) {
      body = `
        <div class="movement-threshold-empty">
          ${escapeHtml(disabledReason)}
        </div>
      `;
    } else if (field.kind !== "numeric") {
      const subtitle = `${formatCount(levelOptions.length)} levels in the visible fixes`;
      const meta = selectedLevels.length
        ? `${formatCount(matchCount)} fixes match ${formatCount(selectedLevels.length)} selected levels.`
        : "Choose one or more levels to highlight matching fixes.";
      const selectionNote = uncheckedCount > 0
        ? `${formatCount(uncheckedCount)} matching fixes can optionally be added to the checked-fix list.`
        : matchCount > 0
          ? "All matching fixes are already in the checked-fix list."
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
        <div class="movement-threshold-note">${escapeHtml(selectionNote)} The main flag action applies the selected-level filter directly.</div>
        <div class="movement-threshold-actions">
          <button
            type="button"
            class="movement-emphasis"
            data-action="check-above-threshold"
            ${uncheckedCount === 0 ? "disabled" : ""}
          >Add matches to checked fixes${uncheckedCount > 0 ? ` (${escapeHtml(formatCount(uncheckedCount))})` : ""}</button>
          <button
            type="button"
            data-action="clear-threshold"
            ${selectedLevels.length === 0 ? "disabled" : ""}
          >Clear selection</button>
        </div>
      `;
    } else if (!numericCount || !histogram) {
      body = `
        ${gpsSpikeControl}
        <div class="movement-threshold-empty">
          ${gpsSpikeMode
            ? "No visible fixes meet the current turn-angle requirement."
            : "The current numeric field has no usable values in the visible scope."}
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
      const subtitle = gpsSpikeMode
        ? `${formatCount(numericCount)} sharp-turn fixes in the visible scope`
        : `${formatCount(numericCount)} visible fixes`;
      const thresholdPrompt = thresholdValue === null
        ? `Click the histogram or type a ${reverse ? "lower-tail" : "upper-tail"} threshold`
        : "Threshold";
      const selectionNote = thresholdValue === null
        ? "No threshold set"
        : matchCount === 0
          ? "No matches"
          : uncheckedCount > 0
            ? `${formatCount(matchCount)} matches • ${formatCount(uncheckedCount)} not in checked fixes`
            : `${formatCount(matchCount)} matches • all in checked fixes`;
      body = `
        ${gpsSpikeControl}
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
        ${gpsSpikeMode ? "" : `<label class="movement-threshold-toggle">
          <input
            type="checkbox"
            data-action="toggle-threshold-reverse"
            ${reverse ? "checked" : ""}
          >
          Reverse threshold: highlight values below the line
        </label>`}
        <div class="movement-threshold-note">${escapeHtml(selectionNote)}</div>
        <div class="movement-threshold-actions">
          <button
            type="button"
            class="movement-emphasis"
            data-action="check-above-threshold"
            ${uncheckedCount === 0 ? "disabled" : ""}
          >Add matches to checked fixes</button>
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
    if (target.closest('input[data-action="set-histogram-min"], input[data-action="set-histogram-max"], input[data-action="set-threshold-value"], input[data-action="set-gps-spike-turn-angle"]')) {
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
        this.syncFlagTargetToThreshold();
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
    this.syncFlagTargetToThreshold();
  }

  handleThresholdPaneChange(event) {
    if (!this.data) {
      return;
    }
    const target = event.target instanceof Element ? event.target : null;
    if (!target) {
      return;
    }
    const turnAngleInput = target.closest('input[data-action="set-gps-spike-turn-angle"]');
    if (turnAngleInput) {
      this.thresholdInputPendingBlur = false;
      const turnAngle = finiteOrNull(String(turnAngleInput.value || "").trim());
      if (turnAngle === null || turnAngle < 0 || turnAngle > 180) {
        this.setStatus("Turn angle must be between 0° and 180°.", true);
        this.renderThresholdPane();
        return;
      }
      this.gpsSpikeTurnAngleDeg = turnAngle;
      this.renderThresholdPane();
      this.renderLayers();
      this.syncFlagTargetToThreshold();
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
      this.syncFlagTargetToThreshold();
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
      this.syncFlagTargetToThreshold();
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
    this.syncFlagTargetToThreshold();
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
    this.flagTargetKind = "fixes";
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

  async loadConfirmedFixes() {
    if (!this.data || !this.currentArtifact || this.data.confirmedState === "loading") {
      return;
    }
    this.data.confirmedState = "loading";
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    try {
      const controller = this.beginRequest("confirmed");
      const payload = await this.fetchJSON(
        this.buildFixesRequestUrl({
          familyName,
          studyName,
          datasetId,
          artifactName,
          reviewStatus: "confirmed",
        }),
        { signal: controller.signal },
      );
      if (
        this.requestControllers.confirmed !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
        || !this.data
      ) {
        return;
      }
      const confirmedFixes = parseMovementFixes(payload.fixes || [])
        .filter(fix => fix.review.status === "confirmed")
        .map(fix => ({ ...fix, analyticallyExcluded: true }));
      this.data.confirmedFixes = confirmedFixes;
      this.data.confirmedState = "loaded";
      this.data.confirmedLimit = payload.detail_scope?.limit ?? null;
      this.data.confirmedMatchingFixCount = Number(payload.matching_fix_count) || 0;
      this.data.confirmedReturnedFixCount = Number(payload.returned_fix_count) || confirmedFixes.length;
      this.data.confirmedTruncated = Boolean(payload.truncated);
      refreshMovementFixCollections(this.data);
      this.renderLegend();
      this.renderLayers();
      this.updateActionButtons();
      if (this.data.confirmedTruncated) {
        this.setStatus(
          `Loaded ${formatCount(confirmedFixes.length)} of ${formatCount(this.data.confirmedMatchingFixCount)} confirmed exclusions for the audit layer.`,
          true,
        );
      }
    } catch (error) {
      if (this.isAbortError(error) || !this.data) {
        return;
      }
      this.data.confirmedState = "error";
      this.setStatus(`Confirmed exclusions could not be loaded: ${error.message}`, true);
    }
  }

  async loadSuspiciousFixes({ focus = true } = {}) {
    if (!this.data || !this.currentArtifact) {
      return;
    }
    if (focus) {
      this.cancelRequest("detail");
    }
    this.data.suspiciousState = "loading";
    this.refs.selectSuspicious.textContent = focus
      ? "Loading suspicious fixes..."
      : "Loading suspicious overlay...";
    this.updateActionButtons();
    if (focus) {
      this.setStatus(`Loading suspicious fixes from ${this.currentArtifact}...`);
    }

    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const artifactName = this.currentArtifact;
    try {
      const controller = this.beginRequest("suspicious");
      const payload = await this.fetchJSON(
        this.buildFixesRequestUrl({
          familyName,
          studyName,
          datasetId,
          artifactName,
          reviewStatus: "suspected",
        }),
        { signal: controller.signal },
      );
      if (
        this.requestControllers.suspicious !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || artifactName !== this.currentArtifact
        || !this.data
      ) {
        return;
      }

      const suspiciousFixes = parseMovementFixes(payload.fixes || [])
        .filter(fix => fix.review.status === "suspected");
      this.data.suspiciousFixes = suspiciousFixes;
      this.data.suspiciousState = "loaded";
      this.data.suspiciousLimit = payload.detail_scope?.limit ?? null;
      this.data.suspiciousMatchingFixCount = Number(payload.matching_fix_count) || 0;
      this.data.suspiciousReturnedFixCount = Number(payload.returned_fix_count) || suspiciousFixes.length;
      this.data.suspiciousTruncated = Boolean(payload.truncated);
      refreshMovementFixCollections(this.data);
      if (focus) {
        this.clearThresholdState();
        this.setTableSelection();
        this.data.selectedFixKeys = new Set(suspiciousFixes.map(fix => fix.fixKey));
        this.flagTargetKind = "fixes";
        this.data.selectedIndividuals = new Set(suspiciousFixes.map(fix => fix.individual));
        this.refs.showTrain.checked = true;
        this.refs.showTest.checked = true;
        this.refs.showPoints.checked = true;
        this.saveUiState();
      }
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLegend();
      this.renderLayers();
      this.updateActionButtons();

      if (!suspiciousFixes.length) {
        if (focus) {
          this.setStatus(`No unresolved suspicious fixes were found in ${artifactName}.`);
        }
        return;
      }
      if (focus) {
        this.zoomToPath(suspiciousFixes.map(fix => fix.position));
      }
      if (this.data.suspiciousTruncated) {
        this.setStatus(
          `Loaded ${formatCount(suspiciousFixes.length)} of ${formatCount(this.data.suspiciousMatchingFixCount)} suspicious fixes due to the ${formatCount(this.data.suspiciousLimit)}-fix cap.`,
          true,
        );
      } else if (focus) {
        this.setStatus(
          `Focused ${formatCount(suspiciousFixes.length)} suspicious fixes across ${formatCount(this.data.selectedIndividuals.size)} individuals.`,
        );
      }
    } catch (error) {
      if (this.isAbortError(error) || !this.data) {
        return;
      }
      this.data.suspiciousState = "error";
      this.setStatus(`Suspicious fixes could not be loaded: ${error.message}`, true);
      this.updateActionButtons();
    } finally {
      if (this.data && this.data.suspiciousState !== "loading") {
        const count = Number(this.data.suspiciousMatchingFixCount) || this.data.suspiciousFixes.length;
        this.refs.selectSuspicious.textContent = count
          ? `Review suspicious fixes (${formatCount(count)})`
          : "Review suspicious fixes";
      }
    }
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
    const wholeRdsStudySelected = (
      MOVEMENT_APP_CONFIG.rdsSource
      && selectedIndividuals.length > 0
      && selectedIndividuals.length === this.data.individuals.length
    );
    if (wholeRdsStudySelected && !this.data.binaryMapReady) {
      this.cancelRequest("detail");
      this.data.detailState = "loading";
      this.data.detailIndividuals = [...selectedIndividuals];
      this.updateActionButtons();
      this.setStatus(
        `Loading all ${formatCount(this.data.totalRows)} indexed fixes across ${formatCount(selectedIndividuals.length)} individuals...`,
      );
      try {
        await this.loadBinaryMovement({
          familyName: this.currentFamily,
          studyName: this.currentStudy,
          datasetId: this.currentDatasetId,
        });
      } catch (error) {
        if (this.isAbortError(error) || !this.data) {
          return;
        }
        this.data.detailState = "error";
        this.updateActionButtons();
        this.setStatus(`Could not load all indexed fixes: ${error.message}`, true);
        return;
      }
      const currentSelection = this.getSelectedIndividuals();
      if (
        !this.data
        || !this.data.binaryMapReady
        || currentSelection.length !== this.data.individuals.length
      ) {
        return;
      }
    }
    if (this.data.binaryMapReady && selectedIndividuals.length > 1) {
      this.cancelRequest("detail");
      this.data.detailState = "idle";
      this.data.detailIndividuals = [];
      this.data.detailFixes = [];
      this.data.detailSegments = [];
      this.data.detailAutoBursts = [];
      this.data.detailMatchingFixCount = 0;
      this.data.detailReturnedFixCount = 0;
      this.data.detailTruncated = false;
      refreshMovementFixCollections(this.data);
      this.renderBurstCountIndicator();
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      this.setStatus(`Showing all indexed fixes for ${formatCount(selectedIndividuals.length)} individuals. Select one individual to load its editable table and burst details.`);
      return;
    }
    if (!selectedIndividuals.length) {
      this.cancelRequest("detail");
      this.data.detailState = "idle";
      this.data.detailIndividuals = [];
      this.data.detailLimit = null;
      this.data.detailMatchingFixCount = 0;
      this.data.detailReturnedFixCount = 0;
      this.data.detailTruncated = false;
      this.data.detailFixes = [];
      this.data.detailSegments = [];
      this.data.detailAutoBursts = [];
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set();
      this.renderIndividuals();
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
      this.data.detailSegments = [];
      this.data.detailAutoBursts = [];
      this.data.detailMatchingFixCount = this.getFixesForIndividualsFrom(this.data.overviewFixes, selectedIndividuals).length;
      this.data.detailReturnedFixCount = this.data.detailMatchingFixCount;
      this.data.detailTruncated = false;
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(preserved, this.data.detailIndividuals);
      this.renderIndividuals();
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
    this.renderBurstCountIndicator();
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
      const payloadIndividuals = Array.isArray(payload.detail_scope?.individuals)
        ? payload.detail_scope.individuals.map(value => String(value)).filter(Boolean)
        : [];
      this.data.detailIndividuals = payloadIndividuals.length ? payloadIndividuals : [...selectedIndividuals];
      this.data.detailLimit = payload.detail_scope?.limit ?? null;
      this.data.detailMatchingFixCount = Number(payload.matching_fix_count) || 0;
      this.data.detailReturnedFixCount = Number(payload.returned_fix_count) || 0;
      this.data.detailTruncated = Boolean(payload.truncated);
      this.data.detailFixes = parseMovementFixes(payload.fixes || []);
      this.data.detailSegments = parseMovementSegments(payload.segments || []);
      this.data.detailAutoBursts = parseMovementAutoBursts(payload.auto_bursts || []);
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = this.filterSelectedFixKeysForIndividuals(preserved, this.data.detailIndividuals);
      this.renderIndividuals();
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
      this.data.detailSegments = [];
      this.data.detailAutoBursts = [];
      refreshMovementFixCollections(this.data);
      this.data.selectedFixKeys = new Set();
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLayers();
      this.updateActionButtons();
      this.setStatus(`Overview loaded, but editable fixes for ${formatCount(selectedIndividuals.length)} visible individuals failed: ${error.message}`, true);
    }
  }

  buildFixesRequestUrl({
    familyName,
    studyName,
    datasetId,
    artifactName,
    individuals = [],
    reviewStatus = "",
    limit,
    data = this.data,
  } = {}) {
    const params = new URLSearchParams({ logical_name: artifactName });
    params.set("burst_gap_mode", this.getBurstGapMode());
    params.set("burst_gap_seconds", String(this.getBurstGapSeconds()));
    params.set("burst_gap_quantile", String(this.getBurstGapQuantile()));
    const effectiveBurstGapSeconds = finiteOrNull(data?.burstGap?.effectiveSeconds);
    if (effectiveBurstGapSeconds !== null && effectiveBurstGapSeconds > 0) {
      params.set("burst_gap_effective_seconds", String(effectiveBurstGapSeconds));
    }
    const normalizedIndividuals = uniqueNonEmpty(individuals).sort((left, right) => left.localeCompare(right));
    const allIndividuals = data ? [...data.individuals].sort((left, right) => left.localeCompare(right)) : [];
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
    return this.buildReportSnapshotWindows(
      this.getReportFixes(),
      this.refs.reportSnapshotUnit.value || "burst",
    );
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

  getSelectedReportBasemapPreset() {
    const choice = this.refs.reportBasemap.value || "current";
    if (choice === "current") {
      const currentName = this.refs.basemap.value || "Blank";
      if (currentName !== "Blank" && BASEMAP_PRESETS[currentName]) {
        return BASEMAP_PRESETS[currentName];
      }
      return BASEMAP_PRESETS.Positron;
    }
    return BASEMAP_PRESETS[choice] || BASEMAP_PRESETS.Positron;
  }

  updateReportModeUi() {
    const reportType = this.getReportType();
    const reportIndividuals = this.getReportIndividuals();
    const showIssueFirstControls = reportType === "issue_first";
    this.refs.reportOutputModeWrap.hidden = reportType !== "individual_profile" || reportIndividuals.length <= 1;
    this.refs.reportScreenshotModeWrap.hidden = !showIssueFirstControls;
    this.refs.reportSnapshotUnitWrap.hidden = !showIssueFirstControls;
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

  serializeSnapshotWindowForReport(window) {
    return {
      snapshot_key: window.snapshotKey,
      snapshot_kind: window.snapshotKind || "context",
      burst_id: window.burstId || "",
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

  buildReportSnapshotWindows(reportFixes, snapshotUnit = "burst") {
    if (snapshotUnit !== "burst") {
      return this.buildMergedReportSnapshotWindows(reportFixes);
    }
    const burstWindows = this.buildBurstReportSnapshotWindows(reportFixes);
    const coveredFixKeys = new Set(
      burstWindows.flatMap(window => window.reportFixKeys || []),
    );
    const uncoveredFixes = reportFixes.filter(fix => !coveredFixKeys.has(fix.fixKey));
    const fallbackWindows = uncoveredFixes.length
      ? this.buildMergedReportSnapshotWindows(uncoveredFixes)
      : [];
    return [...burstWindows, ...fallbackWindows].map((window, index) => ({
      ...window,
      snapshotKey: `snapshot_${String(index + 1).padStart(2, "0")}`,
    }));
  }

  buildBurstReportSnapshotWindows(reportFixes) {
    if (!this.data || !reportFixes.length) {
      return [];
    }
    const groups = new Map();
    for (const fix of reportFixes) {
      const issues = Array.isArray(fix.review?.issues) ? fix.review.issues : [];
      for (const issue of issues) {
        if (
          issue.status !== "suspected"
          || issue.scopeKind !== "burst"
          || !issue.issueId
        ) {
          continue;
        }
        const issueType = issue.issueType || "Unspecified issue";
        const burstIdentity = issue.scopeBurstId || issue.issueId;
        const groupKey = `${burstIdentity}\u0000${issueType}`;
        const group = groups.get(groupKey) || {
          issueId: issue.issueId,
          issueType,
          burstId: issue.scopeBurstId || "",
          individual: fix.individual,
          setName: fix.setName,
          fixes: new Map(),
        };
        group.fixes.set(fix.fixKey, fix);
        groups.set(groupKey, group);
      }
    }
    if (!groups.size) {
      return [];
    }

    const scope = this.refs.reportScope.value || "visible";
    const trackFixesSource = this.getFixesForScope(scope, {
      allowPartialFull: scope === "full",
    });
    const trackFixesByKey = new Map();
    const indexByFixKey = new Map();
    for (const fix of trackFixesSource) {
      const trackKey = reportTrackKey(fix.individual, fix.setName);
      const track = trackFixesByKey.get(trackKey) || [];
      track.push(fix);
      trackFixesByKey.set(trackKey, track);
    }
    for (const [trackKey, track] of trackFixesByKey.entries()) {
      track.sort((left, right) => (
        left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey)
      ));
      track.forEach((fix, index) => {
        indexByFixKey.set(fix.fixKey, { trackKey, index });
      });
    }

    const currentField = this.getCurrentColorField();
    const thresholdFieldKey = this.thresholdState.fieldKey || "";
    const thresholdValue = typeof this.thresholdState.value === "number"
      ? this.thresholdState.value
      : null;
    const thresholdReverse = this.thresholdState.reverse === true;
    const allReportFixMap = new Map(reportFixes.map(fix => [fix.fixKey, fix]));
    const orderedGroups = [...groups.values()].sort((left, right) => {
      const leftTime = Math.min(...[...left.fixes.values()].map(fix => fix.timeMs));
      const rightTime = Math.min(...[...right.fixes.values()].map(fix => fix.timeMs));
      return (
        left.individual.localeCompare(right.individual)
        || left.setName.localeCompare(right.setName)
        || leftTime - rightTime
        || left.issueId.localeCompare(right.issueId)
      );
    });

    const windows = [];
    for (const group of orderedGroups) {
      const focalFixes = [...group.fixes.values()].sort((left, right) => (
        left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey)
      ));
      const located = focalFixes
        .map(fix => indexByFixKey.get(fix.fixKey))
        .filter(Boolean);
      if (!located.length) {
        continue;
      }
      const trackKey = located[0].trackKey;
      const trackFixes = trackFixesByKey.get(trackKey) || focalFixes;
      const indices = located
        .filter(item => item.trackKey === trackKey)
        .map(item => item.index);
      const startIndex = Math.max(0, Math.min(...indices) - 8);
      const endIndex = Math.min(trackFixes.length - 1, Math.max(...indices) + 8);
      const windowFixes = trackFixes.slice(startIndex, endIndex + 1);
      const focalFixKeys = focalFixes.map(fix => fix.fixKey);
      const focalKeySet = new Set(focalFixKeys);
      const secondaryFixKeys = windowFixes
        .filter(fix => allReportFixMap.has(fix.fixKey) && !focalKeySet.has(fix.fixKey))
        .map(fix => fix.fixKey);
      const firstFix = focalFixes[0];
      const lastFix = focalFixes[focalFixes.length - 1];
      const burstLabel = group.burstId || group.issueId;
      windows.push({
        snapshotKey: "",
        snapshotKind: "burst",
        burstId: group.burstId,
        caption: `${group.issueType} | ${group.individual} | ${burstLabel} | ${formatTimestamp(firstFix.timeMs)} to ${formatTimestamp(lastFix.timeMs)}`,
        individual: group.individual,
        setName: group.setName,
        issueType: group.issueType,
        issueTypes: [group.issueType],
        anchorFixKeys: focalFixKeys,
        secondaryFixKeys,
        reportFixKeys: focalFixKeys,
        startFixKey: firstFix.fixKey,
        endFixKey: lastFix.fixKey,
        startTimeMs: firstFix.timeMs,
        endTimeMs: lastFix.timeMs,
        startTimeText: formatTimestamp(firstFix.timeMs),
        endTimeText: formatTimestamp(lastFix.timeMs),
        windowFixCount: windowFixes.length,
        windowFixes,
        reportWindowFixes: focalFixes,
        showGrid: true,
        sampleValue: deriveReportSampleValue(
          focalFixes,
          currentField,
          this.data.colorFieldByKey,
          {
            thresholdFieldKey,
            thresholdValue,
            thresholdReverse,
          },
        ),
      });
    }
    return windows;
  }

  buildMergedReportSnapshotWindows(reportFixes) {
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
          showGrid: true,
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
    const snapshotUnit = this.refs.reportSnapshotUnit.value || "burst";
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
      <div><strong>Snapshot unit:</strong> ${escapeHtml(snapshotUnit === "burst" ? "One per flagged burst" : "Merged nearby flagged fixes")}</div>
      <div><strong>Suspected fixes in report:</strong> ${escapeHtml(formatCount(reportFixes.length))}</div>
      <div><strong>Snapshot windows:</strong> ${escapeHtml(formatCount(snapshotWindows.length))}</div>
      <div><strong>Auto snapshots to render:</strong> ${escapeHtml(formatCount(effectiveSnapshotWindows.length))}</div>
      <div><strong>Snapshot plan:</strong> ${escapeHtml(snapshotNote)}</div>
      <div><strong>Issue types in report:</strong> ${escapeHtml(issueTypes.length ? issueTypes.join(", ") : "Unspecified issue")}</div>
      <div><strong>Scope note:</strong> ${escapeHtml(scopeNote)}</div>
    `;
    this.refs.reportSubmit.disabled = reportFixes.length === 0;
  }

  getActiveFlagTarget() {
    if (!this.data) {
      return { kind: "none", fixes: [], ready: false };
    }
    if (this.flagTargetKind === "individual") {
      const individual = String(this.manualFlagTarget.individual || "");
      return {
        kind: "individual",
        individual,
        fixes: [],
        ready: Boolean(individual && this.data.individuals.includes(individual)),
      };
    }
    if (this.flagTargetKind === "bursts") {
      const bursts = this.flagTargetBursts();
      return {
        kind: "bursts",
        bursts,
        fixes: [],
        ready: bursts.length > 0,
      };
    }
    if (this.flagTargetKind === "segment") {
      const selection = this.getCurrentSegmentSelection();
      return {
        kind: "segment",
        fixes: selection?.fixes || [],
        selection,
        ready: Boolean(selection && selection.fixes.length >= 2),
      };
    }
    if (this.flagTargetKind === "filter") {
      const matchKeys = this.getActiveThresholdMatchKeys();
      const thresholdContext = this.getThresholdContext();
      const fixes = [...matchKeys]
        .map(fixKey => this.data.fixByKey.get(fixKey))
        .filter(Boolean);
      if (fixes.length || matchKeys.size) {
        return {
          kind: "filter",
          filterKind: this.getCurrentColorField()?.key === GPS_SPIKE_COLOR_FIELD_KEY
            ? "gps_spike"
            : "threshold",
          fixes,
          matchCount: Number(thresholdContext?.matchCount) || matchKeys.size,
          ready: fixes.length > 0,
        };
      }
    }
    if (this.flagTargetKind === "none") {
      return { kind: "none", fixes: [], ready: false };
    }
    const fixes = this.getSelectedFixes();
    return { kind: "fixes", fixes, ready: fixes.length > 0 };
  }

  buildIssueWorkflowContext(scopeKind, { selectionMethods = null } = {}) {
    const methods = Array.isArray(selectionMethods)
      ? selectionMethods
      : [...(this.manualFlagTarget.selectionMethods || [])];
    return {
      entry_point: this.individualReviewQueue.mode === "queue"
        ? "individual_review_queue"
        : "movement_map",
      active_individual: this.individualReviewQueue.mode === "queue"
        ? String(this.individualReviewQueue.activeIndividual || "")
        : "",
      scope_kind: String(scopeKind || ""),
      selection_methods: [...new Set(methods.map(String).filter(Boolean))],
    };
  }

  updateActionButtons() {
    const hasData = Boolean(this.data);
    const canPersistEdits = this.canPersistEdits();
    const hasSelectedIndividuals = this.getSelectedIndividuals().length > 0;
    const hasDetail = this.hasLoadedDetailSelection();
    const selectedFixes = this.getSelectedFixes();
    const selectedCount = selectedFixes.length;
    const flagTarget = this.getActiveFlagTarget();
    const flagFixes = flagTarget.fixes || [];
    const loadedSuspiciousKeys = new Set(
      (this.data?.suspiciousFixes || []).map(fix => fix.fixKey),
    );
    const flagFixesAreLoadedSuspicious = (
      flagFixes.length > 0
      && flagFixes.every(fix => loadedSuspiciousKeys.has(fix.fixKey))
    );
    const canEditFlagTarget = (
      hasDetail
      || flagFixesAreLoadedSuspicious
      || ["individual", "bursts"].includes(flagTarget.kind)
    );
    const canConfirmSelectedFixes = this.canConfirmFixes(selectedFixes);
    const suspiciousLoading = this.data?.suspiciousState === "loading";
    const candidatePreviewLoading = this.candidateQueryPreview?.status === "loading";
    const anomalyRankingLoading = ["checking", "restoring", "loading"].includes(
      this.anomalyRanking?.status,
    );
    const burstFeatureSpaceLoading = this.burstFeatureSpace?.status === "loading";
    const returnedCandidateCount = this.getCandidateQueryReturnedMatchKeys().size;
    const selectedCandidateQuery = this.getSelectedCandidateQuery();
    for (const button of [
      this.refs.selectSuspicious,
      this.refs.clearFixes,
      this.refs.dismissSuspected,
      this.refs.runCandidateQuery,
      this.refs.checkCandidates,
      this.refs.clearCandidates,
      this.refs.markSuspected,
      this.refs.markConfirmed,
      this.refs.runAnomalyRanking,
      this.refs.runBurstFeatureSpace,
      this.refs.generateReport,
      this.refs.exportReviewedCsv,
    ]) {
      button.hidden = !hasData;
    }
    if (this.refs.anomalyFeatureSetControl) {
      this.refs.anomalyFeatureSetControl.hidden = !hasData;
    }
    if (MOVEMENT_APP_CONFIG.mode === "slim_movement") {
      for (const element of [
        this.refs.runCandidateQuery,
        this.refs.checkCandidates,
        this.refs.clearCandidates,
        this.refs.anomalyFeatureSetControl,
        this.refs.runBurstFeatureSpace,
      ]) {
        element?.classList.add("movement-profile-hidden");
      }
    }
    this.refs.markSuspected.textContent = flagTarget.kind === "individual"
      ? `Flag entire individual ${flagTarget.individual}`
      : flagTarget.kind === "bursts"
        ? `Flag ${formatCount(flagTarget.bursts.length)} selected burst(s)`
      : flagTarget.kind === "segment"
      ? flagTarget.ready
        ? `Flag selected segment (${formatCount(flagFixes.length)} fixes)`
        : "Select segment end"
      : flagTarget.kind === "filter"
        ? flagTarget.filterKind === "gps_spike"
          ? `Flag ${formatCount(flagFixes.length)} GPS-spike matches`
          : `Flag threshold matches (${formatCount(flagFixes.length)})`
        : flagTarget.kind === "fixes"
          ? `Flag checked fixes (${formatCount(flagFixes.length)})`
          : "Choose what to flag";
    this.refs.markSuspected.disabled = (
      !canPersistEdits
      || !hasData
      || !hasSelectedIndividuals
      || !canEditFlagTarget
      || !flagTarget.ready
    );
    this.refs.markConfirmed.disabled = (
      !canPersistEdits
      || !hasData
      || !canConfirmSelectedFixes
    );
    this.refs.dismissSuspected.disabled = (
      !canPersistEdits
      || !hasData
      || this.getUnresolvedSuspectedIssueGroups(selectedFixes).length === 0
    );
    this.refs.generateReport.disabled = !hasData || !(this.data?.individuals || []).length;
    this.refs.exportReviewedCsv.disabled = !hasData;
    this.refs.selectSuspicious.disabled = !hasData || !this.currentArtifact || suspiciousLoading;
    this.refs.clearFixes.disabled = !hasData || selectedCount === 0;
    this.refs.runCandidateQuery.disabled = !canPersistEdits || !hasData || !this.currentArtifact || candidatePreviewLoading || !selectedCandidateQuery;
    this.refs.runAnomalyRanking.disabled = !hasData || !this.currentArtifact || anomalyRankingLoading || burstFeatureSpaceLoading;
    this.refs.runBurstFeatureSpace.disabled = !hasData || !this.currentArtifact || burstFeatureSpaceLoading || anomalyRankingLoading;
    if (this.refs.anomalyFeatureSet) {
      this.refs.anomalyFeatureSet.disabled = !hasData || anomalyRankingLoading || burstFeatureSpaceLoading;
    }
    this.refs.checkCandidates.disabled = !hasData || candidatePreviewLoading || returnedCandidateCount === 0;
    this.refs.clearCandidates.disabled = !hasData || candidatePreviewLoading || this.candidateQueryPreview?.status === "idle";
    this.updateUndoButton();
    if (this.refs?.sideSheetTabs?.dataset.activeSheet === "table") {
      this.renderTableSheet();
    }
  }

  updateUndoButton() {
    const currentHeadDatasetId = this.graph?.current_dataset_id || "";
    const selectedIsCurrentHead = Boolean(currentHeadDatasetId) && this.currentDatasetId === currentHeadDatasetId;
    const workflowAllowsUndo = !this.editLockProfile?.capabilities
      || this.editLockProfile.capabilities.can_undo === true;
    const canUndo = selectedIsCurrentHead
      && Boolean(this.currentDataset?.parent_dataset_id)
      && workflowAllowsUndo;
    this.refs.undo.disabled = !canUndo;
  }

  getUnresolvedSuspectedIssueGroups(fixes = this.getSelectedFixes()) {
    const groups = new Map();
    for (const fix of fixes || []) {
      if (!fix?.fixKey) {
        continue;
      }
      const issues = Array.isArray(fix.review?.effectiveIssues) && fix.review.effectiveIssues.length
        ? fix.review.effectiveIssues
        : Array.isArray(fix.review?.issues) ? fix.review.issues : [];
      for (const issue of issues) {
        const parentIssueId = issue.parentIssueId || issue.issueId;
        if (issue.status !== "suspected" || !parentIssueId) {
          continue;
        }
        let group = groups.get(parentIssueId);
        if (!group) {
          group = {
            parentAnnotationId: parentIssueId,
            issueType: issue.issueType || "Unspecified issue",
            origin: issue.origin || (issue.issueField || issue.issueThreshold ? "threshold" : "manual"),
            stepId: issue.stepId || "",
            sourceAnalysisId: issue.sourceAnalysisId || "",
            reviewedAt: issue.reviewedAt || "",
            reviewUser: issue.reviewUser || "",
            fixes: [],
          };
          groups.set(parentIssueId, group);
        }
        if (!group.fixes.some(item => item.fixKey === fix.fixKey)) {
          group.fixes.push(fix);
        }
      }
    }
    return [...groups.values()].sort((left, right) => (
      left.issueType.localeCompare(right.issueType)
      || left.parentAnnotationId.localeCompare(right.parentAnnotationId)
    ));
  }

  canConfirmFixes(fixes = this.getSelectedFixes()) {
    const selected = Array.isArray(fixes) ? fixes.filter(Boolean) : [];
    if (!selected.length) {
      return false;
    }
    const coveredFixKeys = new Set(
      this.getUnresolvedSuspectedIssueGroups(selected)
        .flatMap(group => group.fixes.map(fix => fix.fixKey)),
    );
    return selected.every(fix => coveredFixKeys.has(fix.fixKey));
  }

  openConfirmModal(fixes = this.getSelectedFixes()) {
    if (this.rejectLockedEdit()) {
      return;
    }
    const selectedFixes = Array.isArray(fixes) ? fixes.filter(Boolean) : [];
    const groups = this.getUnresolvedSuspectedIssueGroups(selectedFixes);
    if (!selectedFixes.length || !groups.length || !this.canConfirmFixes(selectedFixes)) {
      this.setStatus("Select fixes with unresolved suspected issues before confirming them.", true);
      return;
    }
    this.pendingConfirmationGroups = groups;
    const checkByDefault = groups.length === 1;
    this.refs.confirmMeta.innerHTML = `
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Selected fixes:</strong> ${escapeHtml(formatCount(selectedFixes.length))}</div>
      <div><strong>Originating issues:</strong> ${escapeHtml(formatCount(groups.length))}</div>
    `;
    this.refs.confirmGroups.innerHTML = groups.map((group, index) => {
      const provenance = [
        group.origin,
        group.stepId ? `step ${group.stepId}` : "",
        group.sourceAnalysisId ? `analysis ${group.sourceAnalysisId}` : "",
        group.reviewedAt ? formatTimestamp(Date.parse(group.reviewedAt)) : "",
      ].filter(Boolean).join(" • ");
      return `
        <label class="movement-inline-check">
          <input type="checkbox" data-confirm-group-index="${index}"${checkByDefault ? " checked" : ""}>
          <span>
            <strong>${escapeHtml(group.issueType)}</strong><br>
            ${escapeHtml(formatCount(group.fixes.length))} selected fix(es)<br>
            <span class="movement-subtle">${escapeHtml(provenance || group.parentAnnotationId)}</span>
          </span>
        </label>
      `;
    }).join("");
    this.refs.confirmUser.value = this.getUser();
    this.refs.confirmNote.value = "";
    this.refs.confirmStatus.textContent = groups.length > 1
      ? "Choose the originating issue groups to confirm."
      : "";
    this.refs.confirmStatus.classList.remove("error");
    this.refs.confirmSubmit.disabled = false;
    this.refs.confirmClose.disabled = false;
    this.refs.confirmModal.classList.remove("hidden");
    this.refs.confirmUser.focus();
  }

  async submitConfirmIssues() {
    if (!this.pendingConfirmationGroups.length || !this.currentArtifact) {
      return;
    }
    if (this.rejectLockedEdit()) {
      this.refs.confirmModal.classList.add("hidden");
      return;
    }
    const selectedGroups = [...this.refs.confirmGroups.querySelectorAll("input[data-confirm-group-index]:checked")]
      .map(input => this.pendingConfirmationGroups[Number(input.dataset.confirmGroupIndex)])
      .filter(Boolean);
    const user = this.refs.confirmUser.value.trim();
    if (!selectedGroups.length) {
      this.refs.confirmStatus.textContent = "Choose at least one originating issue group.";
      this.refs.confirmStatus.classList.add("error");
      return;
    }
    if (!user) {
      this.refs.confirmStatus.textContent = "User is required.";
      this.refs.confirmStatus.classList.add("error");
      return;
    }
    if (this.hasUnsavedIndividualReviewDrafts()) {
      this.refs.confirmStatus.textContent = "Save or discard the unsaved individual decision before confirming issues.";
      this.refs.confirmStatus.classList.add("error");
      return;
    }
    this.refs.confirmSubmit.disabled = true;
    this.refs.confirmClose.disabled = true;
    this.refs.confirmStatus.textContent = `Confirming ${formatCount(selectedGroups.length)} issue group(s)...`;
    this.refs.confirmStatus.classList.remove("error");
    this.setStatus(this.refs.confirmStatus.textContent);
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/confirm-issues`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            expected_current_dataset_id: this.expectedCurrentDatasetId(),
            expected_review_revision: this.expectedReviewRevision(),
            logical_name: this.currentArtifact,
            source_bundle_signature: this.data?.sourceSignature || "",
            confirmations: selectedGroups.map(group => ({
              parent_annotation_id: group.parentAnnotationId,
              fix_keys: group.fixes.map(fix => fix.fixKey),
            })),
            note: this.refs.confirmNote.value.trim(),
            user,
          }),
        },
      );
      this.setUser(user);
      this.pendingConfirmationGroups = [];
      this.refs.confirmModal.classList.add("hidden");
      await this.loadStudyAtDataset(result.dataset.dataset_id, {
        result,
        clearTarget: "fixes",
      });
      this.setStatus(`Confirmed suspected outliers in ${result.dataset.dataset_id}.`);
    } catch (error) {
      await this.handleEditRequestError(error);
      this.refs.confirmStatus.textContent = error.message;
      this.refs.confirmStatus.classList.add("error");
      this.refs.confirmSubmit.disabled = false;
      this.refs.confirmClose.disabled = false;
      this.setStatus(error.message, true);
    }
  }

  openDismissModal(fixes = this.getSelectedFixes()) {
    if (this.rejectLockedEdit()) {
      return;
    }
    const selectedFixes = Array.isArray(fixes) ? fixes.filter(Boolean) : [];
    const groups = this.getUnresolvedSuspectedIssueGroups(selectedFixes);
    if (!selectedFixes.length || !groups.length) {
      this.setStatus("Select fixes with unresolved suspected issues before dismissing them.", true);
      return;
    }
    this.pendingDismissalGroups = groups;
    this.refs.dismissMeta.innerHTML = `
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Selected fixes:</strong> ${escapeHtml(formatCount(selectedFixes.length))}</div>
      <div><strong>Originating suspicions:</strong> ${escapeHtml(formatCount(groups.length))}</div>
    `;
    this.refs.dismissGroups.innerHTML = groups.map((group, index) => {
      const provenance = [
        group.origin,
        group.stepId ? `step ${group.stepId}` : "",
        group.sourceAnalysisId ? `analysis ${group.sourceAnalysisId}` : "",
      ].filter(Boolean).join(" • ");
      return `
        <label class="movement-inline-check">
          <input type="checkbox" data-dismiss-group-index="${index}" checked>
          <span>
            <strong>${escapeHtml(group.issueType)}</strong><br>
            ${escapeHtml(formatCount(group.fixes.length))} selected fix(es)<br>
            <span class="movement-subtle">${escapeHtml(provenance || group.parentAnnotationId)}</span>
          </span>
        </label>
      `;
    }).join("");
    this.refs.dismissUser.value = this.getUser();
    this.refs.dismissNote.value = "";
    this.refs.dismissStatus.textContent = "";
    this.refs.dismissStatus.classList.remove("error");
    this.refs.dismissSubmit.disabled = false;
    this.refs.dismissClose.disabled = false;
    this.refs.dismissModal.classList.remove("hidden");
  }

  async submitDismissIssues() {
    if (this.rejectLockedEdit()) {
      this.refs.dismissModal.classList.add("hidden");
      return;
    }
    const selectedGroups = [...this.refs.dismissGroups.querySelectorAll("input[data-dismiss-group-index]:checked")]
      .map(input => this.pendingDismissalGroups[Number(input.dataset.dismissGroupIndex)])
      .filter(Boolean);
    const user = this.refs.dismissUser.value.trim();
    if (!selectedGroups.length) {
      this.refs.dismissStatus.textContent = "Choose at least one originating suspicion.";
      this.refs.dismissStatus.classList.add("error");
      return;
    }
    if (!user) {
      this.refs.dismissStatus.textContent = "User is required.";
      this.refs.dismissStatus.classList.add("error");
      return;
    }
    if (this.hasUnsavedIndividualReviewDrafts()) {
      this.refs.dismissStatus.textContent = "Save or discard the unsaved individual decision before dismissing issues.";
      this.refs.dismissStatus.classList.add("error");
      return;
    }
    this.refs.dismissSubmit.disabled = true;
    this.refs.dismissClose.disabled = true;
    this.refs.dismissStatus.textContent = `Dismissing ${formatCount(selectedGroups.length)} suspicion group(s)...`;
    this.refs.dismissStatus.classList.remove("error");
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/dismiss-issues`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            expected_current_dataset_id: this.expectedCurrentDatasetId(),
            expected_review_revision: this.expectedReviewRevision(),
            logical_name: this.currentArtifact,
            source_bundle_signature: this.data?.sourceSignature || "",
            dismissals: selectedGroups.map(group => ({
              parent_annotation_id: group.parentAnnotationId,
              fix_keys: group.fixes.map(fix => fix.fixKey),
            })),
            note: this.refs.dismissNote.value.trim(),
            user,
          }),
        },
      );
      this.setUser(user);
      this.pendingDismissalGroups = [];
      this.refs.dismissModal.classList.add("hidden");
      await this.loadStudyAtDataset(result.dataset.dataset_id, {
        result,
        clearTarget: "fixes",
      });
      this.setStatus(`Recorded not-suspicious decisions in ${result.dataset.dataset_id}.`);
    } catch (error) {
      await this.handleEditRequestError(error);
      this.refs.dismissStatus.textContent = error.message;
      this.refs.dismissStatus.classList.add("error");
      this.refs.dismissSubmit.disabled = false;
      this.refs.dismissClose.disabled = false;
      this.setStatus(error.message, true);
    }
  }

  hideIssueBurstPreview() {
    if (!this.refs?.issueBurstPreview) {
      return;
    }
    this.refs.issueBurstPreview.classList.add("hidden");
    this.refs.issueBurstPreviewTitle.textContent = "";
    this.refs.issueBurstPreviewList.innerHTML = "";
  }

  normalizeIssueBurstPreviewSource(burst) {
    const burstId = String(burst?.burstId || burst?.burst_id || "");
    if (!burstId || !this.data) {
      return null;
    }
    const stored = this.data.autoBurstById?.get(burstId);
    if (stored) {
      return stored;
    }
    const fixKeys = Array.isArray(burst?.fixKeys)
      ? burst.fixKeys.map(value => String(value || "")).filter(Boolean)
      : Array.isArray(burst?.fix_keys)
        ? burst.fix_keys.map(value => String(value || "")).filter(Boolean)
        : [];
    const fixes = fixKeys
      .map(fixKey => this.data.fixByKey.get(fixKey))
      .filter(Boolean)
      .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
    const suppliedPath = Array.isArray(burst?.path)
      ? burst.path
      : fixes.map(fix => fix.position);
    const path = suppliedPath
      .map(position => [Number(position?.[0]), Number(position?.[1])])
      .filter(position => Number.isFinite(position[0]) && Number.isFinite(position[1]));
    return {
      burstId,
      burstIdx: Math.max(0, Number(burst?.burstIdx ?? burst?.burst_idx) || 0),
      individual: String(burst?.individual || ""),
      setName: String(burst?.setName || burst?.set_name || "train") || "train",
      startFixKey: String(burst?.startFixKey || burst?.start_fix_key || fixKeys[0] || ""),
      endFixKey: String(burst?.endFixKey || burst?.end_fix_key || fixKeys[fixKeys.length - 1] || ""),
      startTimeMs: Number(burst?.startTimeMs ?? burst?.start_time_ms ?? fixes[0]?.timeMs) || 0,
      endTimeMs: Number(burst?.endTimeMs ?? burst?.end_time_ms ?? fixes[fixes.length - 1]?.timeMs) || 0,
      fixCount: Number(burst?.fixCount ?? burst?.fix_count ?? burst?.n_fixes) || fixKeys.length || path.length,
      burstGapSeconds: Number(burst?.burstGapSeconds ?? burst?.burst_gap_seconds) || this.data.burstGap?.effectiveSeconds || DEFAULT_BURST_GAP_SECONDS,
      fixKeys,
      path,
    };
  }

  buildIssueBurstPreviewModel(burst) {
    const selected = this.normalizeIssueBurstPreviewSource(burst);
    if (!selected) {
      return null;
    }
    const trackBursts = (this.data?.autoBursts || [])
      .filter(item => (
        item.individual === selected.individual
        && item.setName === selected.setName
      ))
      .sort((left, right) => (
        left.startTimeMs - right.startTimeMs
        || left.burstIdx - right.burstIdx
        || left.burstId.localeCompare(right.burstId)
      ));
    const selectedIndex = trackBursts.findIndex(item => item.burstId === selected.burstId);
    const previous = selectedIndex > 0
      ? trackBursts[selectedIndex - 1]
      : [...trackBursts]
        .reverse()
        .find(item => item.endTimeMs <= selected.startTimeMs && item.burstId !== selected.burstId)
        || null;
    const next = selectedIndex >= 0 && selectedIndex < trackBursts.length - 1
      ? trackBursts[selectedIndex + 1]
      : trackBursts.find(
        item => item.startTimeMs >= selected.endTimeMs && item.burstId !== selected.burstId,
      ) || null;
    const selectedFixes = selected.fixKeys
      .map(fixKey => this.data.fixByKey.get(fixKey) || null);
    const positions = selected.path;
    const pointIndices = samplePreviewIndices(positions.length, 120);
    const points = pointIndices.map(index => {
      const fix = selectedFixes[index] || null;
      return {
        index,
        position: positions[index],
        color: fix ? this.colorForFix(fix) : [124, 210, 255, POINT_ALPHA],
      };
    });
    const previousContext = Array.isArray(previous?.path)
      ? samplePreviewPath(previous.path, 80)
      : [];
    const nextContext = Array.isArray(next?.path)
      ? samplePreviewPath(next.path, 80)
      : [];
    const durationSeconds = selected.endTimeMs >= selected.startTimeMs
      ? (selected.endTimeMs - selected.startTimeMs) / 1000
      : null;
    const gapBeforeSeconds = previous && selected.startTimeMs >= previous.endTimeMs
      ? (selected.startTimeMs - previous.endTimeMs) / 1000
      : null;
    const gapAfterSeconds = next && next.startTimeMs >= selected.endTimeMs
      ? (next.startTimeMs - selected.endTimeMs) / 1000
      : null;
    return {
      selected,
      previous,
      next,
      positions,
      pathPositions: samplePreviewPath(positions, 300),
      points,
      previousContext,
      nextContext,
      durationSeconds,
      distanceMeters: selected.pathLengthM,
      medianStepMeters: selected.medianStepM,
      gapBeforeSeconds,
      gapAfterSeconds,
      geometry: buildBurstPreviewGeometry(
        positions,
        previousContext,
        nextContext,
      ),
    };
  }

  issueBurstPreviewCardHtml(burst) {
    const model = this.buildIssueBurstPreviewModel(burst);
    if (!model) {
      return "";
    }
    const { selected, geometry } = model;
    const anomalyScore = this.getIssueBurstAnomalyScore(selected.burstId);
    const scoreLabel = anomalyScore === null
      ? "anomaly score unavailable"
      : `anomaly score ${formatMaybeNumber(anomalyScore, "")}`;
    const metrics = [
      `${formatCount(selected.fixCount || model.positions.length)} fixes`,
      `duration ${formatCompactDuration(model.durationSeconds)}`,
      `distance ${formatCompactDistance(model.distanceMeters)}`,
      `median step ${formatCompactDistance(model.medianStepMeters)}`,
      `gap before ${formatCompactDuration(model.gapBeforeSeconds)}`,
      `gap after ${formatCompactDuration(model.gapAfterSeconds)}`,
    ].map(value => `<span>${escapeHtml(value)}</span>`).join("");
    if (!geometry) {
      return `
        <article class="movement-burst-preview-card">
          <div class="movement-burst-preview-card-head">
            <strong>${escapeHtml(selected.burstId)}</strong>
            <span>${escapeHtml(scoreLabel)}</span>
          </div>
          <div class="movement-burst-preview-frame">
            <div class="movement-burst-preview-empty">
              Burst metadata is available, but its path is not loaded in the current map view.
            </div>
          </div>
          <div class="movement-burst-preview-metrics">${metrics}</div>
        </article>
      `;
    }

    const selectedPath = geometry.mapPath(model.pathPositions);
    const previousPath = geometry.mapPath(model.previousContext);
    const nextPath = geometry.mapPath(model.nextContext);
    const previewPoints = model.points.map(item => ({
      ...item,
      point: geometry.mapPosition(item.position),
    }));
    const startPoint = geometry.mapPosition(model.positions[0]);
    const endPoint = geometry.mapPosition(model.positions[model.positions.length - 1]);
    const sameEndpoint = (
      Math.hypot(startPoint.x - endPoint.x, startPoint.y - endPoint.y) < 1
    );
    const contextSvg = [
      previousPath.length
        ? `<polyline points="${previewSvgPoints(previousPath)}" fill="none" stroke="#64748b" stroke-opacity="0.68" stroke-width="2.2" stroke-dasharray="6 4" stroke-linecap="round" stroke-linejoin="round"><title>Previous adjacent burst</title></polyline>`
        : "",
      nextPath.length
        ? `<polyline points="${previewSvgPoints(nextPath)}" fill="none" stroke="#64748b" stroke-opacity="0.68" stroke-width="2.2" stroke-dasharray="6 4" stroke-linecap="round" stroke-linejoin="round"><title>Next adjacent burst</title></polyline>`
        : "",
    ].join("");
    const pointsSvg = previewPoints.map(item => (
      `<circle cx="${item.point.x.toFixed(2)}" cy="${item.point.y.toFixed(2)}" r="3.2" fill="${rgbaCss(item.color)}" stroke="#ffffff" stroke-width="1"><title>Fix ${formatCount(item.index + 1)}</title></circle>`
    )).join("");
    const endpointSvg = sameEndpoint
      ? (
        `<circle cx="${startPoint.x.toFixed(2)}" cy="${startPoint.y.toFixed(2)}" r="7" fill="#8b5cf6" stroke="#ffffff" stroke-width="1.5"/>`
        + `<text x="${(startPoint.x + 9).toFixed(2)}" y="${(startPoint.y - 7).toFixed(2)}" fill="#334155" font-size="10" font-weight="700">Start/end</text>`
      )
      : (
        `<circle cx="${startPoint.x.toFixed(2)}" cy="${startPoint.y.toFixed(2)}" r="6" fill="#0891b2" stroke="#ffffff" stroke-width="1.4"/>`
        + `<text x="${(startPoint.x + 8).toFixed(2)}" y="${(startPoint.y - 7).toFixed(2)}" fill="#334155" font-size="10" font-weight="700">Start</text>`
        + `<circle cx="${endPoint.x.toFixed(2)}" cy="${endPoint.y.toFixed(2)}" r="6" fill="#db2777" stroke="#ffffff" stroke-width="1.4"/>`
        + `<text x="${(endPoint.x + 8).toFixed(2)}" y="${(endPoint.y - 7).toFixed(2)}" fill="#334155" font-size="10" font-weight="700">End</text>`
      );
    const stationaryNote = geometry.stationary
      ? '<text x="12" y="113" fill="#475569" font-size="10">Stationary or overlapping fixes</text>'
      : "";
    const contextNote = model.previousContext.length || model.nextContext.length
      ? '<text x="508" y="14" text-anchor="end" fill="#475569" font-size="10">Dashed gray: adjacent bursts</text>'
      : "";
    const scaleBarSvg = previewScaleBarSvg(geometry);
    const samplingNote = model.positions.length > model.points.length
      ? `<text x="508" y="149" text-anchor="end" fill="#475569" font-size="10">Showing ${formatCount(model.points.length)} of ${formatCount(model.positions.length)} fix marks</text>`
      : "";
    return `
      <article class="movement-burst-preview-card">
        <div class="movement-burst-preview-card-head">
          <strong>${escapeHtml(selected.burstId)}</strong>
          <span>${escapeHtml(scoreLabel)}</span>
        </div>
        <div class="movement-burst-preview-frame">
          <svg viewBox="0 0 520 160" role="img" aria-label="${escapeHtml(`Spatial preview of ${selected.burstId} with adjacent bursts`)}">
            ${contextSvg}
            <polyline
              points="${previewSvgPoints(selectedPath)}"
              fill="none"
              stroke="${rgbaCss(burstPathColor(this.data?.individualPalette, selected, 235))}"
              stroke-width="3.8"
              stroke-linecap="round"
              stroke-linejoin="round"
            />
            ${pointsSvg}
            ${endpointSvg}
            ${stationaryNote}
            ${contextNote}
            ${scaleBarSvg}
            ${samplingNote}
          </svg>
        </div>
        <div class="movement-burst-preview-metrics">${metrics}</div>
      </article>
    `;
  }

  renderIssueBurstPreviews(bursts) {
    if (!this.refs?.issueBurstPreview) {
      return;
    }
    const selectedBursts = Array.isArray(bursts) ? bursts : [];
    if (!selectedBursts.length) {
      this.hideIssueBurstPreview();
      return;
    }
    const currentField = this.getCurrentColorField();
    this.refs.issueBurstPreview.classList.remove("hidden");
    this.refs.issueBurstPreviewTitle.textContent = (
      `${formatCount(selectedBursts.length)} selected`
      + (currentField ? ` • color by ${currentField.label}` : "")
    );
    this.refs.issueBurstPreviewList.innerHTML = selectedBursts
      .map(burst => this.issueBurstPreviewCardHtml(burst))
      .join("");
  }

  resetIssueScopeControls() {
    this.cancelRequest("issueBurstScores");
    this.refs.issueScopeControl?.classList.add("hidden");
    if (this.refs.issueScope) {
      this.refs.issueScope.value = "individual";
    }
    if (this.refs.issueBurstControl) {
      this.refs.issueBurstControl.hidden = true;
    }
    if (this.refs.issueBurstList) {
      this.refs.issueBurstList.innerHTML = "";
    }
    if (this.refs.issueBurstOrder) {
      this.refs.issueBurstOrder.textContent = "";
    }
    if (this.refs.issueSelection) {
      this.refs.issueSelection.hidden = false;
    }
    this.hideIssueBurstPreview();
  }

  setupIndividualQueueIssueScope(
    individual,
    { initialBurstId = "", initialScope = "individual" } = {},
  ) {
    const context = this.pendingIssueContext;
    if (!context || context.individual !== individual) {
      return;
    }
    const bursts = (this.data?.autoBursts || [])
      .filter(burst => burst.individual === individual)
      .sort((left, right) => (
        left.startTimeMs - right.startTimeMs
        || left.burstIdx - right.burstIdx
        || left.burstId.localeCompare(right.burstId)
      ));
    context.queueReviewBursts = bursts;
    context.selectedBurstIds = new Set(
      initialBurstId && bursts.some(burst => burst.burstId === initialBurstId)
        ? [initialBurstId]
        : [],
    );
    const burstScopeOption = this.refs.issueScope.querySelector('option[value="burst"]');
    if (burstScopeOption) {
      burstScopeOption.disabled = !bursts.length;
      burstScopeOption.textContent = "By Burst";
    }
    this.refs.issueScope.value = initialScope === "burst" && bursts.length
      ? "burst"
      : "individual";
    this.refs.issueScopeControl.classList.remove("hidden");
    this.updateIndividualQueueIssueScope();
  }

  getIssueBurstScores() {
    if (
      this.anomalyRanking?.burstScores instanceof Map
      && this.anomalyRanking.burstScores.size
    ) {
      return this.anomalyRanking.burstScores;
    }
    const scores = new Map();
    for (const row of this.anomalyRanking?.rankedIndividuals || []) {
      for (const ref of this.normalizeRankingBurstRefs(row)) {
        const score = finiteOrNull(ref?.anomaly_score);
        if (ref.burst_id && score !== null && !scores.has(ref.burst_id)) {
          scores.set(ref.burst_id, score);
        }
      }
    }
    return scores;
  }

  getIssueBurstAnomalyScore(burstId) {
    const target = String(burstId || "");
    if (!target) {
      return null;
    }
    const direct = this.anomalyRanking?.burstScores instanceof Map
      ? this.anomalyRanking.burstScores.get(target)
      : undefined;
    if (direct !== undefined) {
      return direct;
    }
    const ref = this.getRankingBurstRef(target);
    return finiteOrNull(ref?.anomaly_score);
  }

  orderedIssueBursts() {
    const bursts = this.pendingIssueContext?.queueReviewBursts || [];
    const scores = this.getIssueBurstScores();
    return [...bursts].sort((left, right) => {
      const leftScore = scores.get(left.burstId);
      const rightScore = scores.get(right.burstId);
      if (leftScore !== undefined || rightScore !== undefined) {
        if (leftScore === undefined) {
          return 1;
        }
        if (rightScore === undefined) {
          return -1;
        }
        if (leftScore !== rightScore) {
          return rightScore - leftScore;
        }
      }
      return (
        left.startTimeMs - right.startTimeMs
        || left.burstIdx - right.burstIdx
        || left.burstId.localeCompare(right.burstId)
      );
    });
  }

  selectedIssueBursts() {
    const selectedIds = this.pendingIssueContext?.selectedBurstIds instanceof Set
      ? this.pendingIssueContext.selectedBurstIds
      : new Set();
    return this.orderedIssueBursts().filter(
      burst => selectedIds.has(burst.burstId),
    );
  }

  renderIssueBurstPicker() {
    const context = this.pendingIssueContext;
    if (!context || !this.refs.issueBurstList) {
      return;
    }
    const bursts = this.orderedIssueBursts();
    const scores = this.getIssueBurstScores();
    const scoredCount = bursts.filter(burst => scores.has(burst.burstId)).length;
    if (context.issueBurstScoresLoading) {
      this.refs.issueBurstOrder.textContent = "Loading anomaly scores…";
    } else if (scoredCount) {
      this.refs.issueBurstOrder.textContent = (
        `Highest anomaly score first • ${formatCount(scoredCount)} scored`
      );
    } else if (context.issueBurstScoresError) {
      this.refs.issueBurstOrder.textContent = "Scores unavailable • time order";
    } else if (["checking", "restoring", "loading"].includes(this.anomalyRanking?.status)) {
      this.refs.issueBurstOrder.textContent = "Ranking scores not ready • time order";
    } else {
      this.refs.issueBurstOrder.textContent = "Time order • no compatible burst ranking";
    }
    if (!bursts.length) {
      this.refs.issueBurstList.innerHTML = (
        '<div class="movement-empty">No bursts are available for this individual.</div>'
      );
      return;
    }
    this.refs.issueBurstList.innerHTML = bursts.map(burst => {
      const score = scores.get(burst.burstId);
      const scoreLabel = score === undefined
        ? "not scored"
        : `score ${formatMaybeNumber(score, "")}`;
      const checked = context.selectedBurstIds.has(burst.burstId) ? " checked" : "";
      return `
        <label class="movement-burst-choice">
          <input
            type="checkbox"
            data-issue-burst-id="${escapeHtml(burst.burstId)}"
            ${checked}
          >
          <span class="movement-burst-choice-main">
            <strong>${escapeHtml(`Burst ${formatCount(burst.burstIdx + 1)}`)}</strong>
            • ${escapeHtml(burst.setName)}
            • ${escapeHtml(formatTimestamp(burst.startTimeMs))}
            • ${escapeHtml(`${formatCount(burst.fixCount)} fixes`)}
          </span>
          <span class="movement-burst-choice-score">${escapeHtml(scoreLabel)}</span>
        </label>
      `;
    }).join("");
  }

  setIssueBurstIncluded(burstId, included) {
    const context = this.pendingIssueContext;
    if (!context?.selectedBurstIds || !burstId) {
      return;
    }
    const scrollTop = this.refs.issueBurstList.scrollTop;
    if (included) {
      context.selectedBurstIds.add(burstId);
    } else {
      context.selectedBurstIds.delete(burstId);
    }
    this.updateIndividualQueueIssueScope();
    this.refs.issueBurstList.scrollTop = scrollTop;
  }

  async loadIssueBurstScores() {
    const context = this.pendingIssueContext;
    const analysisId = String(this.anomalyRanking?.analysisId || "");
    if (
      !context?.individual
      || !analysisId
      || (
        this.anomalyRanking?.burstScores instanceof Map
        && this.anomalyRanking.burstScores.size
      )
    ) {
      return;
    }
    const familyName = this.currentFamily;
    const studyName = this.currentStudy;
    const datasetId = this.currentDatasetId;
    const individual = context.individual;
    const controller = this.beginRequest("issueBurstScores");
    context.issueBurstScoresAttempted = true;
    context.issueBurstScoresLoading = true;
    context.issueBurstScoresError = "";
    this.updateIndividualQueueIssueScope();
    try {
      const artifact = await this.fetchJSON(
        `/api/apps/movement/family/${encodeURIComponent(familyName)}/study/${encodeURIComponent(studyName)}/analysis/${encodeURIComponent(analysisId)}/artifact/burst_anomaly_ranking.json`,
        { signal: controller.signal },
      );
      if (
        this.requestControllers.issueBurstScores !== controller
        || familyName !== this.currentFamily
        || studyName !== this.currentStudy
        || datasetId !== this.currentDatasetId
        || this.pendingIssueContext !== context
        || context.individual !== individual
      ) {
        return;
      }
      this.anomalyRanking = {
        ...this.anomalyRanking,
        status: String(artifact?.run_status || this.anomalyRanking.status || "completed"),
        rankedIndividuals: (
          !(this.anomalyRanking?.rankedIndividuals || []).length
          && Array.isArray(artifact?.ranked_individuals)
        )
          ? artifact.ranked_individuals.map(row => ({
            ...row,
            ranked_burst_refs: Array.isArray(row?.ranked_burst_refs)
              ? row.ranked_burst_refs.slice(0, 3)
              : [],
          }))
          : this.anomalyRanking.rankedIndividuals,
        burstScores: this.makeBurstScoreMap(artifact?.scored_bursts),
        warnings: Array.isArray(artifact?.warnings)
          ? artifact.warnings
          : this.anomalyRanking.warnings,
        burstGap: artifact?.burst_gap || this.anomalyRanking.burstGap,
        modelFit: artifact?.model_fit || artifact?.scorer || this.anomalyRanking.modelFit,
      };
      context.issueBurstScoresLoading = false;
      this.updateIndividualQueueIssueScope();
      this.renderAnomalyRanking();
      this.renderIndividuals();
    } catch (error) {
      if (!this.isAbortError(error) && this.pendingIssueContext === context) {
        context.issueBurstScoresLoading = false;
        context.issueBurstScoresError = error.message;
        this.updateIndividualQueueIssueScope();
      }
    } finally {
      if (this.requestControllers.issueBurstScores === controller) {
        this.requestControllers.issueBurstScores = null;
      }
    }
  }

  updateIndividualQueueIssueScope() {
    const context = this.pendingIssueContext;
    if (!context?.individual || !Array.isArray(context.queueReviewBursts)) {
      return;
    }
    const individual = context.individual;
    const bursts = Array.isArray(context.queueReviewBursts)
      ? context.queueReviewBursts
      : [];
    const useBurst = this.refs.issueScope.value === "burst" && Boolean(bursts.length);
    if (
      useBurst
      && context.mode !== "bursts"
      && context.selectedBurstIds instanceof Set
      && !context.selectedBurstIds.size
    ) {
      const firstBurst = this.orderedIssueBursts()[0];
      if (firstBurst) {
        context.selectedBurstIds.add(firstBurst.burstId);
      }
    }
    const selectedBursts = this.selectedIssueBursts();
    this.refs.issueScope.value = useBurst ? "burst" : "individual";
    this.refs.issueBurstControl.hidden = !useBurst;
    this.renderIssueBurstPicker();

    const previousDefaultType = context.defaultIssueType || "";
    const previousDefaultQuestion = context.defaultOwnerQuestion || "";
    const defaultIssueType = useBurst ? "burst review" : "individual review";
    const defaultOwnerQuestion = useBurst
      ? "Could you confirm whether these bursts should be treated as outliers?"
      : "Could you confirm whether this individual's track should be treated as an outlier?";
    if (!this.refs.issueType.value.trim() || this.refs.issueType.value === previousDefaultType) {
      this.refs.issueType.value = defaultIssueType;
    }
    if (
      !this.refs.issueQuestion.value.trim()
      || this.refs.issueQuestion.value === previousDefaultQuestion
    ) {
      this.refs.issueQuestion.value = defaultOwnerQuestion;
    }
    context.defaultIssueType = defaultIssueType;
    context.defaultOwnerQuestion = defaultOwnerQuestion;
    context.mode = useBurst ? "bursts" : "individual";
    context.individual = individual;
    context.setName = "";
    context.burstIds = useBurst
      ? selectedBursts.map(burst => burst.burstId)
      : [];
    if (useBurst) {
      this.renderIssueBurstPreviews(selectedBursts);
    } else {
      this.hideIssueBurstPreview();
    }

    this.refs.issueMeta.innerHTML = `
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Individual:</strong> ${escapeHtml(individual)}</div>
      ${useBurst ? `<div><strong>Bursts selected:</strong> ${escapeHtml(formatCount(selectedBursts.length))}</div>` : ""}
      <div><strong>Flag scope:</strong> ${useBurst ? "selected bursts" : "entire individual"}</div>
      <div><strong>Resolved fixes:</strong> ${escapeHtml(formatCount(
        useBurst
          ? selectedBursts.reduce((total, burst) => total + burst.fixCount, 0)
          : this.data?.stats?.[individual]?.rowCount || 0,
      ))}</div>
      <div><strong>Origin:</strong> ${escapeHtml(context.origin || "manual")}</div>
      ${context.sourceAnalysisId ? `<div><strong>Source analysis:</strong> ${escapeHtml(context.sourceAnalysisId)}</div>` : ""}
      <div><strong>Individual decision:</strong> remains independent from this issue</div>
    `;
    if (useBurst) {
      this.refs.issueSelection.hidden = true;
    } else {
      const burstNote = bursts.length
        ? ` Choose “By Burst” above to select from the ${formatCount(bursts.length)} available bursts.`
        : " No bursts are currently available for this individual under the current burst definition.";
      this.refs.issueSelection.hidden = false;
      this.refs.issueSelection.textContent = (
        `The annotation will apply to every fix for ${individual}.${burstNote}`
      );
    }
    this.refs.issueSubmit.disabled = useBurst && !selectedBursts.length;
    if (
      useBurst
      && this.anomalyRanking?.analysisId
      && !(
        this.anomalyRanking?.burstScores instanceof Map
        && this.anomalyRanking.burstScores.size
      )
      && !context.issueBurstScoresLoading
      && !context.issueBurstScoresAttempted
    ) {
      void this.loadIssueBurstScores();
    }
  }

  openActiveFlagModal() {
    const target = this.getActiveFlagTarget();
    if (!target.ready) return;
    if (target.kind === "individual") {
      this.openIndividualReviewModal(target.individual);
      return;
    }
    if (target.kind === "bursts") {
      this.openSelectedBurstsModal(target.bursts);
      return;
    }
    if (target.kind === "segment") {
      this.openSegmentModal("suspected");
      return;
    }
    this.openIssueModal("suspected", target);
  }

  openIssueModal(status, target = null) {
    if (this.rejectLockedEdit()) {
      return;
    }
    const selectedFixes = Array.isArray(target?.fixes) ? target.fixes : this.getSelectedFixes();
    if (!selectedFixes.length || !this.currentArtifact) {
      return;
    }
    this.resetIssueScopeControls();
    const field = this.getCurrentColorField();
    const isFilterTarget = target?.kind === "filter";
    const isGpsSpikeTarget = isFilterTarget && field?.key === GPS_SPIKE_COLOR_FIELD_KEY;
    const issueThreshold = isFilterTarget ? this.getCurrentIssueThreshold() : "";
    const candidateKeys = this.getCandidateQueryReturnedMatchKeys();
    const candidateGenerated = !isFilterTarget
      && Boolean(this.candidateQueryPreview?.analysisId)
      && selectedFixes.every(fix => candidateKeys.has(fix.fixKey));
    const origin = isFilterTarget ? "threshold" : (candidateGenerated ? "algorithm" : "manual");
    const queueReviewIndividual = (
      !isFilterTarget
      && this.individualReviewQueue.mode === "queue"
      && selectedFixes.length
      && selectedFixes.every(
        fix => fix.individual === this.individualReviewQueue.activeIndividual,
      )
    ) ? this.individualReviewQueue.activeIndividual : "";
    const thresholdFilter = isGpsSpikeTarget
      ? {
          kind: "gps_spike",
          step_length_threshold_m: this.thresholdState.value,
          minimum_abs_turn_angle_deg: this.gpsSpikeTurnAngleDeg,
          individuals: this.getSelectedIndividuals(),
          set_names: [...this.getVisibleSetNames()],
        }
      : isFilterTarget
        ? {
          field_key: field?.key || "",
          field_kind: field?.kind || "",
          operator: this.thresholdState.reverse === true ? "lt" : "gt",
          threshold_value: typeof this.thresholdState.value === "number"
            ? this.thresholdState.value
            : null,
          selected_levels: Array.isArray(this.thresholdState.selectedLevels)
            ? [...this.thresholdState.selectedLevels]
            : [],
          }
        : null;
    this.pendingIssueStatus = status;
    this.pendingIssueContext = {
      mode: "fixes",
      fixes: selectedFixes,
      origin,
      sourceAnalysisId: candidateGenerated ? this.candidateQueryPreview.analysisId : "",
      issueField: isGpsSpikeTarget
        ? "step_length_m + abs(turn_angle_deg)"
        : field?.key || "",
      issueThreshold,
      thresholdFilter,
      queueReviewIndividual,
      workflowContext: this.buildIssueWorkflowContext(
        isFilterTarget ? "filter" : "fix",
        { selectionMethods: isGpsSpikeTarget ? ["color_threshold"] : null },
      ),
    };
    this.refs.issueTitle.textContent = `Mark fixes as ${status}`;
    this.refs.issueMeta.innerHTML = `
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>${isFilterTarget ? "Visible preview matches" : "Checked fixes"}:</strong> ${escapeHtml(formatCount(isFilterTarget ? target?.matchCount || selectedFixes.length : selectedFixes.length))}</div>
      <div><strong>Flag scope:</strong> ${isGpsSpikeTarget ? "all matching fixes in the visible individual and track scope" : isFilterTarget ? "all matching fixes in the full dataset" : "checked fixes"}</div>
      <div><strong>Issue variable:</strong> ${escapeHtml(isGpsSpikeTarget ? "Step length + absolute turn angle" : field?.label || "Not set")}</div>
      <div><strong>Issue threshold:</strong> ${escapeHtml(issueThreshold || "Not set")}</div>
      <div><strong>Origin:</strong> ${escapeHtml(origin)}</div>
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

  openSegmentModal(status) {
    if (this.rejectLockedEdit()) {
      return;
    }
    const selection = this.getCurrentSegmentSelection();
    if (!selection || selection.fixes.length < 2 || !this.currentArtifact) {
      return;
    }
    this.resetIssueScopeControls();
    this.pendingIssueStatus = status;
    this.pendingIssueContext = {
      mode: "segment",
      fixes: selection.fixes,
      startFixKey: selection.startFixKey,
      endFixKey: selection.endFixKey,
      selectedFixKeys: selection.fixes.map(fix => fix.fixKey),
      individual: selection.individual,
      setName: selection.setName,
      selectionMethod: selection.selectionMethod || "",
      issueField: "",
      issueThreshold: "",
      queueReviewIndividual: (
        this.individualReviewQueue.mode === "queue"
        && selection.individual === this.individualReviewQueue.activeIndividual
      ) ? selection.individual : "",
      workflowContext: this.buildIssueWorkflowContext("segment", {
        selectionMethods: [selection.selectionMethod || ""],
      }),
    };
    this.refs.issueTitle.textContent = `Mark segment as ${status}`;
    this.refs.issueMeta.innerHTML = `
      <div><strong>Family:</strong> ${escapeHtml(this.currentFamily)}</div>
      <div><strong>Study:</strong> ${escapeHtml(this.currentStudy)}</div>
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Track:</strong> ${escapeHtml(`${selection.individual} • ${selection.setName}`)}</div>
      <div><strong>Selection method:</strong> ${escapeHtml(formatSelectionMethod(selection.selectionMethod))}</div>
      <div><strong>Segment fixes:</strong> ${escapeHtml(formatCount(selection.fixes.length))}</div>
      <div><strong>Start:</strong> ${escapeHtml(`${formatTimestamp(selection.fixes[0].timeMs)} • ${selection.startFixKey}`)}</div>
      <div><strong>End:</strong> ${escapeHtml(`${formatTimestamp(selection.fixes[selection.fixes.length - 1].timeMs)} • ${selection.endFixKey}`)}</div>
    `;
    this.refs.issueSelection.textContent = selection.fixes
      .slice(0, 40)
      .map(fix => `${fix.individual} • ${formatTimestamp(fix.timeMs)} • ${fix.fixKey}`)
      .join("\n");
    this.refs.issueUser.value = this.getUser();
    this.refs.issueType.value = "";
    this.refs.issueNote.value = "";
    this.refs.issueQuestion.value = "Could you confirm whether this contiguous track segment reflects collar removal, transport, or another non-animal movement event?";
    this.refs.issueStatus.textContent = "";
    this.refs.issueStatus.classList.remove("error");
    this.refs.issueSubmit.disabled = false;
    this.refs.issueClose.disabled = false;
    this.refs.issueModal.classList.remove("hidden");
    this.refs.issueType.focus();
  }

  openIndividualReviewModal(individual) {
    if (this.rejectLockedEdit()) {
      return;
    }
    if (!this.data || !individual || !this.currentArtifact) {
      return;
    }
    this.resetIssueScopeControls();
    const stats = this.data.stats[individual] || {};
    this.pendingIssueStatus = "suspected";
    this.pendingIssueContext = {
      mode: "individual",
      fixes: [],
      individual,
      setName: "",
      origin: "manual",
      issueField: "",
      issueThreshold: "",
      queueReviewIndividual: (
        this.individualReviewQueue.mode === "queue"
        && individual === this.individualReviewQueue.activeIndividual
      ) ? individual : "",
      workflowContext: this.buildIssueWorkflowContext("individual"),
      defaultIssueType: "individual review",
      defaultOwnerQuestion: "Could you confirm whether this individual's track should be treated as an outlier?",
    };
    this.refs.issueTitle.textContent = "Flag entire individual";
    this.refs.issueMeta.innerHTML = `
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Individual:</strong> ${escapeHtml(individual)}</div>
      <div><strong>Resolved fixes:</strong> ${escapeHtml(formatCount(stats.rowCount || 0))}</div>
      <div><strong>Status:</strong> suspected</div>
    `;
    this.refs.issueSelection.textContent = `The annotation will apply to every fix for ${individual} across train and test tracks.`;
    this.refs.issueUser.value = this.getUser();
    this.refs.issueType.value = "individual review";
    this.refs.issueNote.value = "";
    this.refs.issueQuestion.value = "Could you confirm whether this individual's track should be treated as an outlier?";
    this.refs.issueStatus.textContent = "";
    this.refs.issueStatus.classList.remove("error");
    this.refs.issueSubmit.disabled = false;
    this.refs.issueClose.disabled = false;
    this.refs.issueModal.classList.remove("hidden");
    this.refs.issueNote.focus();
  }

  openSelectedBurstsModal(bursts) {
    if (this.rejectLockedEdit()) {
      return;
    }
    bursts = Array.isArray(bursts) ? bursts.filter(Boolean) : [];
    if (!this.data || !bursts.length || !this.currentArtifact) {
      return;
    }
    this.resetIssueScopeControls();
    const burstIds = bursts.map(burst => String(burst.burstId || "")).filter(Boolean);
    const individuals = new Set(bursts.map(burst => String(burst.individual || "")));
    const individual = individuals.size === 1 ? [...individuals][0] : "";
    const fixCount = bursts.reduce((total, burst) => total + (Number(burst.fixCount) || 0), 0);
    const origin = this.manualFlagTarget.origin || "manual";
    const sourceAnalysisId = this.manualFlagTarget.sourceAnalysisId || "";
    this.pendingIssueStatus = "suspected";
    this.pendingIssueContext = {
      mode: "bursts",
      fixes: [],
      burstIds,
      individual,
      origin,
      sourceAnalysisId,
      issueField: "",
      issueThreshold: "",
      queueReviewIndividual: (
        this.individualReviewQueue.mode === "queue"
        && individual === this.individualReviewQueue.activeIndividual
      ) ? individual : "",
      workflowContext: this.buildIssueWorkflowContext("bursts"),
      defaultIssueType: origin === "algorithm" ? "burst anomaly" : "burst review",
      defaultOwnerQuestion: "Could you confirm whether these bursts should be treated as outliers?",
    };
    this.refs.issueTitle.textContent = "Flag bursts for review";
    this.refs.issueMeta.innerHTML = `
      <div><strong>Dataset:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Artifact:</strong> ${escapeHtml(this.currentArtifact)}</div>
      <div><strong>Bursts:</strong> ${escapeHtml(formatCount(burstIds.length))}</div>
      <div><strong>Individual:</strong> ${escapeHtml(individual || "Multiple individuals")}</div>
      <div><strong>Resolved fixes:</strong> ${escapeHtml(formatCount(fixCount))}</div>
      <div><strong>Origin:</strong> ${escapeHtml(origin)}</div>
    `;
    this.refs.issueSelection.textContent = sourceAnalysisId
      ? `This annotation will retain provenance to analysis ${sourceAnalysisId}.`
      : `The backend will resolve ${formatCount(burstIds.length)} selected burst(s) using the current burst-gap settings.`;
    this.refs.issueUser.value = this.getUser();
    this.refs.issueType.value = origin === "algorithm" ? "burst anomaly" : "burst review";
    this.refs.issueNote.value = "";
    this.refs.issueQuestion.value = "Could you confirm whether these bursts should be treated as outliers?";
    this.refs.issueStatus.textContent = "";
    this.refs.issueStatus.classList.remove("error");
    this.refs.issueSubmit.disabled = false;
    this.refs.issueClose.disabled = false;
    this.refs.issueModal.classList.remove("hidden");
    this.refs.issueNote.focus();
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
    this.refs.reportSnapshotUnit.value = "burst";
    this.refs.reportBasemap.value = "current";
    this.refs.reportSpreadIndividuals.checked = true;
    this.renderReportLinks();
    this.refs.reportStatus.textContent = "";
    this.refs.reportStatus.classList.remove("error");
    this.refs.reportSubmit.disabled = false;
    this.refs.reportClose.disabled = false;
    this.populateReportIndividualOptions();
    this.updateReportModeUi();
    this.renderReportSelection();
    this.refs.reportModal.classList.remove("hidden");
  }

  closeModal(modal, submitButton) {
    if (submitButton.disabled) {
      return;
    }
    if (modal === this.refs.issueModal) {
      this.cancelRequest("issueBurstScores");
      this.pendingIssueContext = null;
      this.hideIssueBurstPreview();
    }
    if (modal === this.refs.confirmModal) {
      this.pendingConfirmationGroups = [];
    }
    if (modal === this.refs.dismissModal) {
      this.pendingDismissalGroups = [];
    }
    modal.classList.add("hidden");
  }

  getCurrentIssueThreshold() {
    const field = this.getCurrentColorField();
    if (
      field?.key === GPS_SPIKE_COLOR_FIELD_KEY
      && Number.isFinite(this.thresholdState.value)
    ) {
      return `step > ${this.thresholdState.value} m and |turn| >= ${this.gpsSpikeTurnAngleDeg}°`;
    }
    return getIssueThresholdFromState(field, this.thresholdState);
  }

  async submitIssueAction() {
    if (this.rejectLockedEdit()) {
      this.refs.issueModal.classList.add("hidden");
      return;
    }
    const context = this.pendingIssueContext || {
      mode: "fixes",
      fixes: this.getSelectedFixes(),
      issueField: this.getCurrentColorField()?.key || "",
      issueThreshold: this.getCurrentIssueThreshold(),
    };
    const selectedFixes = Array.isArray(context.fixes) ? context.fixes : [];
    const user = this.refs.issueUser.value.trim();
    const issueType = this.refs.issueType.value.trim();
    const issueField = context.issueField || "";
    const issueThreshold = context.issueThreshold || "";
    const issueNote = this.refs.issueNote.value.trim();
    const ownerQuestion = this.refs.issueQuestion.value.trim();
    const groupScope = context.mode === "individual" || context.mode === "bursts";
    if (!selectedFixes.length && !groupScope) {
      return;
    }
    if (context.mode === "bursts" && !(context.burstIds || []).length) {
      this.refs.issueStatus.textContent = "Choose at least one burst to flag.";
      this.refs.issueStatus.classList.add("error");
      return;
    }
    if (!user || !issueType || !issueNote || !ownerQuestion) {
      this.refs.issueStatus.textContent = "User, issue type, description, and owner question are required.";
      this.refs.issueStatus.classList.add("error");
      return;
    }
    this.refs.issueSubmit.disabled = true;
    this.refs.issueClose.disabled = true;
    this.refs.issueStatus.textContent = context.mode === "segment"
      ? `Marking ${formatCount(selectedFixes.length)} fixes as one ${this.pendingIssueStatus} segment...`
      : context.mode === "individual"
        ? `Flagging individual ${context.individual} for review...`
        : context.mode === "bursts"
          ? `Flagging ${formatCount(context.burstIds?.length || 0)} burst(s) for review...`
          : `Marking ${formatCount(selectedFixes.length)} fixes as ${this.pendingIssueStatus}...`;
    this.refs.issueStatus.classList.remove("error");
    this.setStatus(this.refs.issueStatus.textContent);

    try {
      const endpoint = `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/annotate-scope`;
      let scope;
      if (context.thresholdFilter) {
        scope = {
          kind: "filter",
          filter: context.thresholdFilter,
        };
      } else if (context.mode === "segment") {
        scope = {
          kind: "segment",
          fix_keys: context.selectedFixKeys,
          start_fix_key: context.startFixKey,
          end_fix_key: context.endFixKey,
          individual: context.individual,
          set_name: context.setName,
          selection_method: context.selectionMethod,
        };
      } else if (context.mode === "individual") {
        scope = {
          kind: "individual",
          individual: context.individual,
          set_name: context.setName || "",
        };
      } else if (context.mode === "bursts") {
        scope = {
          kind: "bursts",
          burst_ids: context.burstIds,
        };
      } else {
        scope = {
          kind: "fix",
          fix_keys: selectedFixes.map(fix => fix.fixKey),
        };
      }
      const body = {
        dataset_id: this.currentDatasetId,
        expected_current_dataset_id: this.expectedCurrentDatasetId(),
        expected_review_revision: this.expectedReviewRevision(),
        logical_name: this.currentArtifact,
        source_bundle_signature: this.data?.sourceSignature || "",
        scope,
        status: this.pendingIssueStatus,
        origin: context.origin || (issueThreshold ? "threshold" : "manual"),
        issue_type: issueType,
        issue_field: issueField,
        issue_threshold: issueThreshold,
        comment: issueNote,
        owner_question: ownerQuestion,
        source_analysis_id: context.sourceAnalysisId || "",
        workflow_context: context.workflowContext || this.buildIssueWorkflowContext(context.mode),
        burst_gap_mode: this.getBurstGapMode(),
        burst_gap_seconds: this.getBurstGapSeconds(),
        burst_gap_quantile: this.getBurstGapQuantile(),
        user,
      };
      const result = await this.requestJSON(
        endpoint,
        {
          method: "POST",
          body: JSON.stringify(body),
        },
      );
      const queueReviewIndividual = String(context.queueReviewIndividual || "");
      this.setUser(user);
      this.pendingIssueContext = null;
      this.refs.issueModal.classList.add("hidden");
      await this.loadStudyAtDataset(
        result.dataset.dataset_id,
        {
          preserveAnnotationContext: true,
          result,
          clearTarget: context.thresholdFilter
            ? "filter"
            : context.mode === "segment"
              ? "segment"
              : context.mode === "fixes"
                ? "fixes"
                : context.mode === "individual"
                  ? "individual"
                  : context.mode === "bursts" ? "bursts" : "",
        },
      );
      if (queueReviewIndividual) {
        this.stageIndividualReviewDecision(queueReviewIndividual, "fix_keep");
      }
      this.setStatus(`Created ${result.step.title} in ${result.dataset.dataset_id}.`);
    } catch (error) {
      await this.handleEditRequestError(error);
      this.refs.issueStatus.textContent = error.message;
      this.refs.issueStatus.classList.add("error");
      this.refs.issueSubmit.disabled = false;
      this.refs.issueClose.disabled = false;
      this.setStatus(error.message, true);
    }
  }

  async exportReviewedCsv() {
    if (!this.data || !this.currentFamily || !this.currentStudy || !this.currentDatasetId || !this.currentArtifact) {
      return;
    }
    if (this.hasUnsavedIndividualReviewDrafts()) {
      this.setStatus("Save or discard the unsaved individual decision before exporting.", true);
      return;
    }
    this.refs.exportReviewedCsv.disabled = true;
    const exportRds = MOVEMENT_APP_CONFIG.rdsSource;
    this.setStatus(exportRds ? "Exporting the reviewed RDS study bundle..." : `Exporting reviewed CSV for ${this.currentArtifact}...`);
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/actions/${exportRds ? "export-reviewed-rds" : "export-reviewed-csv"}`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            logical_name: this.currentArtifact,
            user: this.getUser() || "reviewer",
          }),
        },
      );
      const analysisId = String(result?.analysis?.analysis_id || "");
      if (!analysisId) {
        throw new Error("Reviewed CSV export did not return an analysis id.");
      }
      const output = (result?.analysis?.realized_output_artifacts || [])
        .find(item => exportRds
          ? String(item?.logical_name || "").endsWith(".zip")
          : String(item?.logical_name || "").endsWith("_reviewed.csv"));
      const outputName = String(
        output?.logical_name
        || (exportRds ? "movement_reviewed_rds.zip" : result?.analysis?.parameters?.output_artifact)
        || "",
      ).trim();
      if (!outputName) {
        throw new Error("Reviewed CSV export did not return an output artifact.");
      }
      const href = `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/analysis/${encodeURIComponent(analysisId)}/artifact/${encodeURIComponent(outputName)}`;
      this.refs.outputLinks.innerHTML = `<a href="${href}" download="${escapeHtml(outputName)}" data-authenticated-artifact="download" data-artifact-name="${escapeHtml(outputName)}">Download ${escapeHtml(outputName)}</a>`;
      if (exportRds) {
        this.setStatus("Exported one reviewed RDS per source individual plus a writer manifest. The source dataset was not changed.");
      } else {
        const flaggedCount = formatCount(result?.summary?.flagged_row_count || 0);
        const rowCount = formatCount(result?.summary?.exported_row_count || 0);
        this.setStatus(`Exported ${rowCount} rows with ${flaggedCount} flagged rows. The source dataset was not changed.`);
      }
    } catch (error) {
      this.setStatus(`Reviewed ${exportRds ? "RDS" : "CSV"} export failed: ${error.message}`, true);
    } finally {
      this.updateActionButtons();
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
      this.renderReportLinks();
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

  async captureSnapshotsForSelection(snapshotWindows) {
    if (!snapshotWindows.length) {
      return [];
    }
    const preset = this.getSelectedReportBasemapPreset();
    const renderer = await createReportSnapshotRenderer({
      preset,
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

  async loadStudyAtDataset(
    datasetId,
    {
      preserveAnnotationContext = false,
      result = null,
      clearTarget = "",
      reason = "mutation",
    } = {},
  ) {
    return this.transitionToDataset(datasetId, {
      preserveAnnotationContext,
      result,
      clearTarget,
      reason,
    });
  }

  reviewProjectionIndividuals() {
    if (!this.data) return [];
    if (this.data.overviewHasAllFixes || this.data.reportAllState === "loaded") {
      return [...this.data.individuals];
    }
    return uniqueNonEmpty([
      ...this.getSelectedIndividuals(),
      ...(this.data.detailIndividuals || []),
    ]);
  }

  async fetchReviewProjection(datasetId) {
    const params = new URLSearchParams({ logical_name: this.currentArtifact });
    for (const individual of this.reviewProjectionIndividuals()) {
      params.append("individuals", individual);
    }
    const controller = this.beginRequest("reviewProjection");
    const payload = await this.fetchJSON(
      `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/dataset/${encodeURIComponent(datasetId)}/review-projection?${params.toString()}`,
      { signal: controller.signal, cache: "no-store" },
    );
    if (this.requestControllers.reviewProjection !== controller) {
      throw new DOMException("Review update was superseded", "AbortError");
    }
    return payload;
  }

  rememberDatasetMetadata(dataset) {
    if (!dataset?.dataset_id || this.allDatasets.some(item => item.dataset_id === dataset.dataset_id)) {
      return;
    }
    this.allDatasets.push({
      dataset_id: dataset.dataset_id,
      parent_dataset_id: dataset.parent_dataset_id || "",
      created_at: dataset.created_at || "",
      user: dataset.user || "",
      note: dataset.note || "",
      artifact_count: Array.isArray(dataset.artifacts) ? dataset.artifacts.length : 0,
      artifact_names: (dataset.artifacts || []).map(artifact => artifact.logical_name),
      actor: dataset.actor || null,
    });
    this.allDatasets.sort(
      (left, right) => String(right.created_at || "").localeCompare(String(left.created_at || "")),
    );
  }

  async refreshGraphMetadata(preferredDatasetId = this.currentDatasetId) {
    const graph = await this.fetchJSON(
      `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/graph`,
      { cache: "no-store" },
    );
    this.graph = graph;
    this.allDatasets = [...(Array.isArray(graph.datasets) ? graph.datasets : [])]
      .sort((left, right) => String(right.created_at || "").localeCompare(String(left.created_at || "")));
    this.stepByOutputDatasetId = new Map(
      (Array.isArray(graph.steps) ? graph.steps : []).map(step => [step.output_dataset_id, step]),
    );
    this.refreshDatasetOptions(preferredDatasetId);
  }

  addMutationResultToGraph(result) {
    const dataset = result?.dataset;
    const step = result?.step;
    if (!dataset?.dataset_id) return;
    const node = {
      dataset_id: dataset.dataset_id,
      parent_dataset_id: dataset.parent_dataset_id || "",
      created_at: dataset.created_at || "",
      user: dataset.user || "",
      note: dataset.note || "",
      artifact_count: Array.isArray(dataset.artifacts) ? dataset.artifacts.length : 0,
      artifact_names: Array.isArray(dataset.artifacts)
        ? dataset.artifacts.map(artifact => artifact.logical_name)
        : [],
      actor: dataset.actor || null,
    };
    this.allDatasets = [
      node,
      ...this.allDatasets.filter(item => item.dataset_id !== node.dataset_id),
    ].sort((left, right) => String(right.created_at || "").localeCompare(String(left.created_at || "")));
    this.graph = {
      ...(this.graph || {}),
      current_dataset_id: dataset.dataset_id,
      datasets: this.allDatasets,
    };
    if (step?.output_dataset_id) {
      this.stepByOutputDatasetId.set(step.output_dataset_id, step);
      const graphSteps = Array.isArray(this.graph?.steps) ? this.graph.steps : [];
      this.graph = {
        ...this.graph,
        steps: [
          ...graphSteps.filter(item => item.output_dataset_id !== step.output_dataset_id),
          step,
        ],
      };
    }
    this.refreshDatasetOptions(dataset.dataset_id);
  }

  emptyFixReview() {
    return {
      status: "",
      issueId: "",
      issueType: "",
      issueField: "",
      issueThreshold: "",
      issues: [],
      effectiveIssues: [],
      issueNote: "",
      ownerQuestion: "",
      reviewUser: "",
      reviewedAt: "",
    };
  }

  applyReviewProjection(projection) {
    if (!this.data) return;
    const projectedFixes = parseMovementFixes(projection.fixes || []);
    const projectedIndividuals = new Set(
      Array.isArray(projection.projected_individuals)
        ? projection.projected_individuals.map(String)
        : [],
    );
    const reviewByFixKey = new Map(
      projectedFixes.map(fix => [fix.fixKey, fix.review]),
    );
    const projectedSegments = parseMovementSegments(projection.segments || []);
    const segmentMembershipsByFixKey = new Map();
    for (const segment of projectedSegments) {
      for (const fixKey of segment.fixKeys) {
        const memberships = segmentMembershipsByFixKey.get(fixKey) || [];
        memberships.push({
          status: segment.status,
          segmentId: segment.segmentId,
          issueType: segment.issueType,
          startFixKey: segment.startFixKey,
          endFixKey: segment.endFixKey,
          selectionMethod: segment.selectionMethod,
          issueNote: segment.issueNote,
          ownerQuestion: segment.ownerQuestion,
          reviewUser: segment.reviewUser,
          reviewedAt: segment.reviewedAt,
        });
        segmentMembershipsByFixKey.set(fixKey, memberships);
      }
    }
    const patchFixes = fixes => (fixes || []).map(fix => ({
      ...fix,
      review: reviewByFixKey.get(fix.fixKey)
        || (projectedIndividuals.has(fix.individual) ? this.emptyFixReview() : fix.review),
      segments: segmentMembershipsByFixKey.get(fix.fixKey)
        || (projectedIndividuals.has(fix.individual) ? [] : fix.segments),
    }));
    this.data.overviewFixes = patchFixes(this.data.overviewFixes);
    this.data.candidateFixes = patchFixes(this.data.candidateFixes);
    this.data.detailFixes = patchFixes(this.data.detailFixes);
    this.data.reportAllFixes = patchFixes(this.data.reportAllFixes);
    this.data.confirmedFixes = patchFixes(this.data.confirmedFixes);
    this.data.overviewSegments = projectedSegments;
    this.data.detailSegments = [];

    const merged = new Map();
    for (const fix of [
      ...this.data.overviewFixes,
      ...this.data.candidateFixes,
      ...this.data.detailFixes,
      ...this.data.reportAllFixes,
      ...this.data.confirmedFixes,
    ]) {
      merged.set(fix.fixKey, fix);
    }
    this.data.fixes = [...merged.values()]
      .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
    this.data.fixByKey = new Map(this.data.fixes.map(fix => [fix.fixKey, fix]));
    this.data.suspiciousFixes = this.data.fixes.filter(fix => fix.review.status === "suspected");
    this.data.confirmedPointFixes = this.data.fixes.filter(fix => fix.review.status === "confirmed");
    this.data.suspiciousState = "loaded";
    this.data.suspiciousMatchingFixCount = Number(
      projection.review_counts?.suspected,
    ) || 0;
    this.data.suspiciousReturnedFixCount = this.data.suspiciousFixes.length;
    this.data.suspiciousTruncated = false;
    this.data.confirmedMatchingFixCount = Number(
      projection.review_counts?.confirmed,
    ) || 0;
    this.data.flaggedStepOverlays = buildFlaggedStepOverlays(this.data);
    this.data.segments = projectedSegments;
    this.data.segmentById = new Map(projectedSegments.map(segment => [segment.segmentId, segment]));

    const projectedStats = projection.stats || {};
    const individualReviews = projection.individual_reviews || {};
    for (const individual of this.data.individuals) {
      const stats = this.data.stats[individual];
      const counts = projectedStats[individual] || {};
      const decision = individualReviews[individual] || {};
      stats.suspectedCount = Number(counts.suspected_count) || 0;
      stats.unresolvedSuspectedCount = Number(counts.unresolved_suspected_count) || 0;
      stats.confirmedCount = Number(counts.confirmed_count) || 0;
      stats.unresolvedIssueTypes = Array.isArray(counts.unresolved_issue_types)
        ? counts.unresolved_issue_types.map(String)
        : [];
      stats.unresolvedIssueOrigins = Array.isArray(counts.unresolved_issue_origins)
        ? counts.unresolved_issue_origins.map(String)
        : [];
      stats.reviewed = decision.reviewed === true;
      stats.reviewDecision = String(decision.review_decision || "");
      stats.reviewOk = stats.reviewDecision === "ok";
      stats.needsCheck = decision.needs_check === true;
      stats.reviewUser = String(decision.review_user || "");
      stats.reviewedAt = String(decision.reviewed_at || "");
      stats.reviewComment = String(decision.review_comment || "");
    }
  }

  clearCompletedFlagTarget(clearTarget) {
    if (!this.data) return;
    if (clearTarget === "filter") {
      const filterFixKeys = new Set([
        ...this.getCandidateQueryMatchKeys(),
        ...this.getActiveThresholdMatchKeys(),
      ]);
      this.data.selectedFixKeys = new Set(
        [...this.data.selectedFixKeys].filter(fixKey => !filterFixKeys.has(fixKey)),
      );
      this.clearCandidateQueryPreview({ render: false });
      this.clearThresholdState();
    } else if (clearTarget === "segment") {
      this.setTableSelection();
      this.mapRangeAwaitingEnd = false;
    } else if (clearTarget === "fixes") {
      this.data.selectedFixKeys = new Set();
      this.setTableSelection();
    } else if (["individual", "bursts"].includes(clearTarget)) {
      this.resetManualFlagTarget();
    }
  }

  async transitionToDataset(
    datasetId,
    {
      preserveAnnotationContext = false,
      result = null,
      clearTarget = "",
      reason = "dataset_switch",
    } = {},
  ) {
    const transitionId = ++this.viewTransitionId;
    const detailLoadWasInterrupted = this.data?.detailState === "loading";
    for (const requestName of [
      "dataset",
      "overview",
      "reviewProjection",
      "detail",
      "suspicious",
      "confirmed",
    ]) {
      this.cancelRequest(requestName);
    }
    const viewContext = this.captureDatasetViewContext();
    if (viewContext && preserveAnnotationContext) {
      viewContext.annotationReloadContext = this.captureAnnotationReloadContext();
    }
    const previousDatasetId = this.currentDatasetId;
    const previousDataset = this.currentDataset;
    const previousArtifactEntry = this.currentArtifactEntry;
    const previousEditLockProfile = this.editLockProfile;
    this.setStatus(`Loading version ${datasetId}...`);
    if (result) {
      this.addMutationResultToGraph(result);
    }
    try {
      const projection = await this.fetchReviewProjection(datasetId);
      if (transitionId !== this.viewTransitionId) return;
      this.rememberDatasetMetadata(projection.dataset);
      const compatible = Boolean(
        this.data
        && !this.data.binaryMovement
        && this.currentArtifact
        && this.data.sourceSignature
        && this.data.exclusionSignature
        && this.data.sourceSignature === String(projection.source_signature || "")
        && this.data.exclusionSignature === String(projection.exclusion_signature || "")
      );
      if (!compatible) {
        this.currentDatasetId = datasetId;
        if (reason === "editor_release") {
          await this.refreshGraphMetadata(datasetId);
        }
        await this.loadDataset(viewContext);
        if (transitionId !== this.viewTransitionId) return;
        const loadedRequestedView = Boolean(
          this.data
          && this.data.sourceSignature === String(projection.source_signature || "")
          && this.data.exclusionSignature === String(projection.exclusion_signature || "")
        );
        if (!loadedRequestedView) {
          this.currentDatasetId = previousDatasetId;
          this.currentDataset = previousDataset;
          this.currentArtifactEntry = previousArtifactEntry;
          this.editLockProfile = previousEditLockProfile;
          this.refs.dataset.value = previousDatasetId;
          this.renderEditLockProfile();
          this.updateActionButtons();
          throw new Error("The requested version could not be prepared; the previous view is still open.");
        }
        return;
      }

      this.currentDatasetId = datasetId;
      this.currentDataset = projection.dataset || this.currentDataset;
      this.currentArtifactEntry = (this.currentDataset?.artifacts || []).find(
        artifact => artifact.logical_name === this.currentArtifact,
      ) || null;
      this.editLockProfile = projection.edit_profile || this.editLockProfile;
      this.data.sourceSignature = String(projection.source_signature || "");
      this.data.exclusionSignature = String(projection.exclusion_signature || "");
      this.applyReviewProjection(projection);
      this.clearCompletedFlagTarget(clearTarget);
      this.refreshDatasetOptions(datasetId);
      this.restoreAnnotationReloadContext(viewContext);
      this.renderEditLockProfile();
      this.renderIndividuals();
      this.renderSelectedFixes();
      this.renderThresholdPane();
      this.renderLegend();
      this.renderLayers();
      this.updateActionButtons();
      this.saveUiState();
      if (reason === "dataset_switch" || reason === "editor_release") {
        void this.restoreSavedAnalyses();
      }
      if (reason === "editor_release") {
        void this.refreshGraphMetadata(datasetId);
      }
      this.setStatus(`Loaded version ${datasetId} without reloading movement tracks.`);
      if (detailLoadWasInterrupted) {
        void this.loadDetailForCurrentSelection({
          preservedFixKeys: new Set(this.data.selectedFixKeys),
        });
      }
    } catch (error) {
      if (this.isAbortError(error)) return;
      this.currentDatasetId = previousDatasetId;
      this.refs.dataset.value = previousDatasetId;
      this.setStatus(`Could not load version ${datasetId}: ${error.message}`, true);
      throw error;
    }
  }

  openResumeModal() {
    const profile = this.editLockProfile || {};
    const resume = profile.resume || {};
    if (!this.currentDatasetId || resume.allowed !== true || !resume.token) {
      this.setStatus("This dataset version is not currently eligible for Resume.", true);
      return;
    }
    const datasetCount = Number(resume.discard_dataset_count) || 0;
    const stepCount = Number(resume.discard_step_count) || 0;
    const analysisCount = Number(resume.discard_analysis_count) || 0;
    this.refs.resumeMeta.innerHTML = `
      <div><strong>Resume at:</strong> ${escapeHtml(this.currentDatasetId)}</div>
      <div><strong>Current pointer:</strong> ${escapeHtml(profile.current_dataset_id || "")}</div>
      <div><strong>Datasets removed from active history:</strong> ${escapeHtml(formatCount(datasetCount))}</div>
      <div><strong>Steps removed from active history:</strong> ${escapeHtml(formatCount(stepCount))}</div>
      <div><strong>Analyses removed from active history:</strong> ${escapeHtml(formatCount(analysisCount))}</div>
    `;
    this.refs.resumeWarning.innerHTML = `
      <strong>Generated outputs will be deleted.</strong>
      Resuming keeps this dataset version and its complete ancestor chain. Every other active
      dataset version, step, and associated analysis will be removed from the active graph.
      A compact metadata archive will retain provenance, scripts, summaries, IDs, file sizes,
      actor, and discarded paths, but the generated data and analysis outputs will not be
      recoverable from the app.
    `;
    this.refs.resumeUser.value = this.getUser() || "reviewer";
    this.refs.resumeStatus.textContent = "";
    this.refs.resumeStatus.classList.remove("error");
    this.refs.resumeSubmit.disabled = false;
    this.refs.resumeClose.disabled = false;
    this.refs.resumeModal.classList.remove("hidden");
    this.refs.resumeUser.focus();
  }

  async submitResumeHistory() {
    const profile = this.editLockProfile || {};
    const resume = profile.resume || {};
    const user = this.refs.resumeUser.value.trim();
    if (!user) {
      this.refs.resumeStatus.textContent = "User is required.";
      this.refs.resumeStatus.classList.add("error");
      return;
    }
    if (resume.allowed !== true || !resume.token) {
      this.refs.resumeStatus.textContent = "History changed. Close this dialog and reopen Resume.";
      this.refs.resumeStatus.classList.add("error");
      return;
    }
    this.refs.resumeSubmit.disabled = true;
    this.refs.resumeClose.disabled = true;
    this.refs.resumeStatus.textContent = "Archiving metadata and removing forward history...";
    this.refs.resumeStatus.classList.remove("error");
    try {
      const result = await this.requestJSON(
        `/api/apps/movement/family/${encodeURIComponent(this.currentFamily)}/study/${encodeURIComponent(this.currentStudy)}/resume`,
        {
          method: "POST",
          body: JSON.stringify({
            dataset_id: this.currentDatasetId,
            expected_current_dataset_id: profile.current_dataset_id,
            expected_review_revision: this.expectedReviewRevision(),
            resume_token: resume.token,
            user,
          }),
        },
      );
      this.setUser(user);
      this.refs.resumeModal.classList.add("hidden");
      this.currentDatasetId = result.dataset.dataset_id;
      await this.loadStudy({ preferredDatasetId: result.dataset.dataset_id });
      const archive = result.archive || {};
      this.setStatus(
        `Resumed at ${result.dataset.dataset_id}; archived metadata for `
        + `${formatCount(archive.discarded_dataset_count || 0)} dataset(s), `
        + `${formatCount(archive.discarded_step_count || 0)} step(s), and `
        + `${formatCount(archive.discarded_analysis_count || 0)} analysis record(s).`,
      );
    } catch (error) {
      const refreshed = await this.handleEditRequestError(error);
      this.refs.resumeStatus.textContent = refreshed
        ? `${error.message} Close this dialog and reopen Resume.`
        : error.message;
      this.refs.resumeStatus.classList.add("error");
      this.refs.resumeSubmit.disabled = refreshed;
      this.refs.resumeClose.disabled = false;
      this.setStatus(error.message, true);
    }
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
        {
          method: "POST",
          body: JSON.stringify({
            expected_current_dataset_id: this.expectedCurrentDatasetId(),
            expected_review_revision: this.expectedReviewRevision(),
          }),
        },
      );
      await this.loadStudyAtDataset(result.dataset.dataset_id, {
        result,
        reason: "dataset_switch",
      });
      this.setStatus(`Undid to ${result.dataset.dataset_id}.`);
    } catch (error) {
      await this.handleEditRequestError(error);
      this.setStatus(error.message, true);
      this.updateUndoButton();
    }
  }
}

function buildDatasetFromSummary(summary, preferredColorBy) {
  const individuals = collectSummaryIndividuals(summary);
  if (!individuals.length) {
    throw new Error("Dataset summary did not contain any individuals.");
  }
  const overviewFixes = parseMovementFixes(summary.fixes || []);
  const overviewSegments = parseMovementSegments(summary.segments || []);
  const overviewAutoBursts = parseMovementAutoBursts(summary.auto_bursts || []);
  const burstGap = parseMovementBurstGap(summary);
  const totalRows = Number(summary.total_rows) || 0;
  const overviewTruncated = Boolean(summary.overview_truncated) || overviewFixes.length < totalRows;

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
      unresolvedSuspectedCount: Number(item?.unresolved_suspected_count) || 0,
      confirmedCount: Number(item?.confirmed_count) || 0,
      unresolvedIssueTypes: Array.isArray(item?.unresolved_issue_types)
        ? item.unresolved_issue_types.map(value => String(value || "")).filter(Boolean)
        : [],
      unresolvedIssueOrigins: Array.isArray(item?.unresolved_issue_origins)
        ? item.unresolved_issue_origins.map(value => String(value || "")).filter(Boolean)
        : [],
      reviewed: item?.reviewed === true,
      reviewDecision: String(item?.review_decision || ""),
      reviewOk: String(item?.review_decision || "") === "ok",
      needsCheck: item?.needs_check === true,
      reviewUser: String(item?.review_user || ""),
      reviewedAt: String(item?.reviewed_at || ""),
      reviewComment: String(item?.review_comment || ""),
    };
  });

  const colorFields = buildMovementColorFields(
    Array.isArray(summary.color_fields) ? summary.color_fields.map(field => ({
      key: String(field?.key || ""),
      label: String(field?.label || field?.key || ""),
      kind: String(field?.kind || "categorical"),
      source: String(field?.source || "raw"),
    })).filter(field => field.key) : [],
  );
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
        unresolvedSuspectedCount: 0,
        confirmedCount: 0,
        unresolvedIssueTypes: [],
        unresolvedIssueOrigins: [],
        reviewed: false,
        reviewDecision: "",
        reviewOk: false,
        reviewUser: "",
        reviewedAt: "",
        reviewComment: "",
      };
    }
  });

  const defaultColorBy = colorFields.some(field => field.key === preferredColorBy)
    ? preferredColorBy
    : colorFields[0]?.key || "step_length_m";

  const data = {
    sourceFormat: String(summary.source_format || "csv"),
    sourceSignature: String(summary.source_signature || ""),
    exclusionSignature: String(summary.exclusion_signature || ""),
    totalRows,
    individuals,
    speciesByIndividual: summary.species_by_individual || {},
    seriesByIndividual,
    coverageByIndividual,
    stats,
    fixes: [],
    fixByKey: new Map(),
    trackStepSegments: [],
    visiblePointCache: new Map(),
    visibleTrackStepCache: new Map(),
    autoBurstRenderCache: new Map(),
    segments: [],
    segmentById: new Map(),
    autoBursts: [],
    autoBurstById: new Map(),
    overviewFixes,
    overviewSegments,
    overviewAutoBursts,
    burstGap,
    overviewTruncated,
    overviewFixLimit: Number(summary.overview_fix_limit) || null,
    autoBurstsTruncated: Boolean(summary.auto_bursts_truncated),
    overviewHasAllFixes: !overviewTruncated,
    binaryMovement: null,
    binaryMapReady: false,
    candidateFixes: [],
    suspiciousFixes: [],
    suspiciousState: "idle",
    suspiciousLimit: null,
    suspiciousMatchingFixCount: 0,
    suspiciousReturnedFixCount: 0,
    suspiciousTruncated: false,
    confirmedFixes: [],
    confirmedState: "idle",
    confirmedLimit: null,
    confirmedMatchingFixCount: 0,
    confirmedReturnedFixCount: 0,
    confirmedTruncated: false,
    detailFixes: [],
    detailSegments: [],
    detailAutoBursts: [],
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

function initialMovementVisibleIndividuals(data) {
  if (!data) {
    return [];
  }
  if (data.overviewTruncated) {
    return [];
  }
  return data.individuals || [];
}


function isSourceOnlyFlaggedFix(fix) {
  const status = String(fix?.review?.status || "").trim().toLowerCase();
  return Array.isArray(fix?.sourceFlags)
    && fix.sourceFlags.length > 0
    && status !== "suspected"
    && status !== "confirmed";
}

function parseMovementBurstGap(summary) {
  const nested = summary?.burst_gap || {};
  const mode = String(nested.mode ?? summary?.burst_gap_mode ?? DEFAULT_BURST_GAP_MODE).trim().toLowerCase();
  const quantile = finiteOrNull(nested.quantile ?? summary?.burst_gap_quantile) ?? DEFAULT_BURST_GAP_QUANTILE;
  const fallbackSeconds = finiteOrNull(nested.fallback_seconds ?? summary?.burst_gap_fallback_seconds) ?? DEFAULT_BURST_GAP_SECONDS;
  const effectiveSeconds = finiteOrNull(nested.effective_seconds ?? summary?.burst_gap_seconds) ?? fallbackSeconds;
  const gapCount = Math.max(0, Number(nested.gap_count ?? summary?.burst_gap_gap_count) || 0);
  return {
    mode: mode === "manual" || mode === "quantile" ? mode : DEFAULT_BURST_GAP_MODE,
    quantile: quantile > 0 && quantile <= 1 ? quantile : DEFAULT_BURST_GAP_QUANTILE,
    fallbackSeconds,
    effectiveSeconds,
    gapCount,
    usedFallback: Boolean(nested.used_fallback ?? summary?.burst_gap_used_fallback),
  };
}

function formatBurstGapQuantile(value) {
  const quantileValue = Number(value);
  if (!Number.isFinite(quantileValue) || quantileValue <= 0 || quantileValue > 1) {
    return "p99.9";
  }
  const percentile = quantileValue * 100;
  const decimals = Math.abs(percentile - Math.round(percentile)) < 0.0001 ? 0 : 3;
  let formatted = percentile.toFixed(decimals);
  if (formatted.includes(".")) {
    formatted = formatted.replace(/0+$/, "").replace(/\.$/, "");
  }
  return `p${formatted}`;
}

function formatBurstGapMetadata(burstGap) {
  if (!burstGap) {
    return "";
  }
  const effectiveSeconds = finiteOrNull(burstGap.effectiveSeconds);
  if (effectiveSeconds === null) {
    return "";
  }
  const secondsLabel = formatMaybeNumber(effectiveSeconds, "s");
  if (burstGap.mode === "quantile") {
    let label = `${formatBurstGapQuantile(burstGap.quantile)} = ${secondsLabel}`;
    if (Number(burstGap.gapCount) > 0) {
      label += ` from ${formatCount(burstGap.gapCount)} gaps`;
    }
    if (burstGap.usedFallback) {
      label += " (fallback)";
    }
    return label;
  }
  return `${secondsLabel} manual`;
}

function collectSummaryIndividuals(summary) {
  const names = new Set();
  const add = value => {
    const name = String(value || "").trim();
    if (name) {
      names.add(name);
    }
  };
  if (Array.isArray(summary.individuals)) {
    summary.individuals.forEach(add);
  }
  for (const collection of [
    summary.stats,
    summary.series_by_individual,
    summary.coverage_by_individual,
    summary.species_by_individual,
  ]) {
    Object.keys(collection || {}).forEach(add);
  }
  for (const item of [...(summary.fixes || []), ...(summary.auto_bursts || [])]) {
    add(item?.individual);
  }
  return [...names].sort((left, right) => left.localeCompare(right));
}

function parseMovementFixes(items) {
  return Array.isArray(items) ? items.map(item => ({
    fixKey: String(item?.fix_key || ""),
    individual: String(item?.individual || ""),
    setName: String(item?.set || "train") || "train",
    timeMs: Number(item?.time_ms) || 0,
    position: [Number(item?.lon) || 0, Number(item?.lat) || 0],
    attributes: {
      ...(item?.attributes || {}),
      [INDIVIDUAL_COLOR_FIELD_KEY]: String(item?.individual || ""),
    },
    review: {
      status: String(item?.review?.status || ""),
      issueId: String(item?.review?.issue_id || ""),
      issueType: String(item?.review?.issue_type || ""),
      issueField: String(item?.review?.issue_field || ""),
      issueThreshold: String(item?.review?.issue_threshold || ""),
      issues: normalizeReviewIssues(item?.review),
      effectiveIssues: normalizeReviewIssues({ issues: item?.review?.effective_issues || [] }),
      issueNote: String(item?.review?.issue_note || ""),
      ownerQuestion: String(item?.review?.owner_question || ""),
      reviewUser: String(item?.review?.review_user || ""),
      reviewedAt: String(item?.review?.reviewed_at || ""),
    },
    segments: normalizeSegmentMemberships(item?.segments),
    analyticallyExcluded: Boolean(item?.analytically_excluded),
    sourceFlags: Array.isArray(item?.source_flags)
      ? item.source_flags.map(value => String(value || "")).filter(Boolean)
      : [],
  })).filter(item => item.fixKey) : [];
}

function parseMovementSegments(items) {
  return Array.isArray(items) ? items.map(item => ({
    segmentId: String(item?.segment_id || ""),
    individual: String(item?.individual || ""),
    setName: String(item?.set_name || "train") || "train",
    startFixKey: String(item?.start_fix_key || ""),
    endFixKey: String(item?.end_fix_key || ""),
    selectionMethod: String(item?.selection_method || ""),
    startTimeMs: Number(item?.start_time_ms) || 0,
    endTimeMs: Number(item?.end_time_ms) || 0,
    fixCount: Number(item?.fix_count) || 0,
    status: String(item?.status || ""),
    issueType: String(item?.issue_type || ""),
    issueNote: String(item?.issue_note || ""),
    ownerQuestion: String(item?.owner_question || ""),
    reviewUser: String(item?.review_user || ""),
    reviewedAt: String(item?.reviewed_at || ""),
    fixKeys: Array.isArray(item?.fix_keys) ? item.fix_keys.map(value => String(value || "")).filter(Boolean) : [],
    path: Array.isArray(item?.path)
      ? item.path
        .map(position => [Number(position?.[0]) || 0, Number(position?.[1]) || 0])
        .filter(position => Number.isFinite(position[0]) && Number.isFinite(position[1]))
      : [],
  })).filter(item => item.segmentId) : [];
}

function parseMovementAutoBursts(items) {
  return Array.isArray(items) ? items.map(item => ({
    burstId: String(item?.burst_id || ""),
    burstIdx: Number(item?.burst_idx) || 0,
    individual: String(item?.individual || ""),
    setName: String(item?.set_name || "train") || "train",
    startFixKey: String(item?.start_fix_key || ""),
    endFixKey: String(item?.end_fix_key || ""),
    startTimeMs: Number(item?.start_time_ms) || 0,
    endTimeMs: Number(item?.end_time_ms) || 0,
    fixCount: Number(item?.fix_count) || 0,
    burstGapSeconds: Number(item?.burst_gap_seconds) || DEFAULT_BURST_GAP_SECONDS,
    pathLengthM: finiteOrNull(item?.path_length_m),
    medianStepM: finiteOrNull(item?.median_step_m),
    fixKeys: Array.isArray(item?.fix_keys) ? item.fix_keys.map(value => String(value || "")).filter(Boolean) : [],
    path: Array.isArray(item?.path)
      ? item.path
        .map(position => [Number(position?.[0]) || 0, Number(position?.[1]) || 0])
        .filter(position => Number.isFinite(position[0]) && Number.isFinite(position[1]))
      : [],
  })).filter(item => item.burstId) : [];
}

function samplePreviewIndices(length, maxItems) {
  const count = Math.max(0, Number(length) || 0);
  const limit = Math.max(2, Number(maxItems) || 2);
  if (count <= limit) {
    return Array.from({ length: count }, (_value, index) => index);
  }
  const indices = new Set([0, count - 1]);
  for (let sampleIndex = 1; sampleIndex < limit - 1; sampleIndex += 1) {
    indices.add(Math.round((sampleIndex * (count - 1)) / (limit - 1)));
  }
  return [...indices].sort((left, right) => left - right);
}

function samplePreviewPath(path, maxItems) {
  const positions = Array.isArray(path) ? path : [];
  return samplePreviewIndices(positions.length, maxItems)
    .map(index => positions[index])
    .filter(Boolean);
}

function formatCompactDuration(seconds) {
  const value = finiteOrNull(seconds);
  if (value === null || value < 0) {
    return "—";
  }
  if (value < 60) {
    return `${formatMaybeNumber(value, "s")}`;
  }
  if (value < 3600) {
    return `${formatMaybeNumber(value / 60, "min")}`;
  }
  if (value < 86400) {
    return `${formatMaybeNumber(value / 3600, "h")}`;
  }
  return `${formatMaybeNumber(value / 86400, "d")}`;
}

function formatCompactDistance(meters) {
  const value = finiteOrNull(meters);
  if (value === null || value < 0) {
    return "—";
  }
  return value < 1000
    ? formatMaybeNumber(value, "m")
    : formatMaybeNumber(value / 1000, "km");
}

function buildBurstPreviewGeometry(
  selectedPositions,
  previousPositions = [],
  nextPositions = [],
  {
    width = 520,
    height = 160,
    padding = 18,
  } = {},
) {
  const validPositions = positions => (Array.isArray(positions) ? positions : [])
    .map(position => [Number(position?.[0]), Number(position?.[1])])
    .filter(position => Number.isFinite(position[0]) && Number.isFinite(position[1]));
  const selected = validPositions(selectedPositions);
  if (!selected.length) {
    return null;
  }
  const previous = validPositions(previousPositions);
  const next = validPositions(nextPositions);
  const allPositions = [...previous, ...selected, ...next];
  const meanLatitude = allPositions.reduce(
    (sum, position) => sum + position[1],
    0,
  ) / allPositions.length;
  const longitudeScale = Math.max(1e-6, Math.abs(Math.cos(meanLatitude * Math.PI / 180)));
  const rawPosition = position => ({
    x: Number(position[0]) * longitudeScale,
    y: Number(position[1]),
  });
  const rawSelected = selected.map(rawPosition);
  const rawAll = allPositions.map(rawPosition);
  const rawMinX = Math.min(...rawAll.map(point => point.x));
  const rawMaxX = Math.max(...rawAll.map(point => point.x));
  const rawMinY = Math.min(...rawAll.map(point => point.y));
  const rawMaxY = Math.max(...rawAll.map(point => point.y));
  const selectedSpanX = Math.max(...rawSelected.map(point => point.x))
    - Math.min(...rawSelected.map(point => point.x));
  const selectedSpanY = Math.max(...rawSelected.map(point => point.y))
    - Math.min(...rawSelected.map(point => point.y));
  const originalSpanX = rawMaxX - rawMinX;
  const originalSpanY = rawMaxY - rawMinY;
  const stationary = selectedSpanX < 1e-10 && selectedSpanY < 1e-10;
  const centerX = (rawMinX + rawMaxX) / 2;
  const centerY = (rawMinY + rawMaxY) / 2;
  let spanX = Math.max(originalSpanX, 1e-7);
  let spanY = Math.max(originalSpanY, 1e-7);
  const frameWidth = Math.max(1, width - (padding * 2));
  const frameHeight = Math.max(1, height - (padding * 2));
  const targetAspect = frameWidth / frameHeight;
  if ((spanX / spanY) > targetAspect) {
    spanY = spanX / targetAspect;
  } else {
    spanX = spanY * targetAspect;
  }
  spanX *= 1.18;
  spanY *= 1.18;
  const minX = centerX - (spanX / 2);
  const maxX = centerX + (spanX / 2);
  const minY = centerY - (spanY / 2);
  const maxY = centerY + (spanY / 2);
  const mapRaw = point => ({
    x: padding + (((point.x - minX) / spanX) * frameWidth),
    y: padding + (((maxY - point.y) / spanY) * frameHeight),
  });
  const mapPosition = position => mapRaw(rawPosition(position));
  const mapPath = path => validPositions(path).map(mapPosition);
  return {
    width,
    height,
    stationary,
    metersPerPixel: (111195.0802335 * spanX) / frameWidth,
    mapPosition,
    mapPath,
  };
}

function previewSvgPoints(points) {
  return (Array.isArray(points) ? points : [])
    .map(point => `${Number(point?.x).toFixed(2)},${Number(point?.y).toFixed(2)}`)
    .join(" ");
}

function formatPreviewScaleDistance(distanceMeters) {
  if (distanceMeters >= 1000) {
    return formatScaleDistance(distanceMeters);
  }
  if (distanceMeters >= 1) {
    return distanceMeters < 10
      ? `${Number(distanceMeters.toFixed(1))} m`
      : `${Math.round(distanceMeters)} m`;
  }
  if (distanceMeters >= 0.01) {
    return `${Number((distanceMeters * 100).toFixed(1))} cm`;
  }
  return `${Number((distanceMeters * 1000).toFixed(1))} mm`;
}

function previewScaleBarSvg(geometry, targetWidth = 90) {
  const metersPerPixel = finiteOrNull(geometry?.metersPerPixel);
  if (metersPerPixel === null || metersPerPixel <= 0) {
    return "";
  }
  const distanceMeters = niceScaleDistance(metersPerPixel * targetWidth);
  const width = Math.max(1, distanceMeters / metersPerPixel);
  const left = 18;
  const right = left + width;
  const barY = 143;
  return `
    <g aria-label="${escapeHtml(`Scale ${formatPreviewScaleDistance(distanceMeters)}`)}">
      <rect x="12" y="119" width="${(width + 12).toFixed(2)}" height="33" rx="4" fill="#f8fafc" fill-opacity="0.82"/>
      <text x="${((left + right) / 2).toFixed(2)}" y="132" text-anchor="middle" fill="#334155" font-size="10" font-weight="700">${escapeHtml(formatPreviewScaleDistance(distanceMeters))}</text>
      <path d="M ${left} ${barY - 5} V ${barY + 5} M ${left} ${barY} H ${right.toFixed(2)} M ${right.toFixed(2)} ${barY - 5} V ${barY + 5}" fill="none" stroke="#334155" stroke-width="2"/>
    </g>
  `;
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
      origin: String(
        item.origin
        || ((item.issue_field || item.issueField || item.issue_threshold || item.issueThreshold) ? "threshold" : "manual"),
      ).trim(),
      stepId: String(item.step_id || item.stepId || "").trim(),
      sourceAnalysisId: String(item.source_analysis_id || item.sourceAnalysisId || "").trim(),
      scopeKind: String(item.scope_kind || item.scopeKind || "").trim(),
      scopeBurstId: String(item.scope_burst_id || item.scopeBurstId || "").trim(),
      parentAnnotationId: String(item.parent_annotation_id || item.parentAnnotationId || "").trim(),
      annotationKind: String(item.annotation_kind || item.annotationKind || "issue").trim(),
      parentIssueId: String(item.parent_issue_id || item.parentIssueId || "").trim(),
      resolutionIssueId: String(item.resolution_issue_id || item.resolutionIssueId || "").trim(),
      resolutionStepId: String(item.resolution_step_id || item.resolutionStepId || "").trim(),
      resolutionUser: String(item.resolution_user || item.resolutionUser || "").trim(),
      resolutionNote: String(item.resolution_note || item.resolutionNote || "").trim(),
      resolvedAt: String(item.resolved_at || item.resolvedAt || "").trim(),
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
    origin: String(review?.origin || "").trim(),
    stepId: String(review?.step_id || review?.stepId || "").trim(),
    sourceAnalysisId: String(review?.source_analysis_id || review?.sourceAnalysisId || "").trim(),
    scopeKind: String(review?.scope_kind || review?.scopeKind || "").trim(),
    scopeBurstId: String(review?.scope_burst_id || review?.scopeBurstId || "").trim(),
    parentAnnotationId: String(review?.parent_annotation_id || review?.parentAnnotationId || "").trim(),
    annotationKind: String(review?.annotation_kind || review?.annotationKind || "issue").trim(),
  };
  return legacyIssue.issueId || legacyIssue.issueType ? [legacyIssue] : [];
}

function normalizeSegmentMemberships(items) {
  return Array.isArray(items) ? items
    .filter(item => item && typeof item === "object")
    .map(item => ({
      status: String(item.status || "").trim(),
      segmentId: String(item.segment_id || item.segmentId || "").trim(),
      issueType: String(item.issue_type || item.issueType || "").trim(),
      startFixKey: String(item.start_fix_key || item.startFixKey || "").trim(),
      endFixKey: String(item.end_fix_key || item.endFixKey || "").trim(),
      selectionMethod: String(item.selection_method || item.selectionMethod || "").trim(),
      issueNote: String(item.issue_note || item.issueNote || "").trim(),
      ownerQuestion: String(item.owner_question || item.ownerQuestion || "").trim(),
      reviewUser: String(item.review_user || item.reviewUser || "").trim(),
      reviewedAt: String(item.reviewed_at || item.reviewedAt || "").trim(),
    }))
    .filter(item => item.segmentId)
    : [];
}

function refreshMovementFixCollections(data) {
  if (!data) {
    return;
  }
  const merged = new Map();
  for (const fix of [
    ...(data.overviewFixes || []),
    ...(data.candidateFixes || []),
    ...(data.suspiciousFixes || []),
    ...(data.detailFixes || []),
    ...(data.confirmedFixes || []),
  ]) {
    merged.set(fix.fixKey, fix);
  }
  data.fixes = Array.from(merged.values())
    .sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
  data.fixByKey = new Map(data.fixes.map(fix => [fix.fixKey, fix]));
  data.confirmedPointFixes = data.fixes.filter(fix => fix.review?.status === "confirmed");
  data.eligibleFixesByTrack = buildMovementFixTrackIndex(data.fixes);
  data.allFixesByTrack = buildMovementFixTrackIndex(data.fixes, { includeExcluded: true });
  data.eligibleTrackPositionByFixKey = buildMovementTrackPositionLookup(data.eligibleFixesByTrack);
  data.allTrackPositionByFixKey = buildMovementTrackPositionLookup(data.allFixesByTrack);
  data.flaggedStepOverlays = buildFlaggedStepOverlays(data);
  data.trackStepSegments = buildMovementTrackStepSegments(data.eligibleFixesByTrack);
  data.visiblePointCache = new Map();
  data.visibleTrackStepCache = new Map();
  const mergedSegments = new Map();
  for (const segment of [...(data.overviewSegments || []), ...(data.detailSegments || [])]) {
    mergedSegments.set(segment.segmentId, segment);
  }
  data.segments = Array.from(mergedSegments.values())
    .sort((left, right) => left.startTimeMs - right.startTimeMs || left.segmentId.localeCompare(right.segmentId));
  data.segmentById = new Map(data.segments.map(segment => [segment.segmentId, segment]));
  const mergedAutoBursts = new Map();
  for (const burst of [...(data.overviewAutoBursts || []), ...(data.detailAutoBursts || [])]) {
    mergedAutoBursts.set(burst.burstId, burst);
  }
  data.autoBursts = Array.from(mergedAutoBursts.values())
    .sort((left, right) => left.startTimeMs - right.startTimeMs || left.burstId.localeCompare(right.burstId));
  data.autoBurstById = new Map(data.autoBursts.map(burst => [burst.burstId, burst]));
  data.autoBurstRenderCache = new Map(data.autoBursts.map(burst => {
    const sourceFlagged = isSourceOnlyFlaggedBurstFromData(data, burst);
    return [
      burst.burstId,
      {
        pathItem: {
          burst,
          path: burst.path,
          color: burstPathColor(data.individualPalette, burst, 185),
          sourceFlagged,
        },
      },
    ];
  }));
  data.colorStyles = computeMovementColorStyles(
    data.colorFields,
    data.fixes.filter(fix => (
      !fix.analyticallyExcluded && fix.review?.status !== "confirmed"
    )),
  );
}

function buildMovementFixTrackIndex(fixes, { includeExcluded = false } = {}) {
  const byTrack = new Map();
  for (const fix of fixes) {
    if (!includeExcluded && (fix.analyticallyExcluded || fix.review?.status === "confirmed")) {
      continue;
    }
    const key = movementTrackKey(fix.individual, fix.setName);
    const trackFixes = byTrack.get(key) || [];
    trackFixes.push(fix);
    byTrack.set(key, trackFixes);
  }
  for (const trackFixes of byTrack.values()) {
    trackFixes.sort((left, right) => left.timeMs - right.timeMs || left.fixKey.localeCompare(right.fixKey));
  }
  return byTrack;
}

function buildMovementTrackStepSegments(byTrack) {
  const steps = [];
  for (const [trackKey, fixes] of byTrack || []) {
    for (let index = 1; index < fixes.length; index += 1) {
      const previous = fixes[index - 1];
      const destinationFix = fixes[index];
      steps.push({
        stepKey: `${previous.fixKey}->${destinationFix.fixKey}`,
        trackKey,
        individual: destinationFix.individual,
        setName: destinationFix.setName,
        sourceFix: previous,
        destinationFix,
        sourceFlagged: isSourceOnlyFlaggedFix(previous) || isSourceOnlyFlaggedFix(destinationFix),
        path: [previous.position, destinationFix.position],
      });
    }
  }
  return steps;
}

function buildMovementTrackPositionLookup(byTrack) {
  const positions = new Map();
  for (const [trackKey, fixes] of byTrack || []) {
    fixes.forEach((fix, index) => positions.set(fix.fixKey, { trackKey, index }));
  }
  return positions;
}

function buildFlaggedStepOverlays(data) {
  const overlays = [];
  const seen = new Set();
  const addInboundStep = (fix, status, byTrack, positionByFixKey) => {
    const position = positionByFixKey.get(fix.fixKey);
    if (!position || position.index <= 0) return;
    const track = byTrack.get(position.trackKey) || [];
    const previous = track[position.index - 1];
    if (!previous) return;
    const stepKey = `${status}:${previous.fixKey}->${fix.fixKey}`;
    if (seen.has(stepKey)) return;
    seen.add(stepKey);
    overlays.push({
      stepKey,
      fixKey: fix.fixKey,
      individual: fix.individual,
      setName: fix.setName,
      status,
      path: [previous.position, fix.position],
    });
  };
  for (const fix of data.fixes || []) {
    const status = String(fix.review?.status || "").trim().toLowerCase();
    const effectiveIssues = Array.isArray(fix.review?.effectiveIssues)
      ? fix.review.effectiveIssues.filter(issue => issue.status === status)
      : [];
    const needsFixInboundStep = !effectiveIssues.length
      || effectiveIssues.some(issue => issue.scopeKind !== "segment");
    if (!needsFixInboundStep) continue;
    if (status === "suspected") {
      addInboundStep(fix, status, data.eligibleFixesByTrack, data.eligibleTrackPositionByFixKey);
    } else if (status === "confirmed") {
      addInboundStep(fix, status, data.allFixesByTrack, data.allTrackPositionByFixKey);
    }
  }
  return overlays;
}

function isSourceOnlyFlaggedBurstFromData(data, burst) {
  const fixKeys = Array.isArray(burst?.fixKeys) ? burst.fixKeys : [];
  if (!fixKeys.length || !data?.fixByKey) {
    return false;
  }
  const fixes = fixKeys.map(fixKey => data.fixByKey.get(fixKey)).filter(Boolean);
  return fixes.length === fixKeys.length && fixes.every(isSourceOnlyFlaggedFix);
}

function buildMovementColorFields(fields) {
  const merged = [
    INDIVIDUAL_COLOR_FIELD,
    GPS_SPIKE_COLOR_FIELD,
    ...(Array.isArray(fields) ? fields : []),
  ];
  const seen = new Set();
  return merged.filter(field => {
    const key = String(field?.key || "");
    if (!key || seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function computeMovementColorStyles(colorFields, fixes) {
  const colorStyles = new Map();
  for (const field of colorFields) {
    if (field.kind === "numeric") {
      const values = fixes
        .map(fix => finiteOrNull(movementColorFieldValue(fix, field)))
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
      const raw = movementColorFieldValue(fix, field);
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

function movementColorFieldValue(fix, field) {
  if (!fix || !field) return null;
  const attributeKey = field.key === GPS_SPIKE_COLOR_FIELD_KEY
    ? "step_length_m"
    : field.key;
  return fix.attributes?.[attributeKey];
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

function reviewDecisionLabel(value) {
  if (value === "ok") return "OK";
  if (value === "fix_keep") return "Fix & Keep";
  if (value === "remove") return "Remove";
  return "Undecided";
}

function reviewDecisionClass(value, needsCheck = false) {
  if (value === "remove") return "remove";
  if (value === "fix_keep") return "issues";
  if (needsCheck) return "needs-check";
  return "ok";
}

function formatSelectionMethod(value) {
  if (value === "map_double_click") return "Map endpoints";
  if (value === "table_shift_click") return "Table range";
  return "Track range";
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

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function cssEscape(value) {
  const raw = String(value ?? "");
  if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
    return CSS.escape(raw);
  }
  return raw.replace(/["\\]/g, "\\$&");
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
    .map(fix => finiteOrNull(movementColorFieldValue(fix, effectiveField)))
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

function sameArrayItems(left, right) {
  if (left === right) {
    return true;
  }
  if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
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
  const effectiveIssues = Array.isArray(fix?.review?.effectiveIssues)
    ? fix.review.effectiveIssues.filter(issue => issue.status !== "dismissed")
    : [];
  const issues = effectiveIssues.length
    ? effectiveIssues
    : Array.isArray(fix?.review?.issues) ? fix.review.issues : [];
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
  const effectiveIssues = Array.isArray(fix?.review?.effectiveIssues)
    ? fix.review.effectiveIssues.filter(issue => issue.status !== "dismissed")
    : [];
  const issues = effectiveIssues.length
    ? effectiveIssues
    : Array.isArray(fix?.review?.issues) ? fix.review.issues : [];
  const issueIds = uniqueNonEmpty(issues.map(issue => issue.parentIssueId || issue.issueId || ""));
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
      logicalName: item.logical_name,
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

function metersPerCanvasPixel(map, sourceCanvas) {
  const latitude = map?.getCenter?.()?.lat || 0;
  const zoom = map?.getZoom?.() || 0;
  const container = map?.getContainer?.();
  const cssWidth = Math.max(1, container?.clientWidth || sourceCanvas.width || 1);
  const canvasWidth = Math.max(1, sourceCanvas?.width || cssWidth);
  const metersPerCssPixel = (156543.03392 * Math.cos((latitude * Math.PI) / 180)) / (2 ** zoom);
  return metersPerCssPixel * (cssWidth / canvasWidth);
}

function niceScaleDistance(maxDistanceMeters) {
  if (!Number.isFinite(maxDistanceMeters) || maxDistanceMeters <= 0) {
    return 100;
  }
  const magnitude = 10 ** Math.floor(Math.log10(maxDistanceMeters));
  let best = magnitude;
  for (const multiplier of [1, 2, 5, 10]) {
    const candidate = magnitude * multiplier;
    if (candidate <= maxDistanceMeters) {
      best = candidate;
      continue;
    }
    break;
  }
  return best;
}

function formatScaleDistance(distanceMeters) {
  if (distanceMeters >= 1000) {
    const km = distanceMeters / 1000;
    return `${Number.isInteger(km) ? km.toFixed(0) : km.toFixed(1)} km`;
  }
  return `${Math.round(distanceMeters)} m`;
}

function renderSnapshotCanvasWithOverlays(sourceCanvas, map, { showGrid = false, attributionText = "" } = {}) {
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
  const bottomBand = Math.max(34 * scaleY, 28);
  const leftBand = showGrid ? Math.max(66 * scaleX, 56) : 0;
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
  ctx.fillStyle = "rgba(255, 255, 255, 0.74)";
  ctx.fillRect(0, height - bottomBand, width, bottomBand);
  if (showGrid) {
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
  }

  ctx.setLineDash([]);
  ctx.fillStyle = "#243b53";
  ctx.strokeStyle = "#243b53";
  ctx.font = `${Math.max(12, 12 * scaleY)}px Arial, sans-serif`;
  if (showGrid) {
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
  }

  const maxScaleBarPx = Math.max(80, width * 0.18);
  const metersPerPixel = metersPerCanvasPixel(map, sourceCanvas);
  const scaleDistance = niceScaleDistance(metersPerPixel * maxScaleBarPx);
  const scaleBarWidth = Math.max(40, scaleDistance / Math.max(metersPerPixel, 0.000001));
  const barRight = width - (18 * scaleX);
  const barLeft = Math.max(leftBand + (20 * scaleX), barRight - scaleBarWidth);
  const barY = height - (bottomBand * 0.62);
  ctx.lineWidth = Math.max(2, 2.3 * Math.min(scaleX, scaleY));
  ctx.strokeStyle = "#102a43";
  ctx.beginPath();
  ctx.moveTo(barLeft, barY);
  ctx.lineTo(barRight, barY);
  ctx.moveTo(barLeft, barY - (6 * scaleY));
  ctx.lineTo(barLeft, barY + (6 * scaleY));
  ctx.moveTo(barRight, barY - (6 * scaleY));
  ctx.lineTo(barRight, barY + (6 * scaleY));
  ctx.stroke();
  ctx.textAlign = "center";
  ctx.textBaseline = "bottom";
  ctx.fillText(formatScaleDistance(scaleDistance), (barLeft + barRight) / 2, barY - (8 * scaleY));

  if (attributionText) {
    ctx.textAlign = "left";
    ctx.textBaseline = "alphabetic";
    ctx.font = `${Math.max(10, 10 * scaleY)}px Arial, sans-serif`;
    ctx.fillText(attributionText, leftBand + (10 * scaleX), height - (bottomBand * 0.2));
  }
  ctx.restore();
  return outputCanvas.toDataURL("image/png");
}

function getReportSnapshotFitPadding(snapshotWindow) {
  if (snapshotWindow?.showGrid) {
    return {
      top: 22,
      right: 22,
      bottom: 50,
      left: 78,
    };
  }
  return 36;
}

async function createReportSnapshotRenderer({ preset }) {
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
    style: preset?.snapshotStyle || preset?.style || LOCAL_BLANK_STYLE,
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
      const bounds = buildWindowBounds(snapshotWindow.windowFixes, {
        tight: snapshotWindow?.snapshotKind === "individual_profile",
      });
      overlay.setProps({ layers: buildReportSnapshotLayers(snapshotWindow) });
      if (bounds) {
        map.fitBounds(bounds, {
          padding: getReportSnapshotFitPadding(snapshotWindow),
          duration: 0,
          maxZoom: 15,
        });
      }
      map.triggerRepaint();
      const mapReady = await waitForMapReady(map, REPORT_SNAPSHOT_IDLE_TIMEOUT_MS);
      if (!mapReady) {
        throw new Error("Map idle timeout");
      }
      await waitForAnimationFrames(2);
      try {
        return renderSnapshotCanvasWithOverlays(map.getCanvas(), map, {
          showGrid: snapshotWindow?.showGrid === true,
          attributionText: preset?.attributionText || "",
        });
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

function buildWindowBounds(windowFixes, { tight = false } = {}) {
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
  const padFraction = tight ? 0.08 : 0.15;
  const minPad = tight ? 0.001 : 0.002;
  const lonPad = Math.max((maxLon - minLon) * padFraction, minPad);
  const latPad = Math.max((maxLat - minLat) * padFraction, minPad);
  return new maplibregl.LngLatBounds(
    [minLon - lonPad, minLat - latPad],
    [maxLon + lonPad, maxLat + latPad],
  );
}

function waitForAbortableDelay(delayMs, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Request aborted", "AbortError"));
      return;
    }
    const timeoutId = window.setTimeout(() => {
      signal?.removeEventListener("abort", handleAbort);
      resolve();
    }, delayMs);
    const handleAbort = () => {
      window.clearTimeout(timeoutId);
      signal?.removeEventListener("abort", handleAbort);
      reject(new DOMException("Request aborted", "AbortError"));
    };
    signal?.addEventListener("abort", handleAbort, { once: true });
  });
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

function burstPathColor(individualPalette, burst, alpha = 200) {
  const rgb = individualPalette?.[burst?.individual] || [124, 210, 255];
  return splitColor(rgb, burst?.setName || "train", alpha);
}

function movementTrackKey(individual, setName) {
  return `${String(individual || "")}\u0000${String(setName || "train")}`;
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

function nearestTrackFixIndex(fixes, currentTimeMs) {
  if (!Array.isArray(fixes) || !fixes.length) return -1;
  if (currentTimeMs <= fixes[0].timeMs) return 0;
  const lastIndex = fixes.length - 1;
  if (currentTimeMs >= fixes[lastIndex].timeMs) return lastIndex;
  let low = 0;
  let high = lastIndex;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (fixes[middle].timeMs < currentTimeMs) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  const rightIndex = low;
  const leftIndex = rightIndex - 1;
  return currentTimeMs - fixes[leftIndex].timeMs
    <= fixes[rightIndex].timeMs - currentTimeMs
    ? leftIndex
    : rightIndex;
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
