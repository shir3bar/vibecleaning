// Exercise production methods/handlers, stubbing only rendering and network I/O.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const source = fs.readFileSync(process.argv[2] === '--stdin' ? 0 : process.argv[2], 'utf8');
const profile = process.argv[3] || 'rds_movement';
const start = source.indexOf('class MovementExampleApp {');
const end = source.indexOf('\n}\n', start) + 2;
const App = new Function('MOVEMENT_APP_CONFIG', 'GPS_SPIKE_COLOR_FIELD_KEY', 'formatCount',
  `${source.slice(start, end)}; return MovementExampleApp;`
)({rdsSource: profile === 'rds_movement'}, 'gps_spike_step_turn', String);

function handler(name, next) {
  const marker = `this.refs.${name}.addEventListener("click", () => {`;
  const bodyStart = source.indexOf(marker) + marker.length;
  const bodyEnd = source.indexOf(`    this.refs.${next}.addEventListener`, bodyStart);
  assert.ok(bodyStart >= marker.length && bodyEnd > bodyStart);
  const body = source.slice(bodyStart, bodyEnd);
  return new Function(body.slice(0, body.lastIndexOf('    });')));
}
const selectAll = handler('selectAll', 'selectNone');
const selectNone = handler('selectNone', 'selectSuspicious');

function fixture(field, state) {
  const app = Object.create(App.prototype);
  app.currentDatasetId = 'dataset-1';
  app.currentArtifact = 'tracks';
  app.refs = {colorBy: {value: field.key}};
  app.individualReviewQueue = {mode: 'map'};
  app.data = {
    individuals: ['alpha', 'beta'],
    selectedIndividuals: new Set(['alpha']),
    selectedFixKeys: new Set(),
    fixByKey: new Map([['a1', {individual: 'alpha'}], ['b1', {individual: 'beta'}]]),
    colorFields: [field], colorFieldByKey: new Map([[field.key, field]]),
    binaryBlocks: new Map([['alpha', {}], ['beta', {}]]),
    binaryPendingIndividuals: new Map(),
    detailFailedIndividuals: new Set(),
  };
  app.thresholdState = structuredClone(state);
  app.flagTargetKind = 'filter';
  app.checkedThresholdSignature = '';
  app.thresholdFlagScope = 'selected_individuals';
  app.movementDiagnostics = {binaryCacheHits: 0};
  app.paneScopes = [];
  app.renderThresholdPane = () => app.paneScopes.push(app.getSelectedIndividuals());
  for (const method of ['saveUiState', 'syncIndividualSelectionUi', 'renderLayers',
    'renderSelectedFixes', 'updateActionButtons', 'cancelBinaryRequests', 'cancelRequest',
    'setStatus', 'renderBurstCountIndicator']) {
    app[method] = () => {};
  }
  return app;
}

async function main() {
  for (const [field, settings] of [
    [{key: 'speed_mps', kind: 'numeric'}, {value: 12.5, reverse: false}],
    [{key: 'speed_mps', kind: 'numeric'}, {value: 0, reverse: true}],
    [{key: 'is_outlier', kind: 'boolean'}, {value: null, selectedLevels: ['True']}],
  ]) {
    const state = {fieldKey: field.key, value: null, reverse: false,
      selectedLevels: [], histogramMode: 'clipped', histogramMin: 1, histogramMax: 8,
      ...settings};
    const app = fixture(field, state);
    const assertRetained = individuals => {
      assert.deepEqual(app.thresholdState, state);
      assert.deepEqual(app.currentThresholdFilterDefinition().individuals, individuals);
      assert.equal(app.flagTargetKind, 'filter');
      assert.deepEqual(app.paneScopes.at(-1), individuals);
    };

    app.toggleIndividual('beta', true);
    assertRetained(['alpha', 'beta']);
    app.toggleIndividual('alpha', false);
    assertRetained(['beta']);
    // Out-of-range cutoffs, reverse direction and categorical choices survive
    // the empty-selection round trip too; they are never silently clamped.
    selectNone.call(app);
    assertRetained([]);
    selectAll.call(app);
    assertRetained(['alpha', 'beta']);

    // A checked preview belongs to the old scope; do not silently retain it.
    app.checkedThresholdSignature = 'old-preview';
    app.data.selectedFixKeys = new Set(['a1', 'b1']);
    app.toggleIndividual('beta', false);
    assertRetained(['alpha']);
    assert.equal(app.checkedThresholdSignature, '');
    assert.equal(app.data.selectedFixKeys.size, 0);

    // Cached binary loads must refresh histogram/count UI for the new scope.
    app.paneScopes = [];
    await app.loadDetailForCurrentSelection();
    assertRetained(['alpha']);

    // A fresh asynchronous binary load must refresh the same retained settings.
    app.data.binaryBlocks.delete('alpha');
    let finishLoad;
    app.loadBinaryMovement = () => new Promise(resolve => {
      finishLoad = () => { app.data.binaryBlocks.set('alpha', {}); resolve(); };
    });
    const loading = app.loadDetailForCurrentSelection();
    assert.equal(typeof finishLoad, 'function');
    finishLoad();
    await loading;
    assertRetained(['alpha']);

    // No matches must not silently switch the action to old manual selections.
    app.data.selectedFixKeys = new Set(['a1']);
    app.getActiveThresholdMatchKeys = () => new Set();
    app.getThresholdContext = () => ({matchCount: 0});
    const emptyTarget = app.getActiveFlagTarget();
    assert.equal(emptyTarget.kind, 'filter');
    assert.equal(emptyTarget.ready, false);
    assert.deepEqual(emptyTarget.fixes, []);

    // Switching color field/study still uses the existing explicit reset.
    app.clearThresholdState();
    assert.equal(app.hasActiveThreshold(), false);
    assert.equal(app.thresholdState.value, null);
    assert.deepEqual(app.thresholdState.selectedLevels, []);
  }

  // Manual checked fixes are not a threshold preview: retain those still visible.
  const manual = fixture({key: 'speed_mps', kind: 'numeric'}, {
    fieldKey: '', value: null, selectedLevels: [], reverse: false,
  });
  manual.flagTargetKind = 'fixes';
  manual.data.selectedFixKeys = new Set(['a1']);
  manual.toggleIndividual('beta', true);
  assert.deepEqual([...manual.data.selectedFixKeys], ['a1']);
  manual.toggleIndividual('alpha', false);
  assert.equal(manual.data.selectedFixKeys.size, 0);
  console.log(`${profile}: threshold selection regression checks passed`);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
