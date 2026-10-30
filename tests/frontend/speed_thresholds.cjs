// Exercise production matching, both preview representations, and map rendering.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = process.argv[2];
const source = fs.readFileSync(path.join(root, 'app.js'), 'utf8');
const shared = fs.readFileSync(path.join(root, 'movement_speed_filter.js'), 'utf8');
const worker = vm.createContext({console, Map, Set, self: {addEventListener() {}}});
worker.importScripts = name => vm.runInContext(fs.readFileSync(path.join(root, name), 'utf8'), worker);
vm.runInContext(fs.readFileSync(path.join(root, 'movement_binary_worker.js'), 'utf8'), worker);

const context = vm.createContext({console, Map, Set,
  SPEED_SPIKE_FIELDS: {}, STATIONARITY_COLOR_FIELD_KEY: 'stationarity',
  GPS_SPIKE_COLOR_FIELD_KEY: 'gps_spike_speed_turn', INDIVIDUAL_COLOR_FIELD_KEY: 'individual',
  MAX_SELECTED_FIXES_SHOWN: 250, POINT_ALPHA: 215, CONTEXT_GRAY_POINT: [112, 122, 133, 175],
});
vm.runInContext(shared, context);
const classStart = source.indexOf('class MovementExampleApp {');
const classEnd = source.indexOf('\n}\n', classStart) + 2;
const helpers = ['finiteOrNull', 'movementInboundSpeed', 'movementColorFieldValue',
  'computeHistogramBins', 'quantile', 'uniqueNonEmpty'].map(name => {
  const start = source.indexOf(`function ${name}(`);
  assert.ok(start >= 0, name);
  return source.slice(start, source.indexOf('\n}\n', start) + 2);
}).join('\n');
vm.runInContext(`const {matches: speedThresholdMatches, inboundSpeed: binaryInboundSpeed} = MovementSpeedFilter;
  ${source.slice(classStart, classEnd)}\n${helpers}\nglobalThis.App = MovementExampleApp;`, context);

function fixture(speeds, turns, statuses, sets, individuals, reverse = false) {
  const count = speeds.length;
  const arrays = {speed_mps: Float64Array.from(speeds), turn_angle_deg: Float64Array.from(turns),
    review_status: Uint8Array.from(statuses || speeds.map(() => 0)),
    individual_codes: Uint8Array.from(individuals || speeds.map(() => 0)),
    set_codes: Uint8Array.from(sets || speeds.map(() => 0)),
    burst_values: Uint8Array.from(speeds.map(() => 1))};
  const binary = {header: {row_count: count, line_count: 0, individuals: ['A', 'B'], sets: ['train', 'test'],
    color_columns: {speed_mps: {array: 'speed_mps'}}}, arrays,
    individualRanges: new Map([[0, [0, count]]])};
  const fixes = speeds.map((speed, i) => ({fixKey: String(i), individual: 'A', setName: 'train',
    binaryBlock: binary, binaryIndex: i, attributes: {speed_mps: speed, turn_angle_deg: turns[i]}}));
  const field = {key: 'speed_mps', kind: 'numeric'};
  const app = Object.create(context.App.prototype);
  Object.assign(app, {thresholdState: {fieldKey: 'speed_mps', value: 10, reverse, selectedLevels: []},
    refs: {colorBy: {value: 'speed_mps'}}, individualReviewQueue: {mode: 'map'},
    data: {fixes, colorStyles: new Map(), binaryBlocks: new Map([['A', binary]]), selectedFixKeys: new Set()},
    hiddenBurstIds: new Set(), binaryThresholdContextCache: new Map(),
    movementDiagnostics: {binaryAttributeBuilds: 0}, gpsSpikeTurnAngleDeg: 180});
  app.getCurrentColorField = () => field;
  app.speedMetricsReady = () => true;
  app.getSelectedIndividuals = () => ['A'];
  app.getVisibleSetNames = () => new Set(['train', 'test']);
  app.getVisibleReviewFixes = () => fixes.filter((_, i) => arrays.review_status[i] !== 2);
  app.binaryRenderSpecification = () => ({cacheKey: 'test'});
  app.binaryBurstIdAt = () => 'burst';
  app.binaryColorForIndex = () => [1, 2, 3, 215];
  app.recordQueueContextAttributeCounts = () => {};
  app.binaryFixAt = index => fixes[index];
  return {app, binary, fixes, field};
}

function check(speeds, expected, options = {}) {
  for (const turns of [speeds.map(() => 0), speeds.map(() => 180), speeds.map(() => Number.NaN)]) {
    const {app, binary, fixes, field} = fixture(speeds, turns, options.statuses, options.sets, options.individuals, options.reverse);
    const selected = fixes.filter((fix, i) => binary.arrays.review_status[i] !== 2 && app.fixMatchesActiveThreshold(fix)).map(fix => fix.fixKey);
    assert.deepEqual(selected, expected.map(String), 'per-fix match');
    assert.deepEqual([...app.getBinaryThresholdContext(field).matchKeys], expected.map(String), 'binary preview');
    const fallback = app.buildRetainedBinaryAttributes(binary);
    assert.deepEqual(Array.from(fallback.thresholdFilter).flatMap((match, i) => match ? [i] : []), expected, 'map fallback');
    const attributes = worker.buildAttributes(binary, {field, threshold: {active: true, value: 10, reverse: options.reverse}, gpsSpikeTurnAngleDeg: 180});
    assert.deepEqual(Array.from(attributes.thresholdFilter).flatMap((match, i) => match ? [i] : []), expected, 'worker map');

    // Ordinary object preview uses its chronological retained-track lookup.
    app.data.binaryBlocks.clear();
    app.data.eligibleFixesByTrack = new Map([['A', fixes.filter((_, i) => binary.arrays.review_status[i] !== 2)]]);
    app.data.eligibleTrackPositionByFixKey = new Map(app.data.eligibleFixesByTrack.get('A').map((fix, i) =>
      [fix.fixKey, {trackKey: 'A', index: i}]));
    if (!options.sets && !options.individuals) {
      for (const fix of fixes) { delete fix.binaryBlock; delete fix.binaryIndex; }
      assert.deepEqual([...app.getThresholdContext().matchKeys], expected.map(String), 'object preview');
    }
  }
}

check([1, 20, Number.NaN], []); // Departure: only outgoing is high.
check([20, 1, Number.NaN], []); // Arrival: only incoming is high.
check([20, 20, Number.NaN], [1]); // Same result for straight, reversing, or missing turns.
check([10, 20, 10, Number.NaN], []); // Equality is not above.
check([20, Number.NaN, 20, Number.NaN], []); // Invalid/zero-lag step breaks the pair.
check([20, Number.NaN, 20, Number.NaN], [2], {statuses: [0, 2, 0, 0]}); // Confirmed fix skipped, metrics rebuilt.
check([1, 1, 20, Number.NaN], [1], {reverse: true});
check([20, 20, Number.NaN], [], {sets: [0, 1, 1]});
check([20, 20, Number.NaN], [], {individuals: [0, 1, 1]});
check([20], []);
console.log('Speed threshold preview, fallback and worker agree; turns do not affect matches.');
