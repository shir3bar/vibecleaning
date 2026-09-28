const assert = require('node:assert/strict');
const fs = require('node:fs');
const source = fs.readFileSync(process.argv[2], 'utf8').replace(/\r\n/g, '\n');
const start = source.indexOf('function formatStepLabel(');
const end = source.indexOf('\nfunction uniqueStrings(', start);
assert.ok(start >= 0 && end > start);
const {formatDatasetLabel, formatStepLabel, formatStepDetails} = new Function(
  `${source.slice(start, end)}; return {formatDatasetLabel, formatStepLabel, formatStepDetails};`
)();
const dataset = {dataset_id: 'dataset_abc123', parent_dataset_id: 'dataset_root',
  created_at: '2026-09-22T10:00:00Z', note: 'Old step title'};
const params = {action: 'annotate_scope', status: 'suspected', issue_type: 'High speed', scope: {filter: {
  field_key: 'speed_mps', field_kind: 'numeric', operator: 'gt', threshold_value: 12.5,
}}};
const fresh = {parameters: params};
const reloaded = {label_parameters: {...params, filter: params.scope.filter}};
assert.equal(formatStepLabel(fresh, dataset), 'High speed');
assert.equal(formatStepLabel(fresh, dataset), formatStepLabel(reloaded, dataset));
assert.match(formatDatasetLabel(dataset, dataset.dataset_id, fresh), /^head \| abc123 \| High speed \| /);
assert.equal(formatStepDetails(fresh, dataset), 'suspected: Speed > 12.5 m/s');
params.scope.filter.operator = 'lt';
params.scope.filter.threshold_value = 0;
assert.equal(formatStepLabel(fresh, dataset), 'High speed');
assert.equal(formatStepDetails(fresh, dataset), 'suspected: Speed < 0 m/s');
delete params.issue_type;
assert.equal(formatStepLabel(fresh, dataset), 'Speed');
params.scope.filter = {kind: 'gps_spike', step_length_threshold_m: 20000,
  minimum_abs_turn_angle_deg: 150};
assert.equal(formatStepLabel(fresh, dataset), 'GPS spike');
assert.equal(formatStepDetails(fresh, dataset), 'suspected: GPS spike: both steps > 20000 m, |turn| ≥ 150°');
params.scope.filter = {field_key: 'is_outlier', selected_levels: ['True']};
assert.equal(formatStepLabel(fresh, dataset), 'Outlier flag');
assert.equal(formatStepDetails(fresh, dataset), 'suspected: Outlier flag in True');
params.scope.filter = {kind: 'stationarity', radius_m: 100,
  minimum_duration_s: 172800, maximum_gap_s: 259200, position: 'anywhere'};
params.issue_type = 'Filter Stationarity';
assert.equal(formatStepLabel(fresh, dataset), 'Filter Stationarity');
assert.equal(formatStepLabel({label_parameters: {...params, filter: params.scope.filter}}, dataset), 'Filter Stationarity');
assert.equal(formatStepDetails(fresh, dataset), 'suspected: Stationarity: radius ≤ 100 m, duration ≥ 48 h, gaps ≤ 72 h (anywhere)');
assert.equal(formatStepLabel({parameters: {action: 'confirm_issues'}, title: 'Confirm 4 fixes'}, dataset), 'Confirm 4 fixes');
assert.equal(formatStepLabel(null, dataset), 'Old step title');
assert.equal(formatStepLabel(null, {dataset_id: 'root'}), 'Original input');
assert.equal(formatStepLabel({parameters: {action: 'annotate_scope', issue_field: 'speed_mps',
  issue_threshold: '> 5.00'}}, dataset), 'Speed');

// Exercise the actual dropdown renderer: labels stay text, IDs stay option values.
const classStart = source.indexOf('class MovementExampleApp {');
const classEnd = source.indexOf('\n}\n', classStart) + 2;
const App = new Function('formatDatasetLabel', 'formatStepDetails', 'document',
  `${source.slice(classStart, classEnd)}; return MovementExampleApp;`
)(formatDatasetLabel, formatStepDetails, {createElement: () => ({})});
const app = Object.create(App.prototype);
const options = [];
app.allDatasets = [dataset];
app.graph = {current_dataset_id: dataset.dataset_id};
app.stepByOutputDatasetId = new Map([[dataset.dataset_id, {title: '<script>not HTML</script>'}]]);
app.refs = {dataset: {appendChild: option => options.push(option)}};
app.refreshDatasetOptions(dataset.dataset_id);
assert.equal(options[0].value, dataset.dataset_id);
assert.match(options[0].textContent, /<script>not HTML<\/script>/);
assert.equal(options[0].title, '<script>not HTML</script>');
assert.equal(app.currentDatasetId, dataset.dataset_id);
app.stepByOutputDatasetId.set(dataset.dataset_id, fresh);
app.refreshDatasetOptions(dataset.dataset_id);
assert.match(options[1].textContent, /Filter Stationarity/);
assert.doesNotMatch(options[1].textContent, /radius|duration|gaps/);
assert.equal(options[1].title, formatStepDetails(fresh, dataset));
console.log('Step label tests passed');
