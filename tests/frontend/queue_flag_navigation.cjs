const assert = require('node:assert/strict');
const fs = require('node:fs');
const source = fs.readFileSync(process.argv[2], 'utf8').replace(/\r\n/g, '\n');
const start = source.indexOf('function buildQueueFlagNavigation(');
const end = source.indexOf('\nfunction parseMovementFixes(', start);
assert.ok(start >= 0 && end > start);
const build = new Function(`${source.slice(start, end)}; return buildQueueFlagNavigation;`)();

function fixture(records, csv = false) {
  const arrays = {
    artifact_codes: records.map(r => r.file || 0),
    source_rows: records.map(r => r.row),
    set_codes: records.map(r => r.set || 0),
    burst_values: records.map(r => r.burst || 1),
    time_ms: records.map(r => r.time),
    positions: records.flatMap(r => [r.row, 0]),
  };
  const keys = records.map(r => csv ? `event-${r.file || 0}-${r.row}` : `file:${r.file ? 'b' : 'a'}.rds#row:${r.row}`);
  if (csv) {
    const parts = keys.map(key => new TextEncoder().encode(key));
    let offset = 0;
    arrays.fix_key_offsets = parts.map(part => { const start = offset; offset += part.length; return start; });
    arrays.fix_key_offsets.push(offset);
    arrays.fix_key_bytes = Uint8Array.from(parts.flatMap(part => [...part]));
  }
  const binary = {header: {artifacts: ['a.rds', 'b.rds']}, arrays};
  const sequence = Uint32Array.from(records.map((_, i) => i));
  sequence.sort((a, b) => records[a].time - records[b].time || a - b);
  const fixes = records.filter(r => r.flag).map(r => {
    const index = records.indexOf(r);
    return {fixKey: keys[index], timeMs: r.time};
  });
  return {binary, sequence, fixes};
}

for (const csv of [false, true]) {
  // Interleaved source files with equal times/row numbers, a missing flag,
  // nonconsecutive source row IDs, source bursts, train/test sets and duplicate
  // timestamps must not join unrelated stretches or seek by timestamp alone.
  const data = fixture([
    {row: 10, time: 1000, flag: true}, {row: 20, time: 2000, flag: true},
    {row: 30, time: 3000}, {row: 40, time: 4000, flag: true},
    {row: 50, time: 5000, burst: 2, flag: true},
    {row: 60, time: 5000, burst: 2, flag: true},
    {row: 70, time: 6000, set: 1, flag: true},
    {row: 10, time: 1000, file: 1, flag: true},
    {row: 20, time: 2000, file: 1, flag: true},
  ], csv);
  const keys = data.fixes.map(f => f.fixKey).reverse();
  const groups = [{issue_type: 'Stationarity', fix_keys: keys},
    {issue_type: 'GPS spike', fix_keys: keys.slice(0, 1)}];
  const result = build(data.binary, data.sequence, groups, data.fixes);
  assert.deepEqual(result[0].segments.map(s => s.fixes.map(f => [f.sourceArtifact, f.sourceRow])), [
    [['a.rds', 10], ['a.rds', 20]], [['b.rds', 10], ['b.rds', 20]],
    [['a.rds', 40]], [['a.rds', 50]], [['a.rds', 60]], [['a.rds', 70]],
  ]);
  assert.equal(result[0].fixes.length, 8);
  assert.equal(result[1].fixes.length, 1); // Overlaps remain listed per issue.
  for (const {fixes: [fix]} of result[0].fixes) {
    const index = data.sequence[fix.sequenceIndex];
    assert.equal(data.binary.arrays.source_rows[index], fix.sourceRow);
    assert.equal(data.binary.header.artifacts[data.binary.arrays.artifact_codes[index]], fix.sourceArtifact);
  }
}

// The jump list has no preview cap: a long flagged tail stays one segment,
// but every constituent fix is still individually reachable.
const large = fixture(Array.from({length: 5301}, (_, i) => ({row: i + 1, time: i * 1000, flag: true})));
const groups = [{issue_type: 'Stationarity', fix_keys: large.fixes.map(f => f.fixKey)}];
const result = build(large.binary, large.sequence, groups, large.fixes)[0];
assert.equal(result.segments.length, 1);
assert.equal(result.segments[0].fixes.length, 5301);
assert.equal(result.fixes.at(-1).fixes[0].sourceRow, 5301);
assert.equal(build(large.binary, large.sequence, [], []).length, 0);

// Detector periods span bursts and skipped GPS fixes, but remain separate
// across real detector boundaries even when source rows are consecutive.
const periodData = fixture([
  {row: 1, time: 0, flag: true}, {row: 2, time: 1000},
  {row: 3, time: 2000, burst: 2, flag: true}, {row: 4, time: 3000, burst: 3, flag: true},
  {row: 5, time: 4000, burst: 3, flag: true}, {row: 6, time: 5000, burst: 4, flag: true},
]);
const keys = periodData.fixes.map(f => f.fixKey);
const periodGroups = [{issue_type: 'Stationarity', fix_keys: keys, stationary_periods: [
  {fix_keys: keys.slice(0, 3), start_ms: 0, end_ms: 3000},
  {fix_keys: keys.slice(3), start_ms: 4000, end_ms: 5000},
]}];
const periods = build(periodData.binary, periodData.sequence, periodGroups, periodData.fixes)[0];
assert.deepEqual(periods.segments.map(s => s.fixes.map(f => f.sourceRow)), [[1,3,4], [5,6]]);
assert.ok(periods.segments.every(s => s.kind === 'stationary-period'));
assert.equal(periods.fixes.length, 5);
