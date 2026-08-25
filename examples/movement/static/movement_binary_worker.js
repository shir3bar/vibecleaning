const movements = new Map();
const GPS_SPIKE_FIELD_KEY = "gps_spike_step_turn";
const MOVE2_OUTBOUND_FIELDS = new Set([
  "step_length_m",
  "speed_mps",
  "time_delta_s",
  GPS_SPIKE_FIELD_KEY,
]);

function parseMovementBinary(buffer) {
  const bytes = new Uint8Array(buffer);
  if (bytes.length < 8 || String.fromCharCode(...bytes.subarray(0, 4)) !== "VCM1") {
    throw new Error("The movement binary response has an invalid header.");
  }
  const headerLength = new DataView(buffer).getUint32(4, true);
  const headerEnd = 8 + headerLength;
  const header = JSON.parse(new TextDecoder().decode(bytes.subarray(8, headerEnd)));
  if (
    header.format !== "vibecleaning-movement-columns"
    || ![1, 2].includes(Number(header.version))
  ) {
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
    if (!ArrayType) throw new Error(`Unsupported movement binary dtype ${metadata?.dtype}.`);
    arrays[name] = new ArrayType(
      buffer,
      dataOffset + Number(metadata.offset || 0),
      Number(metadata.length || 0),
    );
  }
  return { header, arrays };
}

function numericColor(value, range) {
  if (!Number.isFinite(value)) return [120, 136, 153, 120];
  const requestedMin = Number(range?.min);
  const requestedMax = Number(range?.max);
  const min = Number.isFinite(requestedMin) ? requestedMin : 0;
  const max = Number.isFinite(requestedMax) && requestedMax !== min
    ? requestedMax
    : min + 1;
  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const start = [76, 196, 255];
  const middle = [255, 214, 92];
  const end = [242, 80, 103];
  const [from, to, local] = ratio < 0.5
    ? [start, middle, ratio * 2]
    : [middle, end, (ratio - 0.5) * 2];
  return [
    Math.round(from[0] + ((to[0] - from[0]) * local)),
    Math.round(from[1] + ((to[1] - from[1]) * local)),
    Math.round(from[2] + ((to[2] - from[2]) * local)),
    215,
  ];
}

function burstIdAt(movement, index, individual) {
  const code = Number(movement.arrays.burst_values?.[index]);
  if (movement.header.source_format === "csv") {
    return String(movement.header.burst_ids?.[Math.max(0, code - 1)] || "");
  }
  return `${individual}:${movement.header.implicit_set || "train"}:source_${code}`;
}

function colorAt(movement, index, spec) {
  const arrays = movement.arrays;
  const field = spec.field || {};
  const individual = String(
    movement.header.individuals?.[Number(arrays.individual_codes[index])] || "",
  );
  if (!field.key || field.key === "individual") {
    const color = spec.individualColors?.[individual] || [124, 210, 255];
    return [color[0], color[1], color[2], 215];
  }
  const sourceKey = field.key === GPS_SPIKE_FIELD_KEY ? "step_length_m" : field.key;
  const column = movement.header.color_columns?.[sourceKey];
  const value = arrays[column?.array || sourceKey]?.[index];
  if (field.kind === "boolean") {
    if (Number(value) === 1) return [246, 92, 110, 215];
    if (Number(value) === 0) return [96, 201, 170, 215];
    return [120, 136, 153, 120];
  }
  if (field.kind === "numeric") return numericColor(Number(value), spec.range);
  const code = Number(value);
  const level = code > 0 ? String(column?.levels?.[code - 1] || "") : "Missing";
  return spec.categoryColors?.[level] || [120, 136, 153, 150];
}

function buildAttributes(movement, spec) {
  const arrays = movement.arrays;
  const rowCount = Number(movement.header.row_count) || 0;
  const lineCount = Number(movement.header.line_count) || 0;
  const hiddenBursts = new Set(spec.hiddenBurstIds || []);
  const threshold = spec.threshold || {};
  const thresholdLevels = new Set(threshold.selectedLevels || []);
  const pointColors = new Uint8Array(rowCount * 4);
  const pointFilter = new Uint8Array(rowCount);
  const suspectedFilter = new Uint8Array(rowCount);
  const confirmedFilter = new Uint8Array(rowCount);
  const thresholdFilter = new Uint8Array(rowCount);
  let suspectedCount = 0;
  let confirmedCount = 0;
  let thresholdCount = 0;
  for (let index = 0; index < rowCount; index += 1) {
    const individual = String(
      movement.header.individuals?.[Number(arrays.individual_codes[index])] || "",
    );
    const hidden = hiddenBursts.has(burstIdAt(movement, index, individual));
    const status = Number(arrays.review_status[index]);
    pointFilter[index] = !hidden && status !== 2 ? 1 : 0;
    suspectedFilter[index] = !hidden && status === 1 ? 1 : 0;
    confirmedFilter[index] = !hidden && status === 2 ? 1 : 0;
    if (!hidden && status !== 2 && threshold.active) {
      const field = spec.field || {};
      const sourceKey = field.key === GPS_SPIKE_FIELD_KEY ? "step_length_m" : field.key;
      const column = movement.header.color_columns?.[sourceKey];
      const value = arrays[column?.array || sourceKey]?.[index];
      if (field.kind === "boolean") {
        const level = Number(value) === 1 ? "True" : Number(value) === 0 ? "False" : "Missing";
        thresholdFilter[index] = thresholdLevels.has(level) ? 1 : 0;
      } else if (field.kind === "categorical") {
        const code = Number(value);
        const level = code > 0 ? String(column?.levels?.[code - 1] || "Missing") : "Missing";
        thresholdFilter[index] = thresholdLevels.has(level) ? 1 : 0;
      } else if (field.kind === "numeric" && Number.isFinite(Number(threshold.value))) {
        const number = Number(value);
        const turn = Number(arrays.turn_angle_deg?.[index]);
        const validTurn = field.key !== GPS_SPIKE_FIELD_KEY
          || (Number.isFinite(turn) && Math.abs(turn) >= Number(spec.gpsSpikeTurnAngleDeg));
        const matches = threshold.reverse ? number < Number(threshold.value) : number > Number(threshold.value);
        thresholdFilter[index] = Number.isFinite(number) && validTurn && matches ? 1 : 0;
      }
    }
    suspectedCount += suspectedFilter[index];
    confirmedCount += confirmedFilter[index];
    thresholdCount += thresholdFilter[index];
    const color = colorAt(movement, index, spec);
    pointColors.set(color, index * 4);
  }
  const lineColors = new Uint8Array(lineCount * 4);
  const lineFilter = new Uint8Array(lineCount);
  const burstFilter = new Uint8Array(lineCount);
  for (let index = 0; index < lineCount; index += 1) {
    const sourceIndex = Number(arrays.line_source_indexes[index]);
    const targetIndex = Number(arrays.line_target_indexes[index]);
    lineFilter[index] = pointFilter[targetIndex];
    burstFilter[index] = (
      pointFilter[targetIndex]
      && Number(arrays.burst_values[sourceIndex]) === Number(arrays.burst_values[targetIndex])
    ) ? 1 : 0;
    const colorIndex = MOVE2_OUTBOUND_FIELDS.has(String(spec.field?.key || ""))
      ? sourceIndex
      : targetIndex;
    const pointOffset = colorIndex * 4;
    const lineOffset = index * 4;
    lineColors[lineOffset] = pointColors[pointOffset];
    lineColors[lineOffset + 1] = pointColors[pointOffset + 1];
    lineColors[lineOffset + 2] = pointColors[pointOffset + 2];
    lineColors[lineOffset + 3] = (
      Number(arrays.source_flags?.[sourceIndex])
      || Number(arrays.source_flags?.[targetIndex])
    ) ? 52 : 185;
  }
  return {
    pointColors,
    pointFilter,
    suspectedFilter,
    confirmedFilter,
    thresholdFilter,
    lineColors,
    lineFilter,
    burstFilter,
    suspectedCount,
    confirmedCount,
    thresholdCount,
  };
}

self.addEventListener("message", event => {
  const requestId = event.data?.requestId;
  try {
    if (event.data?.type === "initialize") {
      const blockId = String(event.data.blockId || "");
      if (!blockId) throw new Error("Movement block ID is required.");
      const movement = parseMovementBinary(event.data.buffer);
      movements.set(blockId, movement);
      const positions = movement.arrays.positions;
      const sourceIndexes = movement.arrays.line_source_indexes;
      const targetIndexes = movement.arrays.line_target_indexes;
      const lineSourcePositions = new Float64Array(sourceIndexes.length * 2);
      const lineTargetPositions = new Float64Array(targetIndexes.length * 2);
      for (let index = 0; index < sourceIndexes.length; index += 1) {
        const sourceOffset = Number(sourceIndexes[index]) * 2;
        const targetOffset = Number(targetIndexes[index]) * 2;
        lineSourcePositions.set(positions.subarray(sourceOffset, sourceOffset + 2), index * 2);
        lineTargetPositions.set(positions.subarray(targetOffset, targetOffset + 2), index * 2);
      }
      self.postMessage({
        type: "ready",
        requestId,
        lineSourcePositions: lineSourcePositions.buffer,
        lineTargetPositions: lineTargetPositions.buffer,
      }, [lineSourcePositions.buffer, lineTargetPositions.buffer]);
      return;
    }
    if (event.data?.type === "release") {
      movements.delete(String(event.data.blockId || ""));
      return;
    }
    if (event.data?.type === "review_status") {
      const movement = movements.get(String(event.data.blockId || ""));
      if (!movement) throw new Error("Movement block is not initialized.");
      const reviewStatus = new Uint8Array(event.data.reviewStatus || []);
      if (reviewStatus.length !== movement.arrays.review_status.length) {
        throw new Error("Movement review status length does not match the block.");
      }
      movement.arrays.review_status.set(reviewStatus);
      self.postMessage({ type: "review_status", requestId });
      return;
    }
    if (event.data?.type === "attributes") {
      const movement = movements.get(String(event.data.blockId || ""));
      if (!movement) throw new Error("Movement block is not initialized.");
      const attributes = buildAttributes(movement, event.data.spec || {});
      const transfer = [
        attributes.pointColors.buffer,
        attributes.pointFilter.buffer,
        attributes.suspectedFilter.buffer,
        attributes.confirmedFilter.buffer,
        attributes.thresholdFilter.buffer,
        attributes.lineColors.buffer,
        attributes.lineFilter.buffer,
        attributes.burstFilter.buffer,
      ];
      self.postMessage({ type: "attributes", requestId, attributes }, transfer);
    }
  } catch (error) {
    self.postMessage({ type: "error", requestId, error: error?.message || String(error) });
  }
});
