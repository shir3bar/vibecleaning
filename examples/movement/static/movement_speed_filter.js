// Shared by the review UI and its map worker. Speed filtering has no angle rule.
(() => {
  function matches(incoming, outgoing, threshold, reverse = false) {
    if (![incoming, outgoing, threshold].every(value => typeof value === "number" && Number.isFinite(value))) return false;
    return reverse
      ? incoming < threshold && outgoing < threshold
      : incoming > threshold && outgoing > threshold;
  }

  function inboundSpeed(binary, index) {
    if (!binary?.arrays || index <= 0) return null;
    const arrays = binary.arrays;
    let previous = index - 1;
    const sameTrack = i => Number(arrays.individual_codes[i]) === Number(arrays.individual_codes[index])
      && (!arrays.set_codes || Number(arrays.set_codes[i]) === Number(arrays.set_codes[index]));
    while (previous >= 0 && sameTrack(previous) && Number(arrays.review_status[previous]) === 2) previous -= 1;
    const value = previous >= 0 && sameTrack(previous) ? arrays.speed_mps?.[previous] : null;
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  }

  globalThis.MovementSpeedFilter = Object.freeze({matches, inboundSpeed});
})();
