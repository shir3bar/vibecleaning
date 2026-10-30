// Optional PhyloPic thumbnails. No review data, backend work, or persistent cache.
const API = "https://api.phylopic.org";
const UUID = "[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}";
const cache = new Map();
const menus = new WeakSet();
const queue = [];
let active = 0;

function normalized(name) {
  return String(name || "").trim().replace(/\s+/g, " ").toLowerCase();
}

function apiUrl(href) {
  const url = new URL(href, API);
  if (url.origin !== API || url.username || url.password
      || !new RegExp(`^/(?:nodes(?:/${UUID})?|images/${UUID})$`).test(url.pathname)) {
    throw new Error("Unexpected PhyloPic URL");
  }
  return url;
}

function nodeId(link) {
  return apiUrl(link?.href).pathname.match(new RegExp(`^/nodes/(${UUID})$`))?.[1];
}

function scientificMatch(node, name) {
  return node.names?.some(parts => Array.isArray(parts) && normalized(parts
    .filter(part => part.class === "scientific").map(part => part.text).join(" ")) === name);
}

// Exported for isolated tests; all callers receive null on any provider failure.
export async function lookupSpeciesPortrait(name) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 6000);
  const get = async url => {
    const response = await fetch(apiUrl(url), {
      signal: controller.signal, credentials: "omit", referrerPolicy: "no-referrer",
      headers: {Accept: "application/vnd.phylopic.v2+json"},
    });
    if (!response.ok) throw new Error("PhyloPic unavailable");
    return response.json();
  };
  try {
    const key = normalized(name);
    if (!key || key.length > 180) return null;
    const list = await get(`${API}/nodes?filter_name=${encodeURIComponent(key)}`);
    // One result page only; broad or ambiguous matches keep the text-only entry.
    if (!list.totalItems || list.totalPages !== 1 || !list._links?.firstPage?.href) return null;
    const pageUrl = apiUrl(list._links.firstPage.href);
    pageUrl.searchParams.set("embed_items", "true");
    const page = await get(pageUrl);
    const matches = (page._embedded?.items || []).filter(node => scientificMatch(node, key));
    if (matches.length !== 1) return null;
    const node = matches[0];
    if (!new RegExp(`^${UUID}$`).test(node.uuid) || !node._links?.primaryImage?.href) return null;
    const image = await get(node._links.primaryImage.href);
    // PhyloPic can suggest an ancestor's image. Do not present it as this species.
    if (nodeId(image._links?.specificNode) !== node.uuid
        || !new RegExp(`^${UUID}$`).test(image.uuid)) return null;
    const license = new URL(image._links?.license?.href);
    const by = license.pathname.match(/^\/licenses\/(by|by-sa)\/(1\.0|2\.0|2\.5|3\.0|4\.0)\/$/);
    const cc0 = license.pathname === "/publicdomain/zero/1.0/";
    if (license.origin !== "https://creativecommons.org" || (!cc0 && !by)) return null;
    const credit = image.attribution || image._links?.contributor?.title;
    if (typeof credit !== "string" || !credit.trim() || credit.length > 500) return null;
    const thumbnails = (image._links.thumbnailFiles || []).flatMap(file => {
      const url = new URL(file.href);
      const size = url.pathname.match(new RegExp(`^/images/${image.uuid}/thumbnail/(64|128|192)x\\1\\.png$`));
      return url.origin === "https://images.phylopic.org" && !url.username && !url.password
        && file.type === "image/png" && size ? [{url: url.href, width: Number(size[1])}] : [];
    }).sort((a, b) => Math.abs(a.width - 128) - Math.abs(b.width - 128));
    if (!thumbnails.length) return null;
    return {url: thumbnails[0].url, credit: credit.trim(),
      source: `https://www.phylopic.org/images/${image.uuid}`,
      licenseUrl: license.href, license: cc0 ? "CC0" : `CC ${by[1].toUpperCase()} ${by[2]}`};
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function pump() {
  while (active < 2 && queue.length) {
    const {name, resolve} = queue.shift();
    active += 1;
    lookupSpeciesPortrait(name).then(resolve).finally(() => { active -= 1; pump(); });
  }
}

function portrait(name) {
  const key = normalized(name);
  if (!cache.has(key)) {
    if (cache.size >= 256) cache.delete(cache.keys().next().value);
    cache.set(key, new Promise(resolve => { queue.push({name: key, resolve}); pump(); }));
  }
  return cache.get(key);
}

function link(text, href) {
  const a = document.createElement("a");
  a.textContent = text;
  a.href = href;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  return a;
}

async function decorate(row, onLayout) {
  const image = await portrait(row.dataset.species);
  if (!image || !row.isConnected) return;
  const img = new Image(64, 64);
  img.alt = `Silhouette of ${row.dataset.species}`;
  img.className = "movement-species-silhouette";
  img.referrerPolicy = "no-referrer";
  img.decoding = "async";
  const timer = setTimeout(() => { img.onload = img.onerror = null; img.removeAttribute("src"); }, 4000);
  img.onerror = () => { clearTimeout(timer); img.onload = img.onerror = null; };
  img.onload = () => {
    clearTimeout(timer);
    img.onload = img.onerror = null;
    if (!row.isConnected) return;
    const credit = document.createElement("div");
    credit.className = "movement-species-credit";
    // Provider text is always plain text, never executable markup.
    credit.append(link("PhyloPic", image.source), ` · ${image.credit} · `, link(image.license, image.licenseUrl));
    row.prepend(img);
    row.append(credit);
    row.classList.add("has-species-portrait");
    onLayout();
  };
  img.src = image.url;
}

export function enhanceSpeciesMenu(menu, onLayout = () => {}) {
  if (menus.has(menu)) return;
  menus.add(menu);
  const started = new WeakSet();
  const observer = new IntersectionObserver(entries => {
    if (!menu.isConnected) { observer.disconnect(); return; }
    for (const entry of entries) {
      if (!entry.isIntersecting || !menu.open) continue;
      observer.unobserve(entry.target);
      if (started.has(entry.target)) continue;
      started.add(entry.target);
      void decorate(entry.target, onLayout).catch(() => {});
    }
  }, {root: menu.querySelector(".movement-popover-panel")});
  const observe = () => menu.querySelectorAll("[data-species]").forEach(row => {
    if (!started.has(row)) observer.observe(row);
  });
  menu.addEventListener("toggle", () => { if (menu.open) observe(); else observer.disconnect(); });
  observe();
}
