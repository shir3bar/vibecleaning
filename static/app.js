let activePlugin = null;
let plugins = [];

const pluginSelect = document.getElementById("plugin-select");
const pluginSurface = document.getElementById("plugin-surface");

async function apiFetch(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || `Request failed with status ${response.status}`);
  }
  return response;
}

function showPluginPlaceholder(text, isError = false) {
  pluginSurface.innerHTML = "";
  const div = document.createElement("div");
  div.className = isError ? "plugin-error" : "plugin-placeholder";
  div.textContent = text;
  pluginSurface.appendChild(div);
}

async function mountPlugin(pluginId) {
  const plugin = plugins.find(item => item.id === pluginId);
  if (!plugin) {
    showPluginPlaceholder("No plugin selected.");
    return;
  }

  localStorage.setItem("vibecleaning.activePlugin", plugin.id);
  if (activePlugin && typeof activePlugin.destroy === "function") {
    await activePlugin.destroy();
  }

  const mountEl = document.createElement("div");
  mountEl.className = "plugin-surface";
  pluginSurface.replaceChildren(mountEl);

  try {
    const mod = await import(`${plugin.module}?v=1`);
    if (typeof mod.createPlugin !== "function") {
      throw new Error("Plugin module does not export createPlugin()");
    }
    activePlugin = await mod.createPlugin({
      mountEl,
      plugin,
      storageKey: `vibecleaning.plugin.${plugin.id}`,
      fetchJSON: async url => {
        const response = await apiFetch(url);
        return response.json();
      },
      requestJSON: async (url, options = {}) => {
        const response = await apiFetch(url, {
          ...options,
          headers: {
            "Content-Type": "application/json",
            ...(options.headers || {}),
          },
        });
        return response.json();
      },
      fetchText: async url => {
        const response = await apiFetch(url);
        return response.text();
      },
    });
  } catch (error) {
    activePlugin = null;
    showPluginPlaceholder(`Plugin load failed: ${error.message}`, true);
  }
}

async function loadPlugins() {
  try {
    const response = await fetch("/plugins/manifest.json");
    if (!response.ok) {
      throw new Error(`Manifest request failed (${response.status})`);
    }
    const data = await response.json();
    plugins = Array.isArray(data.plugins) ? data.plugins : [];
  } catch (error) {
    showPluginPlaceholder(`Plugin manifest error: ${error.message}`, true);
    return;
  }

  if (!plugins.length) {
    showPluginPlaceholder("No plugins are installed.");
    return;
  }

  pluginSelect.innerHTML = "";
  for (const plugin of plugins) {
    const option = document.createElement("option");
    option.value = plugin.id;
    option.textContent = plugin.name;
    pluginSelect.appendChild(option);
  }

  const stored = localStorage.getItem("vibecleaning.activePlugin");
  const selected = plugins.some(plugin => plugin.id === stored) ? stored : plugins[0].id;
  pluginSelect.value = selected;
  await mountPlugin(selected);
}

pluginSelect.addEventListener("change", async () => {
  await mountPlugin(pluginSelect.value);
});

loadPlugins();
