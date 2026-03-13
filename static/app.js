const state = {
  loading: false,
  error: "",
  projects: [],
  projectName: "",
  graph: null,
  datasets: [],
  selectedDataset: null,
  selectedDatasetId: "",
  selectedArtifact: "",
  readme: "",
  readmeError: "",
};

const appRoot = document.getElementById("app");
const headerControlsRoot = document.getElementById("header-controls");

function shortId(value) {
  if (!value) {
    return "none";
  }
  const parts = String(value).split("_");
  return parts[parts.length - 1];
}

function formatDateTime(value) {
  if (!value) {
    return "Unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function datasetLabel(dataset) {
  const parts = [shortId(dataset.dataset_id), formatDateTime(dataset.created_at)];
  if (dataset.dataset_id === state.graph?.current_dataset_id) {
    parts.unshift("head");
  }
  return parts.join(" | ");
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `${response.status} ${response.statusText}`);
  }
  return payload;
}

function makeElement(tagName, className, text) {
  const element = document.createElement(tagName);
  if (className) {
    element.className = className;
  }
  if (text !== undefined) {
    element.textContent = text;
  }
  return element;
}

function artifactNames() {
  return (state.selectedDataset?.artifacts || []).map((artifact) => artifact.logical_name);
}

async function loadProjects(preferredProjectName = state.projectName) {
  state.loading = true;
  state.error = "";
  render();
  try {
    const payload = await fetchJson("/api/projects");
    state.projects = payload.projects || [];
    if (!state.projects.length) {
      state.projectName = "";
      state.graph = null;
      state.datasets = [];
      state.selectedDataset = null;
      state.selectedDatasetId = "";
      state.selectedArtifact = "";
      return;
    }
    const nextProjectName = state.projects.some((project) => project.name === preferredProjectName)
      ? preferredProjectName
      : state.projects[0].name;
    await loadProject(nextProjectName);
  } catch (error) {
    state.error = error.message;
  } finally {
    state.loading = false;
    render();
  }
}

async function loadReadme() {
  state.readmeError = "";
  try {
    const response = await fetch("/starter-readme");
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText}`);
    }
    state.readme = await response.text();
  } catch (error) {
    state.readmeError = error.message;
  } finally {
    render();
  }
}

async function loadProject(projectName, preferredDatasetId = state.selectedDatasetId, preferredArtifact = state.selectedArtifact) {
  state.loading = true;
  state.error = "";
  render();
  try {
    const graph = await fetchJson(`/api/project/${encodeURIComponent(projectName)}/graph`);
    state.projectName = projectName;
    state.graph = graph;
    state.datasets = [...(graph.datasets || [])].sort((left, right) => {
      const leftValue = left.created_at || "";
      const rightValue = right.created_at || "";
      return rightValue.localeCompare(leftValue);
    });
    const nextDatasetId = state.datasets.some((dataset) => dataset.dataset_id === preferredDatasetId)
      ? preferredDatasetId
      : (graph.current_dataset_id || state.datasets[0]?.dataset_id || "");
    await loadDataset(nextDatasetId, preferredArtifact);
  } catch (error) {
    state.error = error.message;
  } finally {
    state.loading = false;
    render();
  }
}

async function loadDataset(datasetId, preferredArtifact = state.selectedArtifact) {
  if (!state.projectName || !datasetId) {
    state.selectedDataset = null;
    state.selectedDatasetId = "";
    state.selectedArtifact = "";
    return;
  }
  const dataset = await fetchJson(`/api/project/${encodeURIComponent(state.projectName)}/dataset/${encodeURIComponent(datasetId)}`);
  state.selectedDataset = dataset;
  state.selectedDatasetId = datasetId;
  const nextArtifact = artifactNames().includes(preferredArtifact)
    ? preferredArtifact
    : (artifactNames()[0] || "");
  state.selectedArtifact = nextArtifact;
}

function buildField(labelText, control) {
  const field = makeElement("div", "field");
  field.append(makeElement("label", "", labelText));
  field.append(control);
  return field;
}

function buildProjectSelect() {
  const select = document.createElement("select");
  select.disabled = state.loading || !state.projects.length;
  if (!state.projects.length) {
    const option = document.createElement("option");
    option.textContent = state.loading ? "Loading projects..." : "No projects";
    select.append(option);
    return buildField("Project", select);
  }
  state.projects.forEach((project) => {
    const option = document.createElement("option");
    option.value = project.name;
    option.textContent = project.name;
    option.selected = project.name === state.projectName;
    select.append(option);
  });
  select.addEventListener("change", (event) => {
    loadProject(event.target.value);
  });
  return buildField("Project", select);
}

function buildDatasetSelect() {
  const select = document.createElement("select");
  select.disabled = state.loading || !state.datasets.length;
  if (!state.datasets.length) {
    const option = document.createElement("option");
    option.textContent = "No versions";
    select.append(option);
    return buildField("Version", select);
  }
  state.datasets.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = datasetLabel(dataset);
    option.selected = dataset.dataset_id === state.selectedDatasetId;
    select.append(option);
  });
  select.addEventListener("change", (event) => {
    loadProject(state.projectName, event.target.value, state.selectedArtifact);
  });
  return buildField("Version", select);
}

function buildArtifactSelect() {
  const select = document.createElement("select");
  const names = artifactNames();
  select.disabled = state.loading || !names.length;
  if (!names.length) {
    const option = document.createElement("option");
    option.textContent = "No artifacts";
    select.append(option);
    return buildField("Artifact", select);
  }
  names.forEach((logicalName) => {
    const option = document.createElement("option");
    option.value = logicalName;
    option.textContent = logicalName;
    option.selected = logicalName === state.selectedArtifact;
    select.append(option);
  });
  select.addEventListener("change", (event) => {
    state.selectedArtifact = event.target.value;
  });
  return buildField("Artifact", select);
}

function renderHeaderControls() {
  headerControlsRoot.replaceChildren();
  headerControlsRoot.append(buildProjectSelect(), buildDatasetSelect(), buildArtifactSelect());
}

function renderBody() {
  appRoot.replaceChildren();
  const stack = makeElement("div", "stack");
  if (state.error) {
    const message = makeElement("div", "message error");
    message.append(makeElement("strong", "", "Failed to load starter app data."));
    message.append(document.createElement("br"));
    message.append(document.createTextNode(state.error));
    stack.append(message);
  }
  if (!state.projects.length && !state.loading) {
    const message = makeElement("div", "message");
    message.append(makeElement("strong", "", "No projects yet."));
    message.append(document.createElement("br"));
    message.append(document.createTextNode("Create "));
    message.append(makeElement("code", "", "data/my_project/"));
    message.append(document.createTextNode(" and add top-level files."));
    stack.append(message);
  }

  const readmePanel = makeElement("section", "readme-panel");
  readmePanel.append(makeElement("div", "readme-header", "README"));
  const readmeBody = makeElement("div", "readme-body");
  const pre = document.createElement("pre");
  if (state.readmeError) {
    pre.textContent = `Failed to load README.\n\n${state.readmeError}`;
  } else if (state.readme) {
    pre.textContent = state.readme;
  } else {
    pre.textContent = "Loading README...";
  }
  readmeBody.append(pre);
  readmePanel.append(readmeBody);
  stack.append(readmePanel);

  appRoot.append(stack);
}

function render() {
  renderHeaderControls();
  renderBody();
}

loadProjects();
loadReadme();
