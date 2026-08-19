const loginView = document.querySelector("#login-view");
const loginForm = document.querySelector("#login-form");
const usernameInput = document.querySelector("#login-username");
const passwordInput = document.querySelector("#login-password");
const submitButton = document.querySelector("#login-submit");
const statusText = document.querySelector("#login-status");
const appShell = document.querySelector("#app-shell");
const logoutButton = document.querySelector("#logout-button");

let applicationLoaded = false;

function showLogin(message = "") {
  loginView.hidden = false;
  appShell.hidden = true;
  statusText.textContent = message;
  passwordInput.value = "";
  usernameInput.focus();
}

async function authenticatedFetch(input, init = {}) {
  const response = await fetch(input, {
    ...init,
    cache: init.cache || "no-store",
    credentials: "same-origin",
  });
  if (response.status === 401) {
    showLogin("Your session ended. Log in to continue.");
  }
  return response;
}

async function startApplication(actor) {
  window.vibecleaningActor = Object.freeze({ ...actor });
  loginView.hidden = true;
  appShell.hidden = false;
  if (!applicationLoaded) {
    applicationLoaded = true;
    await import("/static/app.js");
  }
}

window.vibecleaningAuth = Object.freeze({
  fetch: authenticatedFetch,
  async logout() {
    await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin" });
    window.location.reload();
  },
});

loginForm.addEventListener("submit", async event => {
  event.preventDefault();
  submitButton.disabled = true;
  statusText.textContent = "";
  try {
    const response = await fetch("/api/auth/login", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: usernameInput.value.trim(),
        password: passwordInput.value,
      }),
    });
    if (!response.ok) {
      statusText.textContent = response.status === 401
        ? "Incorrect username or password."
        : "The server could not complete this login.";
      return;
    }
    const payload = await response.json();
    passwordInput.value = "";
    await startApplication(payload.actor);
  } catch (error) {
    statusText.textContent = "Could not reach the server. Please try again.";
  } finally {
    submitButton.disabled = false;
  }
});

logoutButton.addEventListener("click", () => void window.vibecleaningAuth.logout());

try {
  const response = await fetch("/api/auth/me", { cache: "no-store", credentials: "same-origin" });
  if (response.ok) {
    const payload = await response.json();
    await startApplication(payload.actor);
  } else {
    showLogin();
  }
} catch (error) {
  showLogin("Could not reach the server. Please try again.");
}
