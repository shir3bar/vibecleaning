const loginView = document.querySelector("#login-view");
const loginForm = document.querySelector("#login-form");
const usernameInput = document.querySelector("#login-username");
const passwordInput = document.querySelector("#login-password");
const submitButton = document.querySelector("#login-submit");
const statusText = document.querySelector("#login-status");
const appShell = document.querySelector("#app-shell");
const logoutButton = document.querySelector("#logout-button");

let authorizationHeader = "";
let applicationLoaded = false;

function encodeCredentials(username, password) {
  const bytes = new TextEncoder().encode(`${username}:${password}`);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return `Basic ${btoa(binary)}`;
}

function clearAuthentication() {
  authorizationHeader = "";
  passwordInput.value = "";
}

function returnToLogin() {
  clearAuthentication();
  window.location.reload();
}

async function authenticatedFetch(input, init = {}) {
  if (!authorizationHeader) {
    returnToLogin();
    throw new Error("Authentication required");
  }
  const headers = new Headers(init.headers || {});
  headers.set("Authorization", authorizationHeader);
  const response = await fetch(input, {
    ...init,
    headers,
    cache: init.cache || "no-store",
  });
  if (response.status === 401) {
    returnToLogin();
    throw new Error("Authentication required");
  }
  return response;
}

window.vibecleaningSlimAuth = Object.freeze({
  fetch: authenticatedFetch,
  logout: returnToLogin,
});

loginForm.addEventListener("submit", async event => {
  event.preventDefault();
  submitButton.disabled = true;
  statusText.textContent = "";

  const candidateHeader = encodeCredentials(
    usernameInput.value.trim(),
    passwordInput.value,
  );

  try {
    const response = await fetch("/api/auth/check", {
      headers: { Authorization: candidateHeader },
      cache: "no-store",
    });
    if (!response.ok) {
      statusText.textContent = response.status === 401
        ? "Incorrect username or password."
        : "The server could not validate this login.";
      return;
    }
    authorizationHeader = candidateHeader;
    passwordInput.value = "";
    loginView.hidden = true;
    appShell.hidden = false;
    if (!applicationLoaded) {
      applicationLoaded = true;
      await import("/static/app.js");
    }
  } catch (error) {
    statusText.textContent = "Could not reach the server. Please try again.";
  } finally {
    submitButton.disabled = false;
  }
});

logoutButton.addEventListener("click", returnToLogin);
passwordInput.focus();
