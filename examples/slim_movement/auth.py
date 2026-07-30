"""Temporary, cookie-free authentication for the slim movement app."""

import base64
import binascii
from dataclasses import dataclass
import secrets
from collections.abc import Mapping

from fastapi import FastAPI
from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


USERNAME_ENV = "SLIM_MOVEMENT_USERNAME"
PASSWORD_ENV = "SLIM_MOVEMENT_PASSWORD"
DEFAULT_USERNAME = "reviewer"
MINIMUM_PASSWORD_LENGTH = 12
PUBLIC_PATHS = frozenset({"/"})
PUBLIC_PREFIXES = ("/static/", "/slim-static/")


@dataclass(frozen=True)
class StartupCredentials:
    username: str
    password: str
    generated_password: bool


def startup_credentials(environ: Mapping[str, str]) -> StartupCredentials:
    username = environ.get(USERNAME_ENV, DEFAULT_USERNAME).strip()
    configured_password = environ.get(PASSWORD_ENV)
    password = configured_password if configured_password is not None else secrets.token_urlsafe(24)
    if not username:
        raise RuntimeError(f"{USERNAME_ENV} cannot be empty")
    if ":" in username or any(character.isspace() for character in username):
        raise RuntimeError(f"{USERNAME_ENV} cannot contain colons or whitespace")
    if len(password) < MINIMUM_PASSWORD_LENGTH:
        raise RuntimeError(
            f"{PASSWORD_ENV} must contain at least {MINIMUM_PASSWORD_LENGTH} characters"
        )
    return StartupCredentials(
        username=username,
        password=password,
        generated_password=configured_password is None,
    )


class LoginAuthMiddleware:
    def __init__(self, app: ASGIApp, *, username: str, password: str):
        self.app = app
        self.expected_username = username.encode("utf-8")
        self.expected_password = password.encode("utf-8")

    @staticmethod
    def _is_public(scope: Scope) -> bool:
        path = scope.get("path", "")
        return path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES)

    def _authorized(self, scope: Scope) -> bool:
        authorization = Headers(scope=scope).get("authorization", "")
        scheme, separator, encoded = authorization.partition(" ")
        if separator != " " or scheme.lower() != "basic" or not encoded:
            return False
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            return False
        supplied_username, separator, supplied_password = decoded.partition(b":")
        if separator != b":":
            return False
        username_matches = secrets.compare_digest(
            supplied_username,
            self.expected_username,
        )
        password_matches = secrets.compare_digest(
            supplied_password,
            self.expected_password,
        )
        return username_matches and password_matches

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or self._is_public(scope):
            await self.app(scope, receive, send)
            return
        if self._authorized(scope):
            scope.setdefault("state", {})["authenticated_username"] = (
                self.expected_username.decode("utf-8")
            )
            await self.app(scope, receive, send)
            return
        response = JSONResponse(
            {"error": "Authentication required"},
            status_code=401,
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Referrer-Policy": "no-referrer",
            },
        )
        await response(scope, receive, send)


def add_login_auth(app: FastAPI, *, username: str, password: str):
    app.add_middleware(
        LoginAuthMiddleware,
        username=username,
        password=password,
    )
