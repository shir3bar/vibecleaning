"""Temporary deployment-wide authentication for the slim movement app."""

import base64
import binascii
import secrets
from collections.abc import Mapping

from fastapi import FastAPI
from starlette.datastructures import Headers
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send


USERNAME_ENV = "SLIM_MOVEMENT_USERNAME"
PASSWORD_ENV = "SLIM_MOVEMENT_PASSWORD"
MINIMUM_PASSWORD_LENGTH = 12
AUTH_REALM = "slim_movement"


def credentials_from_environment(environ: Mapping[str, str]) -> tuple[str, str]:
    username = environ.get(USERNAME_ENV, "").strip()
    password = environ.get(PASSWORD_ENV, "")
    if not username or not password:
        raise RuntimeError(
            f"{USERNAME_ENV} and {PASSWORD_ENV} must be set before starting slim_movement"
        )
    if ":" in username or any(character.isspace() for character in username):
        raise RuntimeError(f"{USERNAME_ENV} cannot contain colons or whitespace")
    if len(password) < MINIMUM_PASSWORD_LENGTH:
        raise RuntimeError(
            f"{PASSWORD_ENV} must contain at least {MINIMUM_PASSWORD_LENGTH} characters"
        )
    return username, password


class BasicAuthMiddleware:
    def __init__(self, app: ASGIApp, *, username: str, password: str):
        self.app = app
        self.expected_username = username.encode("utf-8")
        self.expected_password = password.encode("utf-8")

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
        if scope["type"] != "http" or self._authorized(scope):
            await self.app(scope, receive, send)
            return
        response = PlainTextResponse(
            "Authentication required",
            status_code=401,
            headers={
                "WWW-Authenticate": f'Basic realm="{AUTH_REALM}", charset="UTF-8"',
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Referrer-Policy": "no-referrer",
            },
        )
        await response(scope, receive, send)


def add_basic_auth(app: FastAPI, *, username: str, password: str):
    app.add_middleware(
        BasicAuthMiddleware,
        username=username,
        password=password,
    )
