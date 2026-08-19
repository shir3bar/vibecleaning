from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
from pathlib import Path
import secrets
from threading import RLock
from time import time
from typing import Iterable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send


AUTH_DIR_NAME = ".vibecleaning"
USERS_FILE_NAME = "users.json"
SESSION_COOKIE = "vibecleaning_session"
SESSION_SECONDS = 12 * 60 * 60
SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1
VALID_ROLES = frozenset({"reviewer", "editor"})
PUBLIC_PATHS = frozenset({"/", "/api/auth/login"})
PUBLIC_PREFIXES = ("/static/", "/slim-static/")


class AuthenticationError(ValueError):
    pass


@dataclass(frozen=True)
class Actor:
    user_id: str
    username: str
    display_name: str
    role: str

    def as_dict(self) -> dict[str, str]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "display_name": self.display_name,
            "role": self.role,
        }


@dataclass
class _Session:
    actor: Actor
    expires_at: float


def users_path(data_root: Path) -> Path:
    return data_root.resolve() / AUTH_DIR_NAME / USERS_FILE_NAME


def normalize_username(value: object) -> str:
    if not isinstance(value, str):
        raise AuthenticationError("Username is required")
    username = value.strip().lower()
    if not username or len(username) > 80:
        raise AuthenticationError("Invalid username")
    if not all(character.isalnum() or character in {".", "_", "-"} for character in username):
        raise AuthenticationError("Username may contain letters, numbers, dots, underscores, and hyphens")
    return username


def normalize_display_name(value: object) -> str:
    if not isinstance(value, str):
        raise AuthenticationError("Display name is required")
    display_name = " ".join(value.strip().split())
    if not display_name or len(display_name) > 120:
        raise AuthenticationError("Invalid display name")
    return display_name


def normalize_role(value: object) -> str:
    role = str(value or "").strip().lower()
    if role not in VALID_ROLES:
        raise AuthenticationError("Role must be reviewer or editor")
    return role


def hash_password(password: str, *, salt: bytes | None = None) -> dict[str, object]:
    if not isinstance(password, str) or not password:
        raise AuthenticationError("Password is required")
    actual_salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=actual_salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=32,
    )
    return {
        "algorithm": "scrypt",
        "n": SCRYPT_N,
        "r": SCRYPT_R,
        "p": SCRYPT_P,
        "salt": base64.b64encode(actual_salt).decode("ascii"),
        "digest": base64.b64encode(digest).decode("ascii"),
    }


def verify_password(password: str, password_hash: object) -> bool:
    if not isinstance(password, str) or not isinstance(password_hash, dict):
        return False
    try:
        salt = base64.b64decode(str(password_hash["salt"]), validate=True)
        expected = base64.b64decode(str(password_hash["digest"]), validate=True)
        actual = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=int(password_hash.get("n", SCRYPT_N)),
            r=int(password_hash.get("r", SCRYPT_R)),
            p=int(password_hash.get("p", SCRYPT_P)),
            dklen=len(expected),
        )
    except (KeyError, TypeError, ValueError):
        return False
    return hmac.compare_digest(actual, expected)


def read_users_file(path: Path) -> list[dict]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AuthenticationError(
            f"No user registry exists at {path}; bootstrap an editor account first"
        ) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise AuthenticationError(f"Invalid user registry: {path}") from exc
    users = payload.get("users") if isinstance(payload, dict) else None
    if payload.get("schema_version") != 1 or not isinstance(users, list):
        raise AuthenticationError("Unsupported user registry")
    return users


def write_users_file(path: Path, users: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": 1, "users": list(users)}
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


def build_user_record(
    *,
    username: str,
    display_name: str,
    role: str,
    password: str,
    user_id: str | None = None,
) -> dict:
    return {
        "user_id": user_id or f"user_{secrets.token_hex(6)}",
        "username": normalize_username(username),
        "display_name": normalize_display_name(display_name),
        "role": normalize_role(role),
        "enabled": True,
        "auth_version": 1,
        "password_hash": hash_password(password),
    }


class AuthManager:
    def __init__(
        self,
        users: Iterable[dict],
        *,
        secure_cookie: bool = False,
    ):
        self.secure_cookie = bool(secure_cookie)
        self._lock = RLock()
        self._sessions: dict[str, _Session] = {}
        self._users_by_username: dict[str, dict] = {}
        self._users_by_id: dict[str, dict] = {}
        for raw_user in users:
            user = self._validate_user(raw_user)
            username = user["username"]
            user_id = user["user_id"]
            if username in self._users_by_username or user_id in self._users_by_id:
                raise AuthenticationError("User IDs and usernames must be unique")
            self._users_by_username[username] = user
            self._users_by_id[user_id] = user
        if not self._users_by_username:
            raise AuthenticationError("At least one user is required")

    @classmethod
    def from_data_root(cls, data_root: Path, *, secure_cookie: bool = False) -> "AuthManager":
        return cls(read_users_file(users_path(data_root)), secure_cookie=secure_cookie)

    @classmethod
    def for_testing(
        cls,
        *,
        username: str,
        password: str,
        role: str = "editor",
        display_name: str | None = None,
    ) -> "AuthManager":
        record = build_user_record(
            username=username,
            display_name=display_name or username,
            role=role,
            password=password,
        )
        return cls([record])

    @staticmethod
    def _validate_user(raw_user: object) -> dict:
        if not isinstance(raw_user, dict):
            raise AuthenticationError("Invalid user record")
        user_id = str(raw_user.get("user_id") or "").strip()
        if not user_id or len(user_id) > 100:
            raise AuthenticationError("Invalid user ID")
        password_hash = raw_user.get("password_hash")
        if not isinstance(password_hash, dict):
            raise AuthenticationError("Invalid password hash")
        return {
            "user_id": user_id,
            "username": normalize_username(raw_user.get("username")),
            "display_name": normalize_display_name(raw_user.get("display_name")),
            "role": normalize_role(raw_user.get("role")),
            "enabled": raw_user.get("enabled") is True,
            "auth_version": max(1, int(raw_user.get("auth_version") or 1)),
            "password_hash": dict(password_hash),
        }

    def authenticate(self, username: object, password: object) -> Actor | None:
        try:
            normalized = normalize_username(username)
        except AuthenticationError:
            return None
        user = self._users_by_username.get(normalized)
        if not user or not user["enabled"] or not verify_password(password, user["password_hash"]):
            return None
        return self.actor_for_user(user)

    def actor_for_user(self, user: dict) -> Actor:
        return Actor(
            user_id=user["user_id"],
            username=user["username"],
            display_name=user["display_name"],
            role=user["role"],
        )

    def actor_by_id(self, user_id: str) -> Actor | None:
        user = self._users_by_id.get(str(user_id or ""))
        if not user or not user["enabled"]:
            return None
        return self.actor_for_user(user)

    def list_reviewers(self) -> list[dict[str, str]]:
        return [
            self.actor_for_user(user).as_dict()
            for user in sorted(self._users_by_id.values(), key=lambda item: item["display_name"].lower())
            if user["enabled"] and user["role"] == "reviewer"
        ]

    def create_session(self, actor: Actor) -> tuple[str, float]:
        token = secrets.token_urlsafe(32)
        expires_at = time() + SESSION_SECONDS
        with self._lock:
            self._sessions[token] = _Session(actor=actor, expires_at=expires_at)
            self._prune_sessions_locked()
        return token, expires_at

    def resolve_session(self, token: str | None) -> Actor | None:
        if not token:
            return None
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return None
            if session.expires_at <= time():
                self._sessions.pop(token, None)
                return None
            return session.actor

    def destroy_session(self, token: str | None) -> None:
        if not token:
            return
        with self._lock:
            self._sessions.pop(token, None)

    def _prune_sessions_locked(self) -> None:
        now = time()
        expired = [token for token, session in self._sessions.items() if session.expires_at <= now]
        for token in expired:
            self._sessions.pop(token, None)


def request_actor(request: Request) -> Actor | None:
    actor = getattr(request.state, "actor", None)
    return actor if isinstance(actor, Actor) else None


def require_actor(request: Request) -> Actor:
    actor = request_actor(request)
    if actor is None:
        raise AuthenticationError("Authentication required")
    return actor


def actor_payload(actor: Actor | None) -> dict | None:
    return actor.as_dict() if actor is not None else None


def apply_actor(payload: dict, actor: Actor, *, review_id: str = "") -> dict:
    updated = dict(payload)
    updated["user"] = actor.display_name
    updated["actor"] = actor.as_dict()
    parameters = dict(updated.get("parameters") or {})
    parameters["user"] = actor.display_name
    parameters["actor"] = actor.as_dict()
    workflow = dict(parameters.get("workflow") or {})
    if review_id:
        workflow["review_id"] = review_id
        parameters["review_id"] = review_id
    if workflow:
        parameters["workflow"] = workflow
    updated["parameters"] = parameters
    return updated


class SessionAuthMiddleware:
    def __init__(self, app: ASGIApp, *, manager: AuthManager):
        self.app = app
        self.manager = manager

    @staticmethod
    def _public_path(path: str) -> bool:
        return path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES)

    @staticmethod
    def _origin_allowed(scope: Scope) -> bool:
        method = str(scope.get("method") or "GET").upper()
        if method in {"GET", "HEAD", "OPTIONS"}:
            return True
        headers = Headers(scope=scope)
        origin = headers.get("origin")
        if not origin:
            return True
        scheme = scope.get("scheme", "http")
        host = headers.get("host", "")
        return hmac.compare_digest(origin.rstrip("/"), f"{scheme}://{host}")

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path") or "")
        if self._public_path(path):
            if not self._origin_allowed(scope):
                response = JSONResponse(
                    {"error": "Cross-origin mutation is not allowed"},
                    status_code=403,
                    headers={"Cache-Control": "no-store"},
                )
                await response(scope, receive, send)
                return
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        cookie_header = headers.get("cookie", "")
        token = ""
        for chunk in cookie_header.split(";"):
            name, separator, value = chunk.strip().partition("=")
            if separator and name == SESSION_COOKIE:
                token = value
                break
        actor = self.manager.resolve_session(token)
        if actor is None:
            response = JSONResponse(
                {"error": "Authentication required"},
                status_code=401,
                headers={"Cache-Control": "no-store"},
            )
            await response(scope, receive, send)
            return
        if not self._origin_allowed(scope):
            response = JSONResponse(
                {"error": "Cross-origin mutation is not allowed"},
                status_code=403,
                headers={"Cache-Control": "no-store"},
            )
            await response(scope, receive, send)
            return
        scope.setdefault("state", {})["actor"] = actor
        scope["state"]["session_token"] = token
        await self.app(scope, receive, send)


def add_authentication(app: FastAPI, manager: AuthManager) -> None:
    app.state.auth_manager = manager

    @app.post("/api/auth/login")
    async def login(request: Request):
        try:
            body = await request.json()
        except Exception:
            body = None
        if not isinstance(body, dict):
            return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
        actor = manager.authenticate(body.get("username"), body.get("password"))
        if actor is None:
            return JSONResponse(
                {"error": "Incorrect username or password"},
                status_code=401,
                headers={"Cache-Control": "no-store"},
            )
        token, _expires_at = manager.create_session(actor)
        response = JSONResponse(
            {"authenticated": True, "actor": actor.as_dict()},
            headers={"Cache-Control": "no-store"},
        )
        response.set_cookie(
            SESSION_COOKIE,
            token,
            max_age=SESSION_SECONDS,
            httponly=True,
            samesite="strict",
            secure=manager.secure_cookie,
            path="/",
        )
        return response

    @app.get("/api/auth/me")
    async def me(request: Request):
        actor = require_actor(request)
        return JSONResponse(
            {"authenticated": True, "actor": actor.as_dict()},
            headers={"Cache-Control": "no-store"},
        )

    @app.post("/api/auth/logout")
    async def logout(request: Request):
        manager.destroy_session(getattr(request.state, "session_token", ""))
        response = JSONResponse({"authenticated": False}, headers={"Cache-Control": "no-store"})
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    app.add_middleware(SessionAuthMiddleware, manager=manager)
