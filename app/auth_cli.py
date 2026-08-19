from __future__ import annotations

import argparse
from getpass import getpass
from pathlib import Path
import sys

from .auth import (
    AuthenticationError,
    build_user_record,
    normalize_display_name,
    normalize_role,
    normalize_username,
    read_users_file,
    users_path,
    write_users_file,
)


def _read_or_empty(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return read_users_file(path)


def _password() -> str:
    first = getpass("Password: ")
    second = getpass("Confirm password: ")
    if first != second:
        raise AuthenticationError("Passwords do not match")
    return first


def _find_user(users: list[dict], username: str) -> dict:
    normalized = normalize_username(username)
    for user in users:
        if user.get("username") == normalized:
            return user
    raise AuthenticationError("Unknown user")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage Vibecleaning local users")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser("bootstrap", help="Create the first editor registry")
    bootstrap.add_argument("username")
    bootstrap.add_argument("--display-name", required=True)

    add = subparsers.add_parser("add", help="Add a reviewer or editor")
    add.add_argument("username")
    add.add_argument("--display-name", required=True)
    add.add_argument("--role", choices=("reviewer", "editor"), required=True)

    reset = subparsers.add_parser("reset-password", help="Reset a password")
    reset.add_argument("username")

    for command in ("enable", "disable"):
        action = subparsers.add_parser(command, help=f"{command.title()} an account")
        action.add_argument("username")

    subparsers.add_parser("list", help="List accounts")
    args = parser.parse_args(argv)
    path = users_path(args.data_root)

    try:
        users = _read_or_empty(path)
        if args.command in {"bootstrap", "add"}:
            if args.command == "bootstrap" and users:
                raise AuthenticationError("The user registry is already initialized")
            username = normalize_username(args.username)
            if any(user.get("username") == username for user in users):
                raise AuthenticationError("Username already exists")
            users.append(
                build_user_record(
                    username=username,
                    display_name=normalize_display_name(args.display_name),
                    role="editor" if args.command == "bootstrap" else normalize_role(args.role),
                    password=_password(),
                )
            )
            write_users_file(path, users)
        elif args.command == "reset-password":
            from .auth import hash_password

            user = _find_user(users, args.username)
            user["password_hash"] = hash_password(_password())
            user["auth_version"] = int(user.get("auth_version") or 1) + 1
            write_users_file(path, users)
        elif args.command in {"enable", "disable"}:
            user = _find_user(users, args.username)
            user["enabled"] = args.command == "enable"
            user["auth_version"] = int(user.get("auth_version") or 1) + 1
            write_users_file(path, users)
        else:
            for user in sorted(users, key=lambda item: str(item.get("username") or "")):
                print(
                    f"{user.get('username')}\t{user.get('display_name')}\t"
                    f"{user.get('role')}\t{'enabled' if user.get('enabled') else 'disabled'}"
                )
    except AuthenticationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
