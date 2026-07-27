from pathlib import Path

from fastapi import FastAPI

from app.web import create_app
from examples.movement.routes import register_movement_routes

from . import is_slim_movement_artifact
from .auth import add_basic_auth


def create_slim_movement_app(
    *,
    data_root: Path,
    static_root: Path,
    index_path: Path,
    username: str,
    password: str,
) -> FastAPI:
    app = create_app(
        data_root=data_root,
        static_root=static_root,
        index_path=index_path,
    )
    register_movement_routes(
        app,
        data_root=data_root,
        allowed_families={"movement_raw"},
        artifact_filter=is_slim_movement_artifact,
        include_dev_routes=False,
    )
    add_basic_auth(
        app,
        username=username,
        password=password,
    )
    return app
