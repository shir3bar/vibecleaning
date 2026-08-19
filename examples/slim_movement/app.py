from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.auth import AuthManager
from app.web import create_app
from examples.movement.routes import register_movement_routes

from . import is_slim_movement_artifact
def create_slim_movement_app(
    *,
    data_root: Path,
    static_root: Path,
    index_path: Path,
    auth_manager: AuthManager | None = None,
) -> FastAPI:
    if auth_manager is None:
        auth_manager = AuthManager.from_data_root(data_root)
    app = create_app(
        data_root=data_root,
        static_root=static_root,
        index_path=index_path,
        auth_manager=auth_manager,
    )
    register_movement_routes(
        app,
        data_root=data_root,
        allowed_families={"movement_raw"},
        artifact_filter=is_slim_movement_artifact,
        include_dev_routes=False,
        overview_fix_limit=0,
        overview_series_points=250,
        background_anomaly_ranking=True,
    )
    app.mount(
        "/slim-static",
        StaticFiles(directory=index_path.parent),
        name="slim-static",
    )

    return app
