from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.web import create_app
from examples.movement.routes import register_movement_routes

from . import is_slim_movement_artifact
from .auth import add_login_auth


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
        overview_fix_limit=0,
        overview_series_points=250,
        background_anomaly_ranking=True,
    )
    app.mount(
        "/slim-static",
        StaticFiles(directory=index_path.parent),
        name="slim-static",
    )

    @app.get("/api/auth/check")
    def check_authentication(request: Request):
        return JSONResponse(
            {
                "authenticated": True,
                "username": request.state.authenticated_username,
            },
            headers={"Cache-Control": "no-store"},
        )

    add_login_auth(
        app,
        username=username,
        password=password,
    )
    return app
