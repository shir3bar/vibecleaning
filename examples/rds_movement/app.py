from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.auth import AuthManager
from app.web import create_app
from examples.movement.routes import register_movement_routes
from examples.movement.rds_index import is_rds_artifact


def create_rds_movement_app(
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
        allowed_families={"movement_rds"},
        artifact_filter=is_rds_artifact,
        include_dev_routes=False,
        overview_fix_limit=0,
        overview_series_points=250,
        background_anomaly_ranking=True,
        source_format="rds",
    )

    @app.middleware("http")
    async def inject_rds_movement_mode(request, call_next):
        if request.url.path == "/":
            html = index_path.read_text(encoding="utf-8")
            marker = '<meta name="vibecleaning-movement-mode" content="rds_movement">'
            html = html.replace("<title>", marker + "\n<title>", 1)
            return HTMLResponse(html)
        return await call_next(request)

    return app
