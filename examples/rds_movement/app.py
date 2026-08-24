from pathlib import Path

from fastapi import FastAPI
from app.auth import AuthManager
from app.web import create_app
from examples.movement.routes import register_movement_routes
from examples.movement.rds_index import is_rds_artifact
from examples.movement.shell import install_movement_shell_mode


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

    install_movement_shell_mode(app, index_path=index_path, mode="rds_movement")

    return app
