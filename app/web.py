from pathlib import Path
import re

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .execution import create_analysis, create_step, set_current_head, undo_to_parent
from .preview import preview_artifact
from .state import (
    ProjectStateError,
    get_dataset_artifact,
    graph_payload,
    list_projects,
    load_dataset,
    load_json,
    media_type_for_path,
    project_paths,
    project_state_payload,
    resolve_project_dir,
)


SAFE_PATH_PART = re.compile(r"^[A-Za-z0-9._-]+$")


def json_error(message: str, status_code: int) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status_code)


def validate_path_part(raw_value: object, *, label: str) -> str:
    if not isinstance(raw_value, str):
        raise ValueError(f"Invalid {label}")
    value = raw_value.strip()
    if not value or not SAFE_PATH_PART.fullmatch(value):
        raise ValueError(f"Invalid {label}")
    return value


def get_project_dir(data_root: Path, project_name: str) -> Path:
    project = validate_path_part(project_name, label="project")
    path = resolve_project_dir(data_root, project).resolve()
    if data_root.resolve() not in path.parents:
        raise ValueError("Invalid project")
    if not path.exists() or not path.is_dir():
        raise ValueError("Unknown project")
    return path


async def parse_json_body(request: Request) -> dict | None:
    try:
        body = await request.json()
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    return body


def create_app(*, data_root: Path, static_root: Path) -> FastAPI:
    data_root = data_root.resolve()
    static_root = static_root.resolve()

    app = FastAPI()
    app.state.data_root = data_root
    app.state.static_root = static_root

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )
    app.add_middleware(
        GZipMiddleware,
        minimum_size=1024,
    )

    @app.middleware("http")
    async def add_security_headers(request: Request, call_next):
        response = await call_next(request)
        if request.url.path == "/" or request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "connect-src 'self' "
            "https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com "
            "https://tile.openstreetmap.org "
            "https://services.arcgisonline.com "
            "https://tile.opentopomap.org; "
            "img-src 'self' data: blob: "
            "https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com "
            "https://tile.openstreetmap.org "
            "https://services.arcgisonline.com "
            "https://tile.opentopomap.org; "
            "style-src 'self' 'unsafe-inline'; "
            "script-src 'self'; "
            "worker-src 'self' blob:; "
            "base-uri 'none'; "
            "frame-ancestors 'none'; "
            "form-action 'self'"
        )
        return response

    if static_root.exists():
        app.mount("/static", StaticFiles(directory=static_root), name="static")

    @app.get("/")
    async def index():
        return FileResponse(static_root / "index.html")

    @app.get("/api/projects")
    async def get_projects():
        return JSONResponse({"projects": list_projects(data_root)})

    @app.get("/api/project/{project_name}/state")
    async def get_project_state(project_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse(project_state_payload(project_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/project/{project_name}/graph")
    async def get_project_graph(project_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse(graph_payload(project_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/project/{project_name}/dataset/{dataset_id}")
    async def get_project_dataset(project_name: str, dataset_id: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse(load_dataset(project_dir, dataset_id))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/project/{project_name}/artifact/{dataset_id}/{logical_name}")
    async def get_project_artifact(project_name: str, dataset_id: str, logical_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            _, artifact_path = get_dataset_artifact(project_dir, dataset_id, logical_name)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)
        return FileResponse(artifact_path, media_type=media_type_for_path(artifact_path))

    @app.get("/api/project/{project_name}/artifact/{dataset_id}/{logical_name}/meta")
    async def get_project_artifact_meta(project_name: str, dataset_id: str, logical_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            artifact, artifact_path = get_dataset_artifact(project_dir, dataset_id, logical_name)
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)
        payload = dict(artifact)
        payload["resolved_path"] = str(artifact_path)
        return JSONResponse(payload)

    @app.get("/api/project/{project_name}/artifact/{dataset_id}/{logical_name}/preview")
    async def get_project_artifact_preview(project_name: str, dataset_id: str, logical_name: str, limit_bytes: int = 65536):
        try:
            project_dir = get_project_dir(data_root, project_name)
            _, artifact_path = get_dataset_artifact(project_dir, dataset_id, logical_name)
            return JSONResponse(preview_artifact(artifact_path, limit_bytes=limit_bytes))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/project/{project_name}/analysis/{analysis_id}")
    async def get_project_analysis(project_name: str, analysis_id: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            analysis_dir = project_paths(project_dir)["analyses"] / validate_path_part(analysis_id, label="analysis")
            return JSONResponse(load_json(analysis_dir / "analysis.json"))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)

    @app.get("/api/project/{project_name}/analysis/{analysis_id}/artifact/{logical_name}")
    async def get_project_analysis_artifact(project_name: str, analysis_id: str, logical_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            analysis_dir = project_paths(project_dir)["analyses"] / validate_path_part(analysis_id, label="analysis")
            logical_part = validate_path_part(logical_name, label="artifact")
            artifact_path = (analysis_dir / "outputs" / logical_part).resolve()
            if project_dir.resolve() not in artifact_path.parents:
                raise ProjectStateError("Invalid artifact path")
            if not artifact_path.exists() or not artifact_path.is_file():
                raise ProjectStateError("Unknown artifact")
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 404)
        return FileResponse(artifact_path, media_type=media_type_for_path(artifact_path))

    @app.post("/api/project/{project_name}/analyses")
    async def post_project_analysis(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse(create_analysis(project_dir, body))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/project/{project_name}/steps")
    async def post_project_step(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse(create_step(project_dir, body))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/project/{project_name}/head")
    async def post_project_head(project_name: str, request: Request):
        body = await parse_json_body(request)
        if body is None:
            return json_error("Invalid JSON body", 400)
        try:
            dataset_id = validate_path_part(body.get("dataset_id"), label="dataset")
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse({"dataset": set_current_head(project_dir, dataset_id)})
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    @app.post("/api/project/{project_name}/undo")
    async def post_project_undo(project_name: str):
        try:
            project_dir = get_project_dir(data_root, project_name)
            return JSONResponse(undo_to_parent(project_dir))
        except (ValueError, ProjectStateError) as exc:
            return json_error(str(exc), 400)

    return app
