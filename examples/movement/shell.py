from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse


MOVEMENT_SHELL_MODES = {"slim_movement", "rds_movement"}


def install_movement_shell_mode(
    app: FastAPI,
    *,
    index_path: Path,
    mode: str,
) -> None:
    normalized_mode = str(mode or "").strip()
    if normalized_mode not in MOVEMENT_SHELL_MODES:
        raise ValueError(f"Unsupported movement shell mode: {mode}")
    resolved_index_path = index_path.resolve()

    @app.middleware("http")
    async def inject_movement_mode(request, call_next):
        if request.url.path == "/":
            html = resolved_index_path.read_text(encoding="utf-8")
            marker = (
                '<meta name="vibecleaning-movement-mode" '
                f'content="{normalized_mode}">'
            )
            html = html.replace("<title>", marker + "\n<title>", 1)
            return HTMLResponse(html)
        return await call_next(request)
