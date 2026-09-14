import os
from pathlib import Path

from fastapi.responses import PlainTextResponse

from app.runtime import (
    COOPERATIVE_MODE_WARNING,
    resolve_data_root,
    shared_locking_mode,
    validate_data_root,
)
from app.web import create_app


APP_ROOT = Path(__file__).parent
PORT = int(os.environ.get("PORT", "8420"))
HOST = os.environ.get("HOST", "127.0.0.1")
DATA_ROOT = validate_data_root(resolve_data_root(default=APP_ROOT / "data"))
SHARED_LOCKING = shared_locking_mode()

app = create_app(
    data_root=DATA_ROOT,
    static_root=APP_ROOT / "static",
    shared_locking=SHARED_LOCKING,
)


@app.get("/starter-readme")
async def starter_readme():
    return PlainTextResponse((APP_ROOT / "README.md").read_text())


if __name__ == "__main__":
    import uvicorn

    if SHARED_LOCKING == "disabled":
        print(f"\n  WARNING: {COOPERATIVE_MODE_WARNING}")
    print(f"\n  Vibecleaning Starter App: http://{HOST}:{PORT}\n")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
