import os
from pathlib import Path

from app.web import create_app


APP_ROOT = Path(__file__).parent
PORT = int(os.environ.get("PORT", "8420"))
HOST = os.environ.get("HOST", "127.0.0.1")

app = create_app(
    data_root=APP_ROOT / "data",
    static_root=APP_ROOT / "static",
    plugins_root=APP_ROOT / "plugins",
)
if __name__ == "__main__":
    import uvicorn

    print(f"\n  Vibecleaning: http://{HOST}:{PORT}\n")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
