import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.web import create_app  # noqa: E402
from examples.movement.routes import register_movement_routes  # noqa: E402


PORT = int(os.environ.get("PORT", "8420"))
HOST = os.environ.get("HOST", "127.0.0.1")

app = create_app(
    data_root=ROOT / "data",
    static_root=ROOT / "examples" / "movement" / "static",
)
register_movement_routes(app, data_root=ROOT / "data")


if __name__ == "__main__":
    import uvicorn

    print(f"\n  Vibecleaning Movement Example: http://{HOST}:{PORT}\n")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
