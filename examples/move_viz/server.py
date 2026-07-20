import os
import sys
from pathlib import Path

from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.web import create_app  # noqa: E402
from examples.move_viz.routes import register_move_viz_routes  # noqa: E402


PORT = int(os.environ.get("PORT", "8422"))
HOST = os.environ.get("HOST", "127.0.0.1")
STATIC_ROOT = ROOT / "examples" / "move_viz" / "static"
MOVEMENT_STATIC_ROOT = ROOT / "examples" / "movement" / "static"

app = create_app(data_root=ROOT / "data", static_root=STATIC_ROOT)
app.mount(
    "/movement-assets",
    StaticFiles(directory=MOVEMENT_STATIC_ROOT),
    name="move-viz-movement-assets",
)
register_move_viz_routes(app)


if __name__ == "__main__":
    import uvicorn

    print(f"\n  Vibecleaning Move Viz: http://{HOST}:{PORT}\n")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
