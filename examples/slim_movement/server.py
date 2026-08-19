import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.slim_movement.app import create_slim_movement_app  # noqa: E402
from app.auth import AuthManager  # noqa: E402


PORT = int(os.environ.get("PORT", "8421"))
HOST = os.environ.get("HOST", "127.0.0.1")
SECURE_COOKIE = os.environ.get("VIBECLEANING_SECURE_COOKIE", "").strip().lower() in {
    "1", "true", "yes", "on"
}
app = create_slim_movement_app(
    data_root=ROOT / "data",
    static_root=ROOT / "examples" / "movement" / "static",
    index_path=ROOT / "examples" / "slim_movement" / "static" / "index.html",
    auth_manager=AuthManager.from_data_root(ROOT / "data", secure_cookie=SECURE_COOKIE),
)


if __name__ == "__main__":
    import uvicorn

    print(f"\nVibecleaning Slim Movement: http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
