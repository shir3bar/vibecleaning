import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.slim_movement.app import create_slim_movement_app  # noqa: E402
from app.auth import AuthManager  # noqa: E402
from app.runtime import (  # noqa: E402
    COOPERATIVE_MODE_WARNING,
    resolve_cache_root,
    resolve_data_root,
    shared_locking_mode,
    validate_cache_root,
    validate_data_root,
)


PORT = int(os.environ.get("PORT", "8421"))
HOST = os.environ.get("HOST", "127.0.0.1")
SECURE_COOKIE = os.environ.get("VIBECLEANING_SECURE_COOKIE", "").strip().lower() in {
    "1", "true", "yes", "on"
}
DATA_ROOT = validate_data_root(resolve_data_root(default=ROOT / "data"))
CACHE_ROOT = validate_cache_root(resolve_cache_root())
SHARED_LOCKING = shared_locking_mode()
app = create_slim_movement_app(
    data_root=DATA_ROOT,
    cache_root=CACHE_ROOT,
    static_root=ROOT / "examples" / "movement" / "static",
    index_path=ROOT / "examples" / "movement" / "static" / "index.html",
    auth_manager=AuthManager.from_data_root(DATA_ROOT, secure_cookie=SECURE_COOKIE),
    shared_locking=SHARED_LOCKING,
)


if __name__ == "__main__":
    import uvicorn

    if SHARED_LOCKING == "disabled":
        print(f"\nWARNING: {COOPERATIVE_MODE_WARNING}")
    print(f"\nVibecleaning Slim Movement: http://{HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
