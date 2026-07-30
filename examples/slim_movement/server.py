import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from examples.slim_movement.app import create_slim_movement_app  # noqa: E402
from examples.slim_movement.auth import (  # noqa: E402
    PASSWORD_ENV,
    startup_credentials,
)


PORT = int(os.environ.get("PORT", "8421"))
HOST = os.environ.get("HOST", "127.0.0.1")
AUTH = startup_credentials(os.environ)


app = create_slim_movement_app(
    data_root=ROOT / "data",
    static_root=ROOT / "examples" / "movement" / "static",
    index_path=ROOT / "examples" / "slim_movement" / "static" / "index.html",
    username=AUTH.username,
    password=AUTH.password,
)


if __name__ == "__main__":
    import uvicorn

    print(f"\nVibecleaning Slim Movement: http://{HOST}:{PORT}")
    print("\nSlim Movement temporary login")
    print(f"Username: {AUTH.username}")
    if AUTH.generated_password:
        print(f"Password: {AUTH.password}")
    else:
        print(f"Password: configured through {PASSWORD_ENV} (not printed)")
    print()
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
