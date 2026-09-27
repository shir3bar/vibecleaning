#!/usr/bin/env bash
# macOS/Linux counterpart to start-vibecleaning.ps1.
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
profile=Rds
data_root="${VIBECLEANING_DATA_ROOT:-}"
cache_root="${VIBECLEANING_CACHE_ROOT:-}"
port=""
locking="${VIBECLEANING_SHARED_LOCKING:-required}"
while [ "$#" -gt 0 ]; do
    case "$1" in
        --profile|--data-root|--cache-root|--port|--shared-locking)
            [ "$#" -ge 2 ] || { echo "Missing value for $1" >&2; exit 2; }
            case "$1" in
                --profile) profile="$2" ;;
                --data-root) data_root="$2" ;;
                --cache-root) cache_root="$2" ;;
                --port) port="$2" ;;
                --shared-locking) locking="$2" ;;
            esac
            shift 2 ;;
        --help|-h)
            echo "Usage: bash scripts/start-vibecleaning.sh --profile Rds|Csv --data-root PATH [--cache-root PATH] [--port NUMBER] [--shared-locking required|disabled]"
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done
case "$profile" in
    Rds) entry=examples/rds_movement/server.py; port="${port:-8422}" ;;
    Csv) entry=examples/slim_movement/server.py; port="${port:-8421}" ;;
    *) echo "Profile must be Rds or Csv" >&2; exit 2 ;;
esac
case "$locking" in required|disabled) ;; *) echo "Shared locking must be required or disabled" >&2; exit 2 ;; esac
if [ -z "$data_root" ] || [ ! -d "$data_root" ]; then
    echo "Specify --data-root pointing to an existing data directory or mounted share." >&2
    exit 2
fi
case "$(uname -s)" in
    Darwin) cache_root="${cache_root:-$HOME/Library/Caches/Vibecleaning}" ;;
    Linux) cache_root="${cache_root:-${XDG_CACHE_HOME:-$HOME/.cache}/vibecleaning}" ;;
    *) echo "This launcher supports macOS and Linux." >&2; exit 2 ;;
esac
command -v uv >/dev/null || { echo "Install uv and reopen the terminal before starting." >&2; exit 2; }
mkdir -p -- "$cache_root"
export VIBECLEANING_DATA_ROOT="$(cd -- "$data_root" && pwd)"
export VIBECLEANING_CACHE_ROOT="$(cd -- "$cache_root" && pwd)"
export VIBECLEANING_SHARED_LOCKING="$locking"
export HOST=127.0.0.1 PORT="$port"
export PYTHONUTF8=1
# Keep each release's environment in its local checkout, never on the share.
export UV_PROJECT_ENVIRONMENT="$repository_root/.venv"
cd -- "$repository_root"
uv sync --locked --no-dev
uv run --no-sync python -c 'import os,socket; p=int(os.environ["PORT"]); assert 1 <= p <= 65535, "Port must be 1..65535"; s=socket.socket(); s.bind(("127.0.0.1",p)); s.close()' || {
    echo "Cannot use 127.0.0.1:$port. Check the port or stop the existing instance." >&2; exit 2;
}
echo "Starting $profile at http://127.0.0.1:$port"
echo "Authoritative data: $VIBECLEANING_DATA_ROOT"
echo "Disposable cache: $VIBECLEANING_CACHE_ROOT"
exec uv run --no-sync python "$entry"
