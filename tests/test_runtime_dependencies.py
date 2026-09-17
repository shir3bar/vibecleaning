"""Keep dependencies needed during startup in the production install."""

from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_httpx_is_a_locked_runtime_dependency():
    # app.web imports app.osm (and therefore httpx) for both launcher profiles.
    # A developer environment masks this failure when httpx is dev-only.
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    package = next(p for p in lock["package"] if p["name"] == "vibecleaning")

    assert "httpx" in project["project"]["dependencies"]
    assert "httpx" not in project["dependency-groups"]["dev"]
    assert "httpx" in {dep["name"] for dep in package["dependencies"]}
    assert "httpx" in {dep["name"] for dep in package["metadata"]["requires-dist"]}
