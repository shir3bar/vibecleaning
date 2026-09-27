"""Run the real shared frontend's selection handlers without WebGL or study data."""
from pathlib import Path
import shutil
import subprocess
import os

import pytest


@pytest.mark.parametrize("profile", ["slim_movement", "rds_movement"])
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_threshold_survives_individual_selection(profile, newline):
    node = shutil.which("node")
    if not node:
        playwright = pytest.importorskip("playwright")
        bundled = Path(playwright.__file__).parent / "driver" / ("node.exe" if os.name == "nt" else "node")
        if not bundled.exists():
            pytest.skip("A Node.js runtime is required")
        node = str(bundled)
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/frontend/threshold_selection.cjs"),
         "--stdin", profile],
        input=(root / "examples/movement/static/app.js").read_text(encoding="utf-8").replace("\n", newline).encode("utf-8"),
        capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
