"""Run the real shared frontend's selection handlers without WebGL or study data."""
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize("profile", ["slim_movement", "rds_movement"])
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_threshold_survives_individual_selection(profile, newline, node_runtime):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node_runtime, str(root / "tests/frontend/threshold_selection.cjs"),
         "--stdin", profile],
        input=(root / "examples/movement/static/app.js").read_text(encoding="utf-8").replace("\n", newline).encode("utf-8"),
        capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
