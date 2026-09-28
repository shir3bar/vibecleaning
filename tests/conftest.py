from pathlib import Path
import os
import shutil

import pytest


@pytest.fixture(scope="session")
def node_runtime():
    """Use Node if installed, otherwise the runtime in locked Playwright."""
    node = shutil.which("node")
    if node:
        return node
    import playwright

    bundled = Path(playwright.__file__).parent / "driver" / ("node.exe" if os.name == "nt" else "node")
    assert bundled.is_file(), "Playwright's Node runtime is missing; run uv sync --locked"
    return str(bundled)
