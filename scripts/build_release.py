"""Build a checksummed release and synthetic data bundle; never include live state."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = {"README.md", "AGENTS.md", "pyproject.toml", "uv.lock", ".python-version", "server.py"}
TREES = {"app", "examples", "static", "scripts", "docs", "tests"}


def digest(content):
    return hashlib.sha256(content).hexdigest()


def archive(path, files, manifest):
    with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED) as bundle:
        entries = {**files, "release-manifest.json": (json.dumps(manifest, indent=2) + "\n").encode("utf-8")}
        for name, content in sorted(entries.items()):
            info = zipfile.ZipInfo(name, date_time=(2026, 9, 26, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            bundle.writestr(info, content)


def build(output):
    files = {}
    candidates = [ROOT / name for name in ROOT_FILES]
    for name in sorted(TREES):
        candidates.extend((ROOT / name).rglob("*"))
    for path in sorted(candidates):
        relative = path.relative_to(ROOT)
        if relative.parts[0] not in TREES and str(relative) not in ROOT_FILES:
            continue
        if any(part.startswith(".") or part in {"__pycache__", "node_modules", "sample_data"}
               for part in relative.parts) and str(relative) != ".python-version":
            continue
        if path.is_symlink():
            raise ValueError(f"Release must not depend on symlinks: {relative}")
        if path.is_file() and path.suffix not in {".pyc", ".ipynb"}:
            files[relative.as_posix()] = path.read_bytes()
    hashes = {name: digest(content) for name, content in files.items()}
    release_id = "p0-" + digest(json.dumps(hashes, sort_keys=True).encode())[:12]
    commit = subprocess.run(["git", "-c", f"safe.directory={ROOT}", "rev-parse", "HEAD"],
                            cwd=ROOT, capture_output=True, text=True, check=False).stdout.strip()
    manifest = {"release_id": release_id, "base_commit": commit,
                "identity": "SHA-256 of packaged working-tree files, including intended local changes",
                "files": hashes}
    output.mkdir(parents=True, exist_ok=True)
    release_path = output / f"vibecleaning-{release_id}.zip"
    archive(release_path, files, manifest)
    fixture = ROOT / "tests/fixtures/deployment/data"
    data = {path.relative_to(fixture).as_posix(): path.read_bytes()
            for path in fixture.rglob("*") if path.is_file()}
    data_manifest = {"release_id": release_id, "synthetic": True,
                     "studies_per_profile": 1, "individuals_per_study": 2, "fixes_per_study": 24,
                     "files": {name: digest(content) for name, content in sorted(data.items())}}
    data_path = output / f"vibecleaning-{release_id}-test-data.zip"
    archive(data_path, data, data_manifest)
    checksums = {path.name: digest(path.read_bytes()) for path in (release_path, data_path)}
    (output / f"{release_id}-sha256.json").write_text(json.dumps(checksums, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"release_id": release_id, "archives": checksums, "output": str(output.resolve())}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    build(parser.parse_args().output)
