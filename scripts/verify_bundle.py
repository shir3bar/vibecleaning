"""Verify an extracted release/test-data bundle against its file manifest."""
import argparse
import hashlib
import json
from pathlib import Path


def verify(root):
    root = root.resolve()
    manifest = json.loads((root / "release-manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files"].items():
        path = root / name
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError(f"Missing or nonportable file: {name}")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Checksum mismatch: {name}")
    print(f"Verified {manifest['release_id']}: {len(manifest['files'])} files")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    verify(parser.parse_args().root)
