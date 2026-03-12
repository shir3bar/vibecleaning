from pathlib import Path


def preview_artifact(path: Path, limit_bytes: int = 65536) -> dict:
    limit = max(256, min(limit_bytes, 2_000_000))
    with path.open("rb") as handle:
        raw = handle.read(limit)
    truncated = path.stat().st_size > len(raw)
    try:
        text = raw.decode("utf-8")
        return {
            "kind": "text",
            "byte_count": len(raw),
            "truncated": truncated,
            "text_preview": text,
        }
    except UnicodeDecodeError:
        return {
            "kind": "binary",
            "byte_count": len(raw),
            "truncated": truncated,
            "hex_preview": raw[:128].hex(),
        }
