"""Recoverable publication of an export and its completed-review record."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

from app.edit_locks import project_mutation_lock
from app.filesystem import atomic_replace, atomic_write_json, exclusive_file_lock
from app.reviews import load_review_state
from app.state import project_paths


def _paths(study_dir):
    root = project_paths(study_dir)["meta"]
    return root, root / "cleaned_files", root / ".completion_previous", root / ".completion-publication.json"


def _recover(study_dir):
    root, destination, backup, journal = _paths(study_dir)
    if not journal.exists():
        return
    transaction = json.loads(journal.read_text(encoding="utf-8"))
    staged = root / transaction["staged"]
    committed = any((review.get("completion_export") or {}).get("analysis_id") == transaction["analysis_id"]
                    for review in load_review_state(study_dir).get("reviews", []))
    if not committed:
        if backup.exists():
            if destination.exists():
                shutil.rmtree(destination)
            atomic_replace(backup, destination)
        elif not transaction["had_previous"] and not staged.exists() and destination.exists():
            shutil.rmtree(destination)
    if backup.exists():
        shutil.rmtree(backup)
    if staged.exists():
        shutil.rmtree(staged)
    journal.unlink()


def recover_publication(study_dir):
    _, destination, _, journal = _paths(study_dir)
    if journal.exists():
        with project_mutation_lock(study_dir), exclusive_file_lock(destination.with_suffix(".lock")):
            _recover(study_dir)


@contextmanager
def prepared_publication(study_dir, output_dir, manifest):
    """Copy/check files before the caller takes the study mutation lock."""
    root, destination, backup, journal = _paths(study_dir)
    staged = Path(tempfile.mkdtemp(prefix=".completion-ready-", dir=root))
    try:
        for item in manifest["files"] + manifest.get("auxiliary_files", []):
            name = item["output_name"]
            if Path(name).name != name:
                raise ValueError("Invalid export filename")
            shutil.copy2(output_dir / name, staged / name)
            if hashlib.sha256((staged / name).read_bytes()).hexdigest() != item["output_sha256"]:
                raise ValueError(f"Export checksum mismatch: {name}")
        shutil.copy2(output_dir / "writer_manifest.json", staged / "writer_manifest.json")

        def publish(result, save_review):
            # complete_review invokes this only after its guarded validation.
            with exclusive_file_lock(destination.with_suffix(".lock")):
                _recover(study_dir)
                if destination.exists():
                    previous = json.loads((destination / "writer_manifest.json").read_text(encoding="utf-8"))
                    expected = {item["output_name"] for item in previous["files"] + previous.get("auxiliary_files", [])}
                    expected.add("writer_manifest.json")
                    actual = {path.name for path in destination.iterdir() if path.name != ".DS_Store"}
                    if destination.is_symlink() or actual != expected:
                        raise ValueError("cleaned_files contains files outside the previous export")
                atomic_write_json(journal, {"analysis_id": manifest["analysis_id"],
                    "had_previous": destination.exists(), "staged": staged.name})
                try:
                    if destination.exists():
                        atomic_replace(destination, backup)
                    atomic_replace(staged, destination)
                    save_review()
                except Exception:
                    _recover(study_dir)
                    raise
                # The durable review record is the commit marker. Cleanup can
                # safely be retried on the next request after an interruption.
                try:
                    _recover(study_dir)
                except OSError:
                    pass
        yield publish
    finally:
        if staged.exists() and not journal.exists():
            shutil.rmtree(staged, ignore_errors=True)
