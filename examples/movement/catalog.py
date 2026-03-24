from pathlib import Path
import re

from app.state import ensure_project_state, has_project_inputs, load_dataset


FAMILY_SPECS = [
    {"name": "movement_clean", "label": "Clean"},
    {"name": "movement_hightemporalres", "label": "High Temporal Resolution"},
    {"name": "movement_raw", "label": "Raw"},
]

SAFE_PATH_PART = re.compile(r"^[A-Za-z0-9._-]+$")


def validate_catalog_part(raw_value: object, *, label: str) -> str:
    if not isinstance(raw_value, str):
        raise ValueError(f"Invalid {label}")
    value = raw_value.strip()
    if not value or not SAFE_PATH_PART.fullmatch(value):
        raise ValueError(f"Invalid {label}")
    return value


def family_names() -> list[str]:
    return [item["name"] for item in FAMILY_SPECS]


def detect_study_slug(filename: str) -> str:
    stem = Path(filename).stem
    match = re.match(r"^([A-Za-z0-9]+_[0-9a-f]{8,12})_", stem)
    if match:
        base = match.group(1)
    else:
        base = stem
    slug = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_").lower()
    if not slug:
        raise ValueError(f"Could not derive study slug from {filename}")
    return slug


def _family_spec_map() -> dict[str, dict[str, str]]:
    return {item["name"]: item for item in FAMILY_SPECS}


def get_family_dir(data_root: Path, family_name: str) -> Path:
    family = validate_catalog_part(family_name, label="family")
    if family not in _family_spec_map():
        raise ValueError("Unknown movement family")
    family_dir = (data_root / family).resolve()
    if data_root.resolve() not in family_dir.parents:
        raise ValueError("Invalid family")
    if not family_dir.exists() or not family_dir.is_dir():
        raise ValueError("Unknown movement family")
    return family_dir


def get_study_dir(data_root: Path, family_name: str, study_name: str) -> Path:
    family_dir = get_family_dir(data_root, family_name)
    study = validate_catalog_part(study_name, label="study")
    study_dir = (family_dir / study).resolve()
    if family_dir.resolve() not in study_dir.parents:
        raise ValueError("Invalid study")
    if not study_dir.exists() or not study_dir.is_dir():
        raise ValueError("Unknown study")
    if not has_project_inputs(study_dir):
        raise ValueError("Unknown study")
    return study_dir


def list_families(data_root: Path) -> list[dict[str, object]]:
    data_root = data_root.resolve()
    families = []
    for spec in FAMILY_SPECS:
        family_dir = data_root / spec["name"]
        study_count = 0
        if family_dir.exists() and family_dir.is_dir():
            study_count = len(list_studies(data_root, spec["name"]))
        families.append(
            {
                "name": spec["name"],
                "label": spec["label"],
                "study_count": study_count,
            }
        )
    return families


def list_studies(data_root: Path, family_name: str) -> list[dict[str, object]]:
    family_dir = get_family_dir(data_root, family_name)
    studies = []
    for study_dir in sorted(family_dir.iterdir()):
        if not study_dir.is_dir() or study_dir.name.startswith("."):
            continue
        if not has_project_inputs(study_dir):
            continue
        try:
            project_state = ensure_project_state(study_dir)
            dataset = load_dataset(study_dir, project_state["current_dataset_id"])
        except ValueError:
            continue
        studies.append(
            {
                "name": study_dir.name,
                "current_dataset_id": dataset["dataset_id"],
                "artifact_count": len(dataset.get("artifacts", [])),
            }
        )
    return studies
