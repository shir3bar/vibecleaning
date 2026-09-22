"""Export regressions using a tiny, synthetic base-R data.frame."""

import base64
from pathlib import Path
import shutil
import sys
import zlib

import pandas as pd
import pytest
from rdata.parser import RObjectType
import rdata

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from examples.movement import rds_export
from examples.movement.rds_index import RDS_REVIEW_COLUMNS, read_movement_rds


# Generated in base R (no sf/move2 or R installation needed to run Python tests):
# x <- data.frame(sequence=1:5, value=c(10,20,30,40,50), label=letters[1:5])
# row.names(x) <- paste0("event-",1:5)
# attr(x,"track_data") <- data.frame(id="animal-A", row.names="track-A")
# attr(x,"custom_metadata") <- list(units="metres", nested=data.frame(a=1:2))
# memCompress(serialize(x,NULL,version=3),"gzip")
_FIXTURE = (
    "eJyVUstKw0AUPXnZB/iA+gluW1AruNIKIrhXcFdukxGCmanNTOrWLxa/oCOTzJRkCIrZ3Pe552bO"
    "yxhAhDiJECUAkDw/PUyvgWhSF4AvACGAADFGAI7SNX+nVC1zoSTbeNV4RZLZ3GGdb+zOTBrExSnq7"
    "/a7a1s9yeKsSSzm1t5Ye2/t437mxPTb3QE5Z+Wc1DmZcxgQtwkngjiTFimyyaFkm4qJlLmmLRXVP"
    "ihoxQofJi1IOhiXHGekaPZaEve3jsr1x6y92d0wYFsm1PS8G150w8tuOO+GV96usSopfVsaLvZZA4"
    "/nkETOqZjeNZOR9uphnjWVWP91YeJ3DOrtBtq8b4fYcVpJteZLzhQ17MKJlU4b4YAzVZofteduJRn"
    "oxob6V8FZpfQLrvfegP51rlkXfgLQWu96YEOnkUrkSrqrBJOKZZbp7gftFG3s"
)


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source.rds"
    path.write_bytes(zlib.decompress(base64.b64decode(_FIXTURE)))
    return path


@pytest.mark.parametrize("engine", ["python", "r"])
@pytest.mark.parametrize("annotated", [False, True])
def test_review_round_trip_preserves_compact_vectors_and_metadata(source, tmp_path, engine, annotated):
    if engine == "r" and not shutil.which("Rscript"):
        pytest.skip("Rscript is not installed")
    writer = getattr(rds_export, f"write_reviewed_rds_{engine}")
    original_bytes = source.read_bytes()
    original = read_movement_rds(source)
    assert list(original.index) == [f"event-{i}" for i in range(1, 6)]
    assert rdata.parser.parse_file(source, expand_altrep=False).object.value[0].info.type == RObjectType.ALTREP
    columns = {name: [None] * 5 for name in RDS_REVIEW_COLUMNS}
    if annotated:
        columns["outlier_status"][0] = "suspected"
        columns["outlier_comments"][0] = "001"
    output = tmp_path / "reviewed.rds"
    writer(source, output, columns)
    rds_export._compare_original_columns(source, output, columns)
    original_attrs = rds_export._attributes(rdata.parser.parse_file(source).object)
    reviewed_attrs = rds_export._attributes(rdata.parser.parse_file(output).object)
    for name in ("track_data", "custom_metadata"):
        assert repr(rdata.conversion.convert(reviewed_attrs[name])) == repr(
            rdata.conversion.convert(original_attrs[name])
        )
    # Even entirely missing generated columns must be character, not logical.
    parsed = rdata.parser.parse_file(output, expand_altrep=False).object
    assert all(col.info.type == RObjectType.STR for col in parsed.value[-4:])
    columns["outlier_status"][0] = "confirmed"
    second = tmp_path / "reviewed_again.rds"
    writer(output, second, columns)
    rds_export._compare_original_columns(source, second, columns)
    assert source.read_bytes() == original_bytes


def test_python_writer_rejects_wrong_projection_length(source, tmp_path):
    with pytest.raises(ValueError, match="projection length"):
        rds_export.write_reviewed_rds_python(
            source, tmp_path / "bad.rds", {name: [None] * 4 for name in RDS_REVIEW_COLUMNS}
        )


def test_validation_handles_legacy_all_missing_logical_columns(monkeypatch):
    original = pd.DataFrame({"value": [1, 2]})
    output = original.copy()
    for name in RDS_REVIEW_COLUMNS:
        output[name] = pd.array([pd.NA, pd.NA], dtype="boolean")
    monkeypatch.setattr(rds_export, "read_movement_rds", lambda path: original if path.name == "source.rds" else output)
    columns = {name: [None, None] for name in RDS_REVIEW_COLUMNS}
    rds_export._compare_original_columns(Path("source.rds"), Path("output.rds"), columns)
    output.loc[0, "outlier_status"] = True
    with pytest.raises(ValueError, match="incorrect generated outlier_status"):
        rds_export._compare_original_columns(Path("source.rds"), Path("output.rds"), columns)
