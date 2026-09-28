"""Reports must describe effective decisions and preserve Unicode on Windows."""
from copy import deepcopy
from pathlib import Path
import json

import pytest

from app.execution import create_analysis, create_step
from app.state import ensure_project_state
from examples.movement import report_analysis_template as report
from examples.movement.review_annotations import (
    apply_annotations_to_report_records, individual_review_decisions, normalize_annotation,
)


def review_fixture(tmp_path):
    path = tmp_path / "tracks.csv"
    path.write_text(
        "eventid,individual,timestamp,longitude,latitude,species,source\n"
        "a,animal-A,2024-01-01T00:00:00Z,1,2,Species A,original-A.rds\n"
        "b,animal-A,2024-01-01T01:00:00Z,1.1,2.1,Species A,original-A.rds\n"
        "c,animal-A,2024-01-01T02:00:00Z,1.2,2.2,Species A,original-A.rds\n",
        encoding="utf-8",
    )
    annotations = [
        {"annotation_id": "speed", "status": "suspected", "issue_type": "speed",
         "issue_threshold": "> 5 m/s", "scope": {"kind": "fix", "row_ranges": [[1, 2]]}},
        {"annotation_id": "gps", "status": "suspected", "issue_type": "GPS spike",
         "issue_threshold": "150°", "scope": {"kind": "fix", "row_ranges": [[1, 1]]}},
        {"annotation_id": "dismiss-speed", "annotation_kind": "dismissal", "status": "dismissed",
         "parent_annotation_id": "speed", "scope": {"kind": "dismissal", "row_ranges": [[1, 1]]}},
        {"annotation_id": "confirm-gps", "annotation_kind": "confirmation", "status": "confirmed",
         "parent_annotation_id": "gps", "scope": {"kind": "fix", "row_ranges": [[1, 1]]}},
        {"annotation_id": "decision", "annotation_kind": "individual_review", "reviewed": True,
         "review_decision": "fix_keep", "needs_check": True, "comment": "José: check 150° <tag>",
         "user": "José", "scope": {"kind": "individual", "individual": "animal-A"}},
    ]
    annotations = [normalize_annotation(item) for item in annotations]
    fields, columns, _, records = report.load_rows_with_context(path)
    original = deepcopy(annotations)
    apply_annotations_to_report_records(records, annotations, source_artifact="tracks.csv")
    assert annotations == original
    return annotations, fields, columns, records


def test_partial_dismissal_and_confirmation_have_independent_report_statuses(tmp_path):
    annotations, fields, columns, records = review_fixture(tmp_path)
    selected = report.selected_contexts(records, selected_issue_ids=["speed"])
    assert [record["row_index"] for record in selected] == [2]
    assert report.issue_types_for(records[0]) == ["GPS spike"]
    sections = report.build_issue_sections(records[:2], [], fields, columns)
    by_type = {section["issue_type"]: section for section in sections}
    assert by_type["speed"]["status_counts"] == {"suspected": 1}
    assert by_type["speed"]["issue_ids"] == ["speed"]
    assert by_type["GPS spike"]["status_counts"] == {"confirmed": 1}
    assert by_type["GPS spike"]["issue_ids"] == ["gps"]
    assert len(annotations) == 5  # Audit history was not deleted.


def test_overlapping_active_issues_do_not_inherit_each_others_status(tmp_path):
    _, fields, columns, records = review_fixture(tmp_path)
    records[0]["review"]["effective_issues"][0]["status"] = "suspected"
    sections = report.build_issue_sections(records[:1], [], fields, columns)
    assert {section["issue_type"]: section["status_counts"] for section in sections} == {
        "speed": {"suspected": 1}, "GPS spike": {"confirmed": 1},
    }


def test_profile_includes_resolved_source_species_decision_and_escaped_notes(tmp_path):
    annotations, fields, columns, records = review_fixture(tmp_path)
    sections = report.build_individual_profile_sections(
        records, fields, columns, ["animal-A"], "wrong-first-file.rds",
        individual_review_decisions(annotations, source_artifact="tracks.csv"),
    )
    html = report.build_individual_profile_html_report("wrong-first-file.rds", "José", sections)
    markdown = report.build_individual_profile_markdown_report("wrong-first-file.rds", "José", sections)
    assert "wrong-first-file.rds" not in html
    for value in ("original-A.rds", "Species A", "Fix &amp; Keep", "José: check 150° &lt;tag&gt;"):
        assert value in html
    assert "Needs check:</strong> Yes" in html
    assert "José: check 150° <tag>" in markdown
    assert sections[0]["reviewed_fix_count"] == 2


@pytest.mark.parametrize("output_mode", ["combined", "separate"])
def test_profile_summary_uses_latest_decisions_and_includes_unreviewed(tmp_path, output_mode):
    annotations, fields, columns, records = review_fixture(tmp_path)
    notes = "José: check 150° | <tag>\nSecond line"
    annotations[-1]["comment"] = notes
    for individual, decision, needs_check in [
        ("animal-B", "remove", True), ("animal-B", "ok", False),
        ("animal-C", "remove", True), ("outside-report", "ok", False),
    ]:
        annotations.append(normalize_annotation({
            "annotation_kind": "individual_review", "reviewed": True,
            "review_decision": decision, "needs_check": needs_check, "user": "Reviewer",
            "scope": {"kind": "individual", "individual": individual},
        }))
    decisions = individual_review_decisions(annotations, source_artifact="tracks.csv")
    base = report.build_individual_profile_sections(
        records, fields, columns, ["animal-A"], "tracks.csv", decisions,
    )[0]
    sections = [dict(base, individual=name, review_decision=decisions.get(name, {}))
                for name in ("animal-A", "animal-B", "animal-C", "animal-D")]
    if output_mode == "combined":
        html = report.build_individual_profile_html_report("tracks.csv", "Reviewer", sections)
        markdown = report.build_individual_profile_markdown_report("tracks.csv", "Reviewer", sections)
    else:
        plan = [{"individual": item["individual"], "markdown_name": f"{index}.md",
                 "html_name": f"{index}.html"} for index, item in enumerate(sections)]
        html = report.build_individual_profile_index_html(sections, plan)
        markdown = report.build_individual_profile_index_markdown(sections, plan)
        assert 'href="0.html"' in html
    summary_html = html.split('<section class="review-summary">', 1)[1].split("</section>", 1)[0]
    summary_markdown = markdown.split("## Review decision summary", 1)[1].split("\n## Individual", 1)[0]
    for summary in (summary_html, summary_markdown):
        assert "4 individuals included in this report" in summary
        for label in ("OK", "Remove", "Not reviewed"):
            assert f"{label}: 1" in summary
        assert "Needs check: 2" in summary
        assert "outside-report" not in summary
        assert "<tag>" not in summary
    assert "Fix &amp; Keep: 1" in summary_html
    assert "Fix & Keep: 1" in summary_markdown
    assert "<td>animal-B</td><td>OK</td><td>No</td>" in summary_html
    assert "<td>animal-D</td><td>Not reviewed</td><td>—</td>" in summary_html
    assert "José: check 150° | &lt;tag&gt;<br>Second line" in summary_html
    assert "José: check 150° &#124; &lt;tag&gt;<br>Second line" in summary_markdown
    table_rows = [line for line in summary_markdown.splitlines() if line.startswith("| ")]
    assert len(table_rows) == 6  # Header, separator, four individuals.
    assert all(line.count("|") == 6 for line in table_rows)


@pytest.mark.parametrize("operation", [create_analysis, create_step])
def test_generated_scripts_and_outputs_use_utf8_under_legacy_default(tmp_path, monkeypatch, operation):
    project = tmp_path / "données 漢字"
    project.mkdir()
    (project / "source.txt").write_text("raw", encoding="utf-8")
    ensure_project_state(project)
    original_write = Path.write_text

    def legacy_default(path, data, encoding=None, errors=None, newline=None):
        return original_write(path, data, encoding=encoding or "cp1252", errors=errors, newline=newline)

    monkeypatch.setattr(Path, "write_text", legacy_default)
    result = operation(project, {
        "user": "José", "title": "Unicode report", "kind": "python",
        "input_artifacts": ["source.txt"], "output_artifacts": ["report.md"],
        "script": '''import json, os
from pathlib import Path
spec = json.loads(Path(os.environ["VIBECLEANING_SPEC_PATH"]).read_text(encoding="utf-8"))
text = "José 150° — 鹿"
Path(spec["output_artifacts"][0]["path"]).write_text(text, encoding="utf-8")
Path(os.environ["VIBECLEANING_SUMMARY_PATH"]).write_text(json.dumps({"text": text}, ensure_ascii=False), encoding="utf-8")
print(text)
''',
    })
    summary = result["summary"] if operation is create_analysis else result["step"]["summary"]
    assert summary["text"] == "José 150° — 鹿"
    assert (project / "source.txt").read_text(encoding="utf-8") == "raw"
