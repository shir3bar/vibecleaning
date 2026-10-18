"""Portable individual-decision CSV shared by downloads and saved exports."""

import csv
import io
import hashlib


DECISION_FIELDS = (
    "study", "dataset_id", "individual", "review_decision", "removal_reason",
    "needs_check", "comment", "review_id", "review_round", "reviewer_user_id",
    "reviewer_username", "reviewer_display_name", "reviewed_at", "step_id",
)


def decision_rows(study, dataset_id, individuals, decisions, review=None, needs_checks=()):
    review = review or {}
    rows = []
    for individual in sorted(set(individuals)):
        decision = decisions.get(individual) or {}
        actor = decision.get("actor") or (review.get("reviewer") if decision else {}) or {}
        rows.append({
            "study": study, "dataset_id": dataset_id, "individual": individual,
            "review_decision": decision.get("review_decision") or "undecided",
            "removal_reason": decision.get("removal_reason") or "",
            "needs_check": "true" if decision.get("needs_check") or individual in needs_checks else "false",
            "comment": decision.get("comment") or "",
            "review_id": decision.get("review_id") or review.get("review_id") or "",
            "review_round": decision.get("review_round") or review.get("review_round") or "",
            "reviewer_user_id": actor.get("user_id") or "",
            "reviewer_username": actor.get("username") or "",
            "reviewer_display_name": actor.get("display_name") or decision.get("user") or "",
            "reviewed_at": decision.get("created_at") or "",
            "step_id": decision.get("step_id") or "",
        })
    return rows


def decision_csv(rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=DECISION_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def save_decision_export(output_dir, rows):
    path = output_dir / "individual_decisions.csv"
    path.write_text(decision_csv(rows), encoding="utf-8", newline="")
    return {"output_name": path.name, "row_count": len(rows),
            "output_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
