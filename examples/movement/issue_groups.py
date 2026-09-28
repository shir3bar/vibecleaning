"""Exact, individual-scoped groups of unresolved saved allegations."""


def group_unresolved_issues(fixes: list[dict], individual: str) -> dict:
    groups: dict[str, dict] = {}
    included = {}
    for fix in fixes:
        if fix.get("individual") != individual:
            continue
        key = str(fix.get("fix_key") or "")
        if not key:
            continue
        review = fix.get("review") or {}
        for issue in review.get("effective_issues", review.get("issues", [])):
            parent_id = str(issue.get("parent_issue_id") or issue.get("issue_id") or "")
            if issue.get("status") != "suspected" or not parent_id:
                continue
            issue_type = str(issue.get("issue_type") or "").strip()
            group = groups.setdefault(issue_type, {"fix_keys": set(), "parents": {}})
            group["fix_keys"].add(key)
            group["parents"].setdefault(parent_id, set()).add(key)
            included[key] = fix
    return {
        "individual": individual,
        "groups": [
            {
                "issue_type": issue_type,
                "fix_count": len(group["fix_keys"]),
                "fix_keys": sorted(group["fix_keys"]),
                "resolutions": [
                    {"parent_annotation_id": parent_id, "fix_keys": sorted(keys)}
                    for parent_id, keys in sorted(group["parents"].items())
                ],
            }
            for issue_type, group in sorted(groups.items())
        ],
        "fixes": list(included.values()),
    }
