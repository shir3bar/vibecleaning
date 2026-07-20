import argparse
import json
import sys
from urllib.parse import quote

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trigger the movement OSM context enrichment preprocessing action."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--family", required=True)
    parser.add_argument("--study", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--logical-name", default="movement.csv")
    parser.add_argument("--radius-m", required=True, type=float)
    parser.add_argument("--user", default="dev")
    parser.add_argument("--confirmed-large-download", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    url = (
        f"{args.base_url.rstrip('/')}/api/apps/movement/family/"
        f"{quote(args.family, safe='')}/study/{quote(args.study, safe='')}"
        "/actions/enrich-osm-context"
    )
    payload = {
        "dataset_id": args.dataset_id,
        "logical_name": args.logical_name,
        "search_radius_m": args.radius_m,
        "user": args.user,
        "confirmed_large_download": args.confirmed_large_download,
    }
    try:
        response = httpx.post(url, json=payload, timeout=None)
    except httpx.RequestError as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1

    if response.is_success:
        try:
            result = response.json()
        except json.JSONDecodeError:
            print(response.text)
        else:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    print(f"Request failed with HTTP {response.status_code}", file=sys.stderr)
    try:
        body = json.dumps(response.json(), indent=2, sort_keys=True)
    except json.JSONDecodeError:
        body = response.text
    print(body, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
