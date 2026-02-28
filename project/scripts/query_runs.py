from __future__ import annotations
import argparse, json, sqlite3, sys
from pathlib import Path

# Support both `python -m scripts.query_runs` (package import) and direct invocation.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR.parent))

from scripts.build_run_registry import query_top_promoted, DATA_ROOT


def main() -> int:
    parser = argparse.ArgumentParser(description="Query the run registry SQLite database")
    parser.add_argument(
        "--db",
        default=None,
        help="Path to the SQLite registry file (default: data/meta/runs.sqlite)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of top promoted runs to display (used with default query)",
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="Arbitrary SQL query to execute; results printed as JSON",
    )
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DATA_ROOT / "meta" / "runs.sqlite"

    if not db_path.exists():
        print(f"Error: registry not found at {db_path}", file=sys.stderr)
        print("Run build_run_registry.py first to create it.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        if args.sql:
            cur = conn.execute(args.sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        else:
            rows = query_top_promoted(conn, limit=args.limit)

        print(json.dumps(rows, indent=2))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
