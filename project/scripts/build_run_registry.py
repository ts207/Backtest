from __future__ import annotations
import argparse, json, logging, os, sqlite3, sys
from pathlib import Path
from typing import Dict, List

DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", str(Path(__file__).resolve().parents[3] / "data")))


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            stage TEXT,
            status TEXT,
            survivors_count INTEGER,
            tested_count INTEGER,
            timestamp TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promoted_strategies (
            run_id TEXT,
            strategy_id TEXT,
            blueprint_id TEXT,
            family TEXT,
            trades INTEGER,
            PRIMARY KEY (run_id, strategy_id)
        )
    """)


def upsert_run(conn: sqlite3.Connection, row: Dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO runs (run_id, stage, status, survivors_count, tested_count, timestamp)
        VALUES (:run_id, :stage, :status, :survivors_count, :tested_count, :timestamp)
    """, row)


def query_top_promoted(conn: sqlite3.Connection, limit: int = 10) -> List[Dict]:
    cur = conn.execute(
        "SELECT * FROM runs WHERE status='success' ORDER BY survivors_count DESC LIMIT ?",
        (limit,)
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Index run manifests into SQLite registry")
    parser.add_argument("--data_root", default=str(DATA_ROOT))
    parser.add_argument("--db", default=None)
    args = parser.parse_args()
    data_root = Path(args.data_root)
    db_path = Path(args.db) if args.db else data_root / "meta" / "runs.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    create_schema(conn)

    for manifest_path in sorted((data_root / "runs").glob("*/run_manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logging.warning("Skipping malformed manifest %s: %s", manifest_path, exc)
            continue
        run_id = manifest.get("run_id", manifest_path.parent.name)
        upsert_run(conn, {
            "run_id": run_id,
            "stage": manifest.get("stage", ""),
            "status": manifest.get("status", ""),
            "survivors_count": int(manifest.get("stats", {}).get("survivors_count", 0) or 0),
            "tested_count": int(manifest.get("stats", {}).get("tested_count", 0) or 0),
            "timestamp": manifest.get("started_at", ""),
        })
    conn.commit()
    conn.close()
    print(f"Registry updated: {db_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
