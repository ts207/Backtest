from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))


@dataclass(frozen=True)
class Artifact:
    category: str
    source: Path
    rel_target: Path


def _collect_artifacts(reports_root: Path) -> Dict[str, List[Artifact]]:
    by_run: Dict[str, List[Artifact]] = {}

    bt_root = reports_root / "vol_compression_expansion_v1"
    if bt_root.exists():
        for run_dir in sorted(p for p in bt_root.iterdir() if p.is_dir()):
            run_id = run_dir.name
            for file_path in sorted(run_dir.glob("*")):
                if not file_path.is_file():
                    continue
                art = Artifact(
                    category="backtest",
                    source=file_path,
                    rel_target=Path("backtest") / "vol_compression_expansion_v1" / file_path.name,
                )
                by_run.setdefault(run_id, []).append(art)

    phase1_root = reports_root / "vol_shock_relaxation"
    if phase1_root.exists():
        for run_dir in sorted(p for p in phase1_root.iterdir() if p.is_dir()):
            run_id = run_dir.name
            for file_path in sorted(run_dir.glob("*")):
                if not file_path.is_file():
                    continue
                art = Artifact(
                    category="phase1",
                    source=file_path,
                    rel_target=Path("research") / "vol_shock_relaxation" / file_path.name,
                )
                by_run.setdefault(run_id, []).append(art)

    phase2_root = reports_root / "phase2"
    if phase2_root.exists():
        for run_dir in sorted(p for p in phase2_root.iterdir() if p.is_dir()):
            run_id = run_dir.name
            for event_dir in sorted(p for p in run_dir.iterdir() if p.is_dir()):
                event_type = event_dir.name
                for file_path in sorted(event_dir.glob("*")):
                    if not file_path.is_file():
                        continue
                    art = Artifact(
                        category="phase2",
                        source=file_path,
                        rel_target=Path("research") / "phase2" / event_type / file_path.name,
                    )
                    by_run.setdefault(run_id, []).append(art)

    return by_run


def _safe_remove(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _materialize(source: Path, target: Path, mode: str, force: bool) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        if not force:
            return
        _safe_remove(target)

    if mode == "symlink":
        try:
            target.symlink_to(source.resolve())
            return
        except OSError:
            # Fallback for systems where symlink is unavailable.
            pass
    shutil.copy2(source, target)


def _write_run_readme(run_dir: Path, run_id: str, artifacts: List[Artifact]) -> None:
    lines = [
        f"# Report Index: {run_id}",
        "",
        "## Files",
    ]
    for art in sorted(artifacts, key=lambda x: str(x.rel_target)):
        lines.append(f"- `{art.rel_target}`")
    (run_dir / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_root_readme(out_root: Path, by_run: Dict[str, List[Artifact]]) -> None:
    lines = [
        "# Reports By Run",
        "",
        "| run_id | backtest | phase1_vsr | phase2 |",
        "| --- | --- | --- | --- |",
    ]
    for run_id in sorted(by_run.keys()):
        cats = {a.category for a in by_run[run_id]}
        lines.append(
            f"| `{run_id}` | {'yes' if 'backtest' in cats else 'no'} | "
            f"{'yes' if 'phase1' in cats else 'no'} | {'yes' if 'phase2' in cats else 'no'} |"
        )
    (out_root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Organize reports into a run-centric index directory.")
    parser.add_argument("--reports_root", default=str(DATA_ROOT / "reports"))
    parser.add_argument("--out_root", default=str(DATA_ROOT / "reports" / "by_run"))
    parser.add_argument("--mode", choices=["symlink", "copy"], default="symlink")
    parser.add_argument("--force", type=int, default=1)
    args = parser.parse_args()

    reports_root = Path(args.reports_root)
    out_root = Path(args.out_root)
    force = bool(int(args.force))

    by_run = _collect_artifacts(reports_root)
    out_root.mkdir(parents=True, exist_ok=True)

    for run_id, artifacts in sorted(by_run.items()):
        run_dir = out_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        for art in artifacts:
            _materialize(art.source, run_dir / art.rel_target, args.mode, force=force)
        _write_run_readme(run_dir, run_id, artifacts)

    _write_root_readme(out_root, by_run)
    manifest = {
        "reports_root": str(reports_root),
        "out_root": str(out_root),
        "runs": {run_id: len(arts) for run_id, arts in sorted(by_run.items())},
        "mode": args.mode,
    }
    (out_root / "index_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote run-centric report index: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
