import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.alpha_bundle import build_cross_section_features


def test_resolve_symbol_feature_path_supports_signals_prefix(tmp_path: Path) -> None:
    base_dir = tmp_path / "signals"
    base_dir.mkdir(parents=True, exist_ok=True)
    expected = base_dir / "signals_BTCUSDT.parquet"
    expected.write_text("x", encoding="utf-8")

    resolved = build_cross_section_features._resolve_symbol_feature_path(base_dir, "BTCUSDT")
    assert resolved == expected


def test_resolve_symbol_feature_path_prefers_exact_symbol_file(tmp_path: Path) -> None:
    base_dir = tmp_path / "signals"
    base_dir.mkdir(parents=True, exist_ok=True)
    exact = base_dir / "ETHUSDT.parquet"
    prefixed = base_dir / "signals_ETHUSDT.parquet"
    exact.write_text("exact", encoding="utf-8")
    prefixed.write_text("prefixed", encoding="utf-8")

    resolved = build_cross_section_features._resolve_symbol_feature_path(base_dir, "ETHUSDT")
    assert resolved == exact
