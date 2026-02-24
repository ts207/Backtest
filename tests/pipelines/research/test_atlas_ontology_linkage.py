from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))


def test_generate_candidate_templates_enriches_event_rows_with_ontology_context(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_001",
                "concept_id": "C_1",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""CROSS_VENUE_DESYNC""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 10,
                "assets": "*",
            }
        ]
    ).to_csv(backlog_path, index=False)

    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
    ]

    with patch.object(sys, "argv", test_args):
        rc = gct.main()
    assert rc == 0

    templates = pd.read_parquet(atlas_dir / "candidate_templates.parquet")
    assert not templates.empty
    row = templates.iloc[0]
    assert "runtime_event_type" in templates.columns
    assert row["runtime_event_type"] == "CROSS_VENUE_DESYNC"
    assert row["event_type"] == "CROSS_VENUE_DESYNC"
    assert row["canonical_event_type"] == "CROSS_VENUE_DESYNC"
    assert row["canonical_family"] == "INFORMATION_DESYNC"
    assert bool(row["ontology_event_known"]) is True
    assert bool(row["ontology_in_taxonomy"]) is True
    assert bool(row["ontology_in_canonical_registry"]) is True
    assert bool(row["ontology_fully_known"]) is True
    assert "DESYNC_PERSISTENCE_STATE" in list(row["ontology_source_states"])
    assert "DESYNC_PERSISTENCE_STATE" in list(row["ontology_all_states"])
    assert list(row["ontology_unknown_templates"]) == []
    assert set(row["rule_templates"]) == {
        "desync_repair",
        "convergence",
        "basis_repair",
        "lead_lag_follow",
        "divergence_continuation",
    }

    linkage = json.loads((atlas_dir / "ontology_linkage.json").read_text(encoding="utf-8"))
    assert str(linkage.get("ontology_spec_hash", "")).startswith("sha256:")
    assert int(linkage.get("counts", {}).get("events_in_taxonomy", 0)) > 0
    assert "events_missing_in_canonical_registry" in linkage.get("unresolved", {})


def test_generate_candidate_templates_strict_mode_fails_closed_on_unknown_template(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_STRICT",
                "concept_id": "C_STRICT",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""CROSS_VENUE_DESYNC""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 1,
                "assets": "*",
            }
        ]
    ).to_csv(backlog_path, index=False)

    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    bad_taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC"],
                "runtime_templates": ["desync_repair", "invalid_template_verb"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
        "--ontology_strict",
        "1",
    ]

    with patch.object(gct, "_load_taxonomy", return_value=bad_taxonomy):
        with patch.object(gct, "_load_canonical_event_registry", return_value=canonical_registry):
            with patch.object(gct, "_load_state_registry", return_value=state_registry):
                with patch.object(gct, "_load_template_verb_lexicon", return_value=verb_lexicon):
                    with patch.object(sys, "argv", test_args):
                        rc = gct.main()

    assert rc == 1
    assert not (atlas_dir / "candidate_templates.parquet").exists()


def test_generate_candidate_templates_strict_mode_fails_closed_on_missing_canonical_event(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_MISSING_CANON",
                "concept_id": "C_STRICT",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""UNKNOWN_EVENT""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 1,
                "assets": "*",
            }
        ]
    ).to_csv(backlog_path, index=False)

    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["UNKNOWN_EVENT"],
                "runtime_templates": ["desync_repair"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
        "--ontology_strict",
        "1",
    ]

    with patch.object(gct, "_load_taxonomy", return_value=taxonomy):
        with patch.object(gct, "_load_canonical_event_registry", return_value=canonical_registry):
            with patch.object(gct, "_load_state_registry", return_value=state_registry):
                with patch.object(gct, "_load_template_verb_lexicon", return_value=verb_lexicon):
                    with patch.object(sys, "argv", test_args):
                        rc = gct.main()

    assert rc == 1
    assert not (atlas_dir / "candidate_templates.parquet").exists()


def test_generate_candidate_templates_strict_mode_fails_on_state_registry_invalid_source_event(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_STATE_SRC",
                "concept_id": "C_STRICT",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""CROSS_VENUE_DESYNC""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 1,
                "assets": "*",
            }
        ]
    ).to_csv(backlog_path, index=False)

    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC"],
                "runtime_templates": ["desync_repair"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {
        "states": [
            {"state_id": "BROKEN_STATE", "source_event_type": "MISSING_EVENT"},
        ]
    }
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
        "--ontology_strict",
        "1",
    ]

    with patch.object(gct, "_load_taxonomy", return_value=taxonomy):
        with patch.object(gct, "_load_canonical_event_registry", return_value=canonical_registry):
            with patch.object(gct, "_load_state_registry", return_value=state_registry):
                with patch.object(gct, "_load_template_verb_lexicon", return_value=verb_lexicon):
                    with patch.object(sys, "argv", test_args):
                        rc = gct.main()
    assert rc == 1


def test_ontology_linkage_manifest_counts_and_unresolved_match_parquet(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_001",
                "concept_id": "C_1",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""CROSS_VENUE_DESYNC""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 10,
                "assets": "*",
            },
            {
                "claim_id": "CL_002",
                "concept_id": "C_2",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""UNKNOWN_EVENT""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 20,
                "assets": "*",
            },
        ]
    ).to_csv(backlog_path, index=False)

    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC", "UNKNOWN_EVENT"],
                "runtime_templates": ["desync_repair", "invalid_template_verb"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {
        "states": [
            {"state_id": "DESYNC_PERSISTENCE_STATE", "source_event_type": "CROSS_VENUE_DESYNC"},
            {"state_id": "BROKEN_STATE", "source_event_type": "MISSING_EVENT"},
        ]
    }
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
        "--ontology_strict",
        "0",
        "--implemented_only_events",
        "0",
    ]

    with patch.object(gct, "_load_taxonomy", return_value=taxonomy):
        with patch.object(gct, "_load_canonical_event_registry", return_value=canonical_registry):
            with patch.object(gct, "_load_state_registry", return_value=state_registry):
                with patch.object(gct, "_load_template_verb_lexicon", return_value=verb_lexicon):
                    with patch.object(sys, "argv", test_args):
                        rc = gct.main()
    assert rc == 0

    templates = pd.read_parquet(atlas_dir / "candidate_templates.parquet")
    linkage = json.loads((atlas_dir / "ontology_linkage.json").read_text(encoding="utf-8"))

    counts = linkage.get("counts", {})
    unresolved = linkage.get("unresolved", {})
    assert int(counts.get("templates_total", -1)) == len(templates)
    assert int(counts.get("event_templates_total", -1)) == int((templates["object_type"] == "event").sum())
    assert int(counts.get("events_total", -1)) == int(templates["canonical_event_type"].nunique())
    assert int(counts.get("events_in_canonical_registry", -1)) == 1
    assert int(counts.get("unknown_templates_total", -1)) == 2
    assert int(counts.get("states_total", -1)) == 2
    assert int(counts.get("states_linked_total", -1)) == 1
    assert unresolved.get("events_missing_in_canonical_registry") == ["UNKNOWN_EVENT"]
    assert unresolved.get("templates_missing_in_verb_lexicon") == ["invalid_template_verb"]
    assert unresolved.get("states_with_missing_source_event") == ["BROKEN_STATE"]


def test_generate_candidate_plan_prefers_canonical_event_type_column(tmp_path):
    run_id = "r_onto_plan"
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    pd.DataFrame(
        [
            {
                "template_id": "CL_001@VOL_SHOCK",
                "source_claim_id": "CL_001",
                "concept_id": "C_1",
                "object_type": "event",
                "runtime_event_type": "vol_shock_relaxation",
                "event_type": "vol_shock_relaxation",
                "canonical_event_type": "VOL_SHOCK",
                "canonical_family": "VOLATILITY_TRANSITION",
                "ontology_in_taxonomy": True,
                "ontology_in_canonical_registry": True,
                "ontology_unknown_templates": [],
                "ontology_source_states": [],
                "ontology_family_states": [],
                "ontology_all_states": [],
                "ontology_spec_hash": "sha256:test",
                "target_spec_path": "spec/events/VOL_SHOCK.yaml",
                "rule_templates": ["mean_reversion"],
                "horizons": ["5m"],
                "conditioning": {"vol_regime": ["high"]},
                "assets_filter": "*",
                "min_events": 50,
            }
        ]
    ).to_parquet(atlas_dir / "candidate_templates.parquet", index=False)

    spec_dir = tmp_path / "spec" / "events"
    spec_dir.mkdir(parents=True)
    (spec_dir / "VOL_SHOCK.yaml").write_text(
        "event_type: VOL_SHOCK\n"
        "reports_dir: vol_shock_relaxation\n"
        "events_file: vol_shock_relaxation_events.csv\n"
        "signal_column: vol_shock_relaxation_event\n",
        encoding="utf-8",
    )

    lake_dir = tmp_path / "data" / "lake" / "cleaned" / "perp" / "BTCUSDT" / "bars_5m"
    lake_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(lake_dir / "bars.parquet")

    ctx_dir = tmp_path / "data" / "lake" / "context" / "market_state" / "BTCUSDT"
    ctx_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(ctx_dir / "5m.parquet")

    out_dir = tmp_path / "reports"
    test_args = [
        "generate_candidate_plan.py",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--atlas_dir",
        str(atlas_dir),
        "--out_dir",
        str(out_dir),
    ]

    import pipelines.research.generate_candidate_plan as gcp

    with patch.dict(os.environ, {"BACKTEST_DATA_ROOT": str(tmp_path / "data")}):
        with patch.object(sys, "argv", test_args):
            mock_project_root = tmp_path / "project"
            with patch.object(gcp, "PROJECT_ROOT", mock_project_root):
                with patch.object(gcp, "DATA_ROOT", tmp_path / "data"):
                    with patch.object(gcp, "ontology_spec_hash", return_value="sha256:test"):
                        rc = gcp.main()
    assert rc == 0

    plan_path = out_dir / "candidate_plan.jsonl"
    rows = [json.loads(x) for x in plan_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert rows
    assert all(row["event_type"] == "VOL_SHOCK" for row in rows)


def test_generate_candidate_plan_keeps_runtime_event_type_and_canonical_ids_unique(tmp_path):
    run_id = "r_onto_alias"
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    pd.DataFrame(
        [
            {
                "template_id": "CL_ALIAS@VOL_SHOCK",
                "source_claim_id": "CL_ALIAS",
                "concept_id": "C_ALIAS",
                "object_type": "event",
                "runtime_event_type": "vol_shock_relaxation",
                "event_type": "VOL_SHOCK",
                "canonical_event_type": "VOL_SHOCK",
                "canonical_family": "VOLATILITY_TRANSITION",
                "ontology_in_taxonomy": True,
                "ontology_in_canonical_registry": True,
                "ontology_unknown_templates": [],
                "ontology_source_states": [],
                "ontology_family_states": [],
                "ontology_all_states": [],
                "ontology_spec_hash": "sha256:test",
                "target_spec_path": "spec/events/VOL_SHOCK.yaml",
                "rule_templates": ["mean_reversion"],
                "horizons": ["5m"],
                "conditioning": {"vol_regime": ["high"]},
                "assets_filter": "*",
                "min_events": 50,
            }
        ]
    ).to_parquet(atlas_dir / "candidate_templates.parquet", index=False)

    spec_dir = tmp_path / "spec" / "events"
    spec_dir.mkdir(parents=True)
    (spec_dir / "VOL_SHOCK.yaml").write_text(
        "event_type: VOL_SHOCK\n"
        "reports_dir: vol_shock_relaxation\n"
        "events_file: vol_shock_relaxation_events.csv\n"
        "signal_column: vol_shock_relaxation_event\n",
        encoding="utf-8",
    )

    lake_dir = tmp_path / "data" / "lake" / "cleaned" / "perp" / "BTCUSDT" / "bars_5m"
    lake_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(lake_dir / "bars.parquet")

    ctx_dir = tmp_path / "data" / "lake" / "context" / "market_state" / "BTCUSDT"
    ctx_dir.mkdir(parents=True)
    pd.DataFrame({"timestamp": [pd.Timestamp.now()]}).to_parquet(ctx_dir / "5m.parquet")

    out_dir = tmp_path / "reports"
    test_args = [
        "generate_candidate_plan.py",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--atlas_dir",
        str(atlas_dir),
        "--out_dir",
        str(out_dir),
    ]

    import pipelines.research.generate_candidate_plan as gcp

    with patch.dict(os.environ, {"BACKTEST_DATA_ROOT": str(tmp_path / "data")}):
        with patch.object(sys, "argv", test_args):
            mock_project_root = tmp_path / "project"
            with patch.object(gcp, "PROJECT_ROOT", mock_project_root):
                with patch.object(gcp, "DATA_ROOT", tmp_path / "data"):
                    with patch.object(gcp, "ontology_spec_hash", return_value="sha256:test"):
                        rc = gcp.main()
    assert rc == 0

    rows = [
        json.loads(x)
        for x in (out_dir / "candidate_plan.jsonl").read_text(encoding="utf-8").splitlines()
        if x.strip()
    ]
    assert rows
    assert all(row["event_type"] == "VOL_SHOCK" for row in rows)
    assert all(row["runtime_event_type"] == "vol_shock_relaxation" for row in rows)
    plan_ids = [row["plan_row_id"] for row in rows]
    assert len(plan_ids) == len(set(plan_ids))


def test_event_ontology_context_resolves_alias_and_unions_states():
    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "runtime_event_aliases": {"legacy_desync_event": "CROSS_VENUE_DESYNC"},
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC"],
                "states": ["FAMILY_STATE_A"],
                "runtime_templates": ["desync_repair", "convergence"],
            }
        },
    }
    canonical_registry = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC"],
            }
        }
    }
    state_registry = {
        "states": [
            {
                "state_id": "SOURCE_STATE_B",
                "source_event_type": "CROSS_VENUE_DESYNC",
            }
        ]
    }
    verb_lexicon = {
        "verbs": {
            "cross_signal_relative_value": ["desync_repair", "convergence"],
        }
    }

    event_index = gct._build_event_ontology_index(taxonomy, state_registry)
    canonical_event_info = gct._build_canonical_event_info(canonical_registry)
    ctx = gct._event_ontology_context(
        "legacy_desync_event",
        taxonomy=taxonomy,
        canonical_registry=canonical_registry,
        event_index=event_index,
        canonical_event_info=canonical_event_info,
        verb_lexicon=verb_lexicon,
        strict=True,
    )

    assert ctx["canonical_event_type"] == "CROSS_VENUE_DESYNC"
    assert ctx["canonical_family"] == "INFORMATION_DESYNC"
    assert ctx["family_states"] == ["FAMILY_STATE_A"]
    assert ctx["source_states"] == ["SOURCE_STATE_B"]
    assert ctx["all_states"] == ["FAMILY_STATE_A", "SOURCE_STATE_B"]
    assert set(ctx["rule_templates"]) == {"desync_repair", "convergence"}
    assert ctx["unknown_templates"] == []


def test_event_ontology_context_filters_unknown_templates_non_strict():
    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC"],
                "runtime_templates": ["desync_repair", "not_a_real_template"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    event_index = gct._build_event_ontology_index(taxonomy, state_registry)
    canonical_event_info = gct._build_canonical_event_info(canonical_registry)
    ctx = gct._event_ontology_context(
        "CROSS_VENUE_DESYNC",
        taxonomy=taxonomy,
        canonical_registry=canonical_registry,
        event_index=event_index,
        canonical_event_info=canonical_event_info,
        verb_lexicon=verb_lexicon,
        strict=False,
    )

    assert ctx["rule_templates"] == ["desync_repair"]
    assert ctx["unknown_templates"] == ["not_a_real_template"]


def test_event_ontology_context_rejects_unknown_templates_strict():
    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC"],
                "runtime_templates": ["desync_repair", "not_a_real_template"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    event_index = gct._build_event_ontology_index(taxonomy, state_registry)
    canonical_event_info = gct._build_canonical_event_info(canonical_registry)
    with pytest.raises(ValueError, match="Unknown template verbs found"):
        gct._event_ontology_context(
            "CROSS_VENUE_DESYNC",
            taxonomy=taxonomy,
            canonical_registry=canonical_registry,
            event_index=event_index,
            canonical_event_info=canonical_event_info,
            verb_lexicon=verb_lexicon,
            strict=True,
        )


def test_event_ontology_context_prefers_canonical_family_and_flags_mismatch():
    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "POSITIONING_EXTREMES": {
                "events": ["CROSS_VENUE_DESYNC"],
                "runtime_templates": ["desync_repair"],
            }
        }
    }
    canonical_registry = {"families": {"INFORMATION_DESYNC": {"events": ["CROSS_VENUE_DESYNC"]}}}
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    event_index = gct._build_event_ontology_index(taxonomy, state_registry)
    canonical_event_info = gct._build_canonical_event_info(canonical_registry)
    ctx = gct._event_ontology_context(
        "CROSS_VENUE_DESYNC",
        taxonomy=taxonomy,
        canonical_registry=canonical_registry,
        event_index=event_index,
        canonical_event_info=canonical_event_info,
        verb_lexicon=verb_lexicon,
        strict=False,
    )
    assert ctx["canonical_family"] == "INFORMATION_DESYNC"
    assert ctx["family_mismatch"] is True

    with pytest.raises(ValueError, match="Canonical/taxonomy family mismatch"):
        gct._event_ontology_context(
            "CROSS_VENUE_DESYNC",
            taxonomy=taxonomy,
            canonical_registry=canonical_registry,
            event_index=event_index,
            canonical_event_info=canonical_event_info,
            verb_lexicon=verb_lexicon,
            strict=True,
        )


def test_generate_candidate_templates_strict_fails_on_unimplemented_nonplanned_ontology_event(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_IMPL_001",
                "concept_id": "C_IMPL",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""CROSS_VENUE_DESYNC""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 1,
                "assets": "*",
            }
        ]
    ).to_csv(backlog_path, index=False)
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC", "FUTURE_EVENT"],
                "runtime_templates": ["desync_repair"],
            }
        }
    }
    canonical_registry = {
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["CROSS_VENUE_DESYNC", "FUTURE_EVENT"],
            }
        }
    }
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
        "--ontology_strict",
        "1",
    ]

    with patch.object(gct, "_load_taxonomy", return_value=taxonomy):
        with patch.object(gct, "_load_canonical_event_registry", return_value=canonical_registry):
            with patch.object(gct, "_load_state_registry", return_value=state_registry):
                with patch.object(gct, "_load_template_verb_lexicon", return_value=verb_lexicon):
                    with patch.object(gct, "_implemented_registry_event_types", return_value={"CROSS_VENUE_DESYNC"}):
                        with patch.object(sys, "argv", test_args):
                            rc = gct.main()
    assert rc == 1
    assert not (atlas_dir / "candidate_templates.parquet").exists()


def test_generate_candidate_templates_planned_unimplemented_is_skipped_but_allowed(tmp_path):
    backlog_path = tmp_path / "backlog.csv"
    pd.DataFrame(
        [
            {
                "claim_id": "CL_PLAN_001",
                "concept_id": "C_PLAN",
                "operationalizable": "Y",
                "status": "unverified",
                "candidate_type": "event",
                "statement_summary": 'payload {""event_type"": ""FUTURE_EVENT""}',
                "next_artifact": "spec/events/{event_type}.yaml",
                "priority_score": 1,
                "assets": "*",
            }
        ]
    ).to_csv(backlog_path, index=False)
    atlas_dir = tmp_path / "atlas"
    atlas_dir.mkdir()

    import pipelines.research.generate_candidate_templates as gct

    taxonomy = {
        "planned_events": ["FUTURE_EVENT"],
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["FUTURE_EVENT"],
                "runtime_templates": ["desync_repair"],
            }
        },
    }
    canonical_registry = {
        "planned_events": ["FUTURE_EVENT"],
        "families": {
            "INFORMATION_DESYNC": {
                "events": ["FUTURE_EVENT"],
            }
        },
    }
    state_registry = {"states": []}
    verb_lexicon = {"verbs": {"cross_signal_relative_value": ["desync_repair"]}}

    test_args = [
        "generate_candidate_templates.py",
        "--backlog",
        str(backlog_path),
        "--atlas_dir",
        str(atlas_dir),
        "--ontology_strict",
        "0",
        "--implemented_only_events",
        "1",
        "--allow_planned_unimplemented",
        "1",
    ]

    with patch.object(gct, "_load_taxonomy", return_value=taxonomy):
        with patch.object(gct, "_load_canonical_event_registry", return_value=canonical_registry):
            with patch.object(gct, "_load_state_registry", return_value=state_registry):
                with patch.object(gct, "_load_template_verb_lexicon", return_value=verb_lexicon):
                    with patch.object(gct, "_implemented_registry_event_types", return_value={"CROSS_VENUE_DESYNC"}):
                        with patch.object(sys, "argv", test_args):
                            rc = gct.main()
    assert rc == 0
    templates = pd.read_parquet(atlas_dir / "candidate_templates.parquet")
    assert templates.empty
    linkage = json.loads((atlas_dir / "ontology_linkage.json").read_text(encoding="utf-8"))
    unresolved = linkage.get("unresolved", {})
    assert unresolved.get("planned_unimplemented_events") == ["FUTURE_EVENT"]
    assert int(linkage.get("counts", {}).get("event_claims_skipped_unimplemented", 0)) == 1
