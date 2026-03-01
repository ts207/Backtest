import pytest
import yaml
from pathlib import Path
import sys
import os

# Add project to path
# This test file is in backtest/tests/specs/test_event_specs_contract.py
# project root is backtest/project
PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

# Force BACKTEST_DATA_ROOT to something safe
os.environ["BACKTEST_DATA_ROOT"] = "/tmp/backtest_dummy_data"

from events.phase2 import PHASE2_EVENT_CHAIN
from events.registry import EVENT_REGISTRY_SPECS
from strategy_dsl.policies import EVENT_POLICIES

REPO_ROOT = Path(__file__).resolve().parents[3]
SPEC_DIR = Path(__file__).resolve().parents[2] / "spec" / "events"

def get_active_event_specs():
    specs = []
    if not SPEC_DIR.exists():
        return specs
        
    for yaml_file in sorted(SPEC_DIR.glob("*.yaml")):
        if yaml_file.name == "canonical_event_registry.yaml":
            continue
        
        with open(yaml_file, "r") as f:
            try:
                data = yaml.safe_load(f)
                if not data:
                    continue
                if not data.get("active", True):
                    continue
                # Skip helper files or design docs
                if "event_type" not in data:
                    continue
                specs.append((yaml_file.name, data))
            except Exception:
                continue
    return specs

ACTIVE_SPECS = get_active_event_specs()

@pytest.mark.parametrize("yaml_filename, data", ACTIVE_SPECS)
def test_event_spec_io_contract(yaml_filename, data):
    """
    Lightweight lint for event specs to prevent format drift.
    Ensures all active events are properly wired into the pipeline and policies.
    """
    # 1. Check required fields
    required_fields = ["reports_dir", "events_file", "signal_column", "event_type"]
    for field in required_fields:
        assert field in data, f"Missing '{field}' in {yaml_filename}"
        assert data[field], f"Empty '{field}' in {yaml_filename}"

    # 2. Check events_file suffix
    events_file = data["events_file"]
    allowed_suffixes = [".parquet", ".csv"]
    suffix = Path(events_file).suffix.lower()
    assert suffix in allowed_suffixes, f"Disallowed suffix '{suffix}' in {yaml_filename}. Allowed: {allowed_suffixes}"
    
    # 3. Check phase2 coverage
    event_type = data["event_type"]
    phase2_types = {item[0] for item in PHASE2_EVENT_CHAIN}
    
    # Explicitly exempted types (e.g. specialized trading events or test types)
    exempt_from_phase2 = {
        "TYPE_A", "TYPE_B", 
        "BASIS_DISLOC", # Handled via custom logic or pending
        "COPULA_PAIRS_TRADING", # specialized
    }
    
    if event_type not in exempt_from_phase2:
        assert event_type in phase2_types, (
            f"Event type '{event_type}' from {yaml_filename} not found in "
            f"project/events/phase2.py (PHASE2_EVENT_CHAIN). "
            f"This will cause it to be skipped in Phase 2 pipeline runs."
        )

    # 4. Check policy coverage (or fallback)
    # strategy_dsl/policies.py uses EVENT_REGISTRY_SPECS as a fallback.
    # We ensure it's either explicitly defined or registered.
    explicit_policies = {k.upper() for k in EVENT_POLICIES.keys()}
    registry_types = set(EVENT_REGISTRY_SPECS.keys())
    
    has_policy = (event_type.upper() in explicit_policies) or (event_type.upper() in registry_types)
    assert has_policy, (
        f"Event type '{event_type}' has no explicit policy and is missing from "
        f"EVENT_REGISTRY_SPECS (registry.py). Strategy compilation will fail."
    )

def test_registry_completeness():
    """
    Verify that all YAML files that should be in the registry are actually there.
    """
    registry_types = set(EVENT_REGISTRY_SPECS.keys())
    for yaml_filename, data in ACTIVE_SPECS:
        event_type = data["event_type"]
        assert event_type in registry_types, f"Active event '{event_type}' from {yaml_filename} missing from registry.py EVENT_REGISTRY_SPECS"

if __name__ == "__main__":
    # Quick manual run
    for name, d in ACTIVE_SPECS:
        print(f"Checking {name}...")
        try:
            test_event_spec_io_contract(name, d)
        except Exception as e:
            print(f"  FAILED: {e}")
