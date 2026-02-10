# Repository Analysis Audit (2026-02-10)

## Scope
This audit covers repository structure, automated test health, dependency and vulnerability posture, and a lightweight static security sweep.

## Commands executed
- `python3 -m pytest -q`
- `python3 -m pip check`
- `python3 -m pip_audit`
- `rg -n "(TODO|FIXME|XXX|HACK)" project tests docs README.md`
- `rg -n "\\beval\\(|\\bexec\\(|subprocess\\.|shell=True|yaml\\.load\\(" project tests`
- `rg --files -g '*.py' | wc -l`
- `rg --files tests -g '*.py' | wc -l`
- `rg --files docs -g '*.md' | wc -l`
- `rg --files -g '*.yaml' -g '*.yml' | wc -l`

## Snapshot
- Python files: **111**
- Test files: **32**
- Docs markdown files: **11**
- YAML/YML config files: **4**

## Architecture and maintainability findings
- The repository keeps strong modular boundaries: orchestration (`project/pipelines`), simulation (`project/engine`), strategy contracts (`project/strategies`), and edge governance (`project/portfolio`, `edges/`).
- Run orchestration is centralized in `project/pipelines/run_all.py` with stage-level fail-fast behavior and per-stage logs/manifests, improving traceability.
- Strategy availability is no longer single-strategy concentrated; the registry exposes six concrete strategies.

## Test and runtime health
- `python3 -m pytest -q` result: **92 passed, 0 failed**.
- `python3 -m pip check` result: **No broken requirements found**.
- No TODO/FIXME/HACK markers were found in source/docs scope queried.

## Security and dependency posture
### Static pattern sweep
- No direct use of `eval(`, `exec(`, `yaml.load(`, or `shell=True` found.
- `subprocess.run(...)` usage exists in orchestrator and selected research/export paths, but observed invocations use argument lists rather than shell interpolation.

### Vulnerability scan (`pip_audit`)
`pip_audit` reported **4 known vulnerabilities in 3 packages**:
1. `pip==25.3` (`CVE-2026-1703`) → fix in `26.0`
2. `pyarrow==15.0.2` (`PYSEC-2024-161`) → fix in `17.0.0`
3. `requests==2.31.0` (`CVE-2024-35195`) → fix in `2.32.0`
4. `requests==2.31.0` (`CVE-2024-47081`) → fix in `2.32.4`

## Risk rating
- **Code correctness risk:** Low (green tests)
- **Operational risk:** Low to medium (depends on ingest/network variability)
- **Security/dependency risk:** Medium until vulnerable package pins are upgraded

## Recommended remediation plan
1. **Upgrade pinned dependencies (highest priority)**
   - `requests` to `>=2.32.4`
   - `pyarrow` to `>=17.0.0`
   - keep `pip` up to date in CI images/tooling
2. Re-run full test suite after dependency updates and monitor for schema or dtype drift.
3. Add a CI security gate (`pip_audit`) and fail on high-severity vulnerabilities.
4. Keep orchestrator subprocess usage list-based (current pattern is appropriate); avoid shell-based command assembly.

## Suggested acceptance criteria for follow-up hardening PR
- `python3 -m pytest -q` remains green.
- `python3 -m pip_audit` reports zero known vulnerabilities in pinned runtime dependencies.
- No regression in stage manifests/log paths or report generation flows.
