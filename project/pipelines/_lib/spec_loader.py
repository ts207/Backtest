from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml


DEFAULT_GLOBAL_DEFAULTS_ENV_VAR = "BACKTEST_GLOBAL_DEFAULTS_PATH"


def _safe_defaults(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        return {}
    return dict(defaults)


def load_global_defaults(
    *,
    project_root: Path,
    explicit_path: str | Path | None = None,
    env_var: str = DEFAULT_GLOBAL_DEFAULTS_ENV_VAR,
    required: bool = False,
) -> Dict[str, Any]:
    """Load global defaults with deterministic precedence.

    Precedence:
    1) explicit_path argument
    2) environment variable path (`env_var`)
    3) repository default path: <project_root>/../spec/global_defaults.yaml
    4) {} (unless required=True)
    """

    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))

    env_path = str(os.getenv(env_var, "")).strip()
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(Path(project_root).resolve().parent / "spec" / "global_defaults.yaml")

    for path in candidates:
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle)
            return _safe_defaults(payload)

    if required:
        raise FileNotFoundError(
            "Unable to locate global defaults spec. Checked: "
            + ", ".join(str(p) for p in candidates)
        )
    return {}
