from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict

def get_spec_hashes(project_root: Path) -> Dict[str, str]:
    hashes = {}
    spec_dir = project_root / "spec"
    
    # Files in spec root (e.g., gates.yaml)
    for f in spec_dir.glob("*.yaml"):
        if f.is_file():
            content = f.read_text(encoding="utf-8")
            hashes[f.name] = hashlib.sha256(content.encode("utf-8")).hexdigest()
            
    # Files in subdirectories
    for subdir in ["concepts", "features", "events", "tests"]:
        d = spec_dir / subdir
        if d.exists() and d.is_dir():
            for f in d.glob("*.yaml"):
                if f.is_file():
                    content = f.read_text(encoding="utf-8")
                    hashes[f"{subdir}/{f.name}"] = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return hashes
