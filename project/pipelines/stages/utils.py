from functools import lru_cache
from pathlib import Path
from typing import List, Optional

@lru_cache(maxsize=None)
def script_supports_log_path(script_path: Path) -> bool:
    try:
        return "--log_path" in script_path.read_text(encoding="utf-8")
    except OSError:
        return False

@lru_cache(maxsize=None)
def script_supports_flag(script_path: Path, flag: str) -> bool:
    try:
        return flag in script_path.read_text(encoding="utf-8")
    except OSError:
        return False

def flag_value(args: List[str], flag: str) -> Optional[str]:
    try:
        idx = args.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(args):
        return None
    return str(args[idx + 1]).strip()

def as_flag(value: int) -> str:
    return str(int(value))
