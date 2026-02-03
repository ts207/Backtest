from __future__ import annotations

from typing import Iterable


def join_url(base: str, *parts: Iterable[str]) -> str:
    """
    Join URL parts with forward slashes, avoiding os.path.join.
    """
    cleaned = [base.rstrip("/")]
    for part in parts:
        if part is None:
            continue
        text = str(part).strip("/")
        if text:
            cleaned.append(text)
    return "/".join(cleaned)
