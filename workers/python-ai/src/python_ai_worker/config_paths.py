from __future__ import annotations

import os
from pathlib import Path


def resolve_config_dir(env_var: str, current_file: str, *segments: str) -> Path:
    override = os.getenv(env_var, "").strip()
    if override:
        return Path(override).expanduser().resolve()

    current_path = Path(current_file).resolve()
    bases: list[Path] = [current_path.parent, *current_path.parents, Path.cwd().resolve()]
    seen: set[Path] = set()
    for base in bases:
        if base in seen:
            continue
        seen.add(base)
        candidate = base / "config"
        for segment in segments:
            candidate /= segment
        if candidate.exists():
            return candidate

    fallback = Path.cwd().resolve() / "config"
    for segment in segments:
        fallback /= segment
    return fallback
