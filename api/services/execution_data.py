import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _file_signature(path: str) -> Optional[Tuple[str, int, int]]:
    file_path = Path(path)
    if not file_path.exists():
        return None

    stat_result = file_path.stat()
    return (str(file_path.resolve()), stat_result.st_mtime_ns, stat_result.st_size)


@lru_cache(maxsize=8)
def _load_json_cached(resolved_path: str, modified_ns: int, size: int) -> Dict[str, Any]:
    with open(resolved_path, "r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def load_json_file(path: str) -> Dict[str, Any]:
    signature = _file_signature(path)
    if not signature:
        return {}

    resolved_path, modified_ns, size = signature
    try:
        return _load_json_cached(resolved_path, modified_ns, size)
    except Exception:
        return {}


def load_execution_output() -> Dict[str, Any]:
    return load_json_file(os.path.join("output", "execution_output.json"))


def load_execution_metadata() -> Dict[str, Any]:
    return load_json_file(os.path.join("output", "execution_metadata.json"))