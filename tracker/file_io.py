import json
import os
import tempfile
from pathlib import Path


def ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def write_private_json(path: Path, payload: object) -> None:
    ensure_private_dir(path.parent)

    with tempfile.NamedTemporaryFile(
        "w",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        json.dump(payload, tmp, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_path = Path(tmp.name)

    try:
        temp_path.chmod(0o600)
    except OSError:
        pass

    temp_path.replace(path)
    try:
        path.chmod(0o600)
    except OSError:
        pass
