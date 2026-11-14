#!/usr/bin/env python3
"""Build and replay versions of one SQLite test database.

Use --root PATH to select the data directory. The build command creates
initial.sqlite, changes.jsonl, and manifest.json. Reset and next prepare
working.sqlite; validate-history replays all updates and records hashes.
Only the Python standard library is required.

The public seed produces repeatable test data. Existing datasets use the
seed saved in their manifest when replaying updates.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import platform
import shutil
import sqlite3
import struct
import tempfile
from collections.abc import Iterator
from pathlib import Path

ROWS = 243_712
STEPS = 240
CHANGED_ROWS = 8192
PAYLOAD_SIZE = 1536
INSERT_BATCH_SIZE = 4096
CHECK_INTERVAL = 20
SEED = b"s3-single-file-workload-v1"
COMMANDS = ("build", "verify", "reset", "next", "validate-history")


@contextlib.contextmanager
def _temp_path(path: Path) -> Iterator[Path]:
    """Create a nearby temporary file and remove it on exit."""
    descriptor, name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    os.close(descriptor)
    temporary = Path(name)
    try:
        yield temporary
    finally:
        temporary.unlink(missing_ok=True)


def _write_text(path: Path, text: str) -> None:
    """Replace a UTF-8 text file after its new contents are written."""
    with _temp_path(path) as temporary:
        temporary.write_text(text, encoding="utf-8", newline="\n")
        temporary.replace(path)


def runtime_info() -> dict[str, str]:
    """Return the Python and SQLite versions used for this run."""
    return {
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
    }


def make_payload(step: int, row: int, seed: bytes = SEED) -> bytes:
    """Return repeatable payload bytes for one row and update step."""
    return hashlib.shake_256(seed + struct.pack("<II", step, row)).digest(
        PAYLOAD_SIZE
    )


def file_hash(path: Path) -> str:
    """Return a file's SHA-256 checksum."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build(root: Path) -> None:
    """Create the initial database, update history, and manifest.

    Refuse to overwrite an existing initial.sqlite file.
    """
    root.mkdir(parents=True, exist_ok=True)
    path = root / "initial.sqlite"
    if path.exists():
        raise ValueError("Refusing to overwrite initial.sqlite")
    with contextlib.closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA page_size=4096")
        database.execute("PRAGMA journal_mode=DELETE")
        database.execute(
            "CREATE TABLE items("
            "id INTEGER PRIMARY KEY,"
            "revision INTEGER NOT NULL,"
            "payload BLOB NOT NULL)"
        )
        for start in range(0, ROWS, INSERT_BATCH_SIZE):
            database.executemany(
                "INSERT INTO items VALUES(?,?,?)",
                (
                    (row, 0, make_payload(0, row))
                    for row in range(
                        start, min(start + INSERT_BATCH_SIZE, ROWS)
                    )
                ),
            )
        database.commit()
    history = [
        {
            "step": step,
            "action": "update_rows",
            "row_count": CHANGED_ROWS,
            "first_row": (step * 7919) % ROWS,
            "stride": 31,
            "logical_payload_bytes": CHANGED_ROWS * PAYLOAD_SIZE,
        }
        for step in range(1, STEPS + 1)
    ]
    _write_text(
        root / "changes.jsonl",
        "".join(json.dumps(operation) + "\n" for operation in history),
    )
    manifest = {
        "dataset": "single-file-v1",
        "file": "initial.sqlite",
        "bytes": path.stat().st_size,
        "sha256": file_hash(path),
        "rows": ROWS,
        "updates": STEPS,
        "data_seed": SEED.decode("utf-8"),
        "preparation_runtime": runtime_info(),
        "history_type": (
            "controlled synthetic SQL updates, not a real user trace"
        ),
    }
    _write_text(root / "manifest.json", json.dumps(manifest, indent=2))
    print(json.dumps(manifest), flush=True)


def reset(root: Path) -> None:
    """Restore the checked initial file and reset the working step."""
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    initial = root / "initial.sqlite"
    if (
        initial.stat().st_size != manifest["bytes"]
        or file_hash(initial) != manifest["sha256"]
    ):
        raise ValueError("Initial database size or SHA-256 mismatch")
    shutil.copyfile(initial, root / "working.sqlite")
    _write_text(root / "run_state.json", '{"step":0}')


def apply_step(root: Path) -> int:
    """Apply the next scheduled row update and return its step number.

    Use the seed recorded in manifest.json. Save the new step only after
    the database transaction has committed.
    """
    state = root / "run_state.json"
    current = json.loads(state.read_text(encoding="utf-8"))["step"]
    if not 0 <= current <= STEPS:
        raise ValueError("Saved history step is out of range")
    step = current + 1
    if step > STEPS:
        raise ValueError(f"All {STEPS} updates already applied")
    history = (root / "changes.jsonl").read_text(encoding="utf-8").splitlines()
    if len(history) < step:
        raise ValueError(f"Update history is missing step {step}")
    operation = json.loads(history[step - 1])
    if operation["step"] != step or operation["action"] != "update_rows":
        raise ValueError(f"Invalid update record for step {step}")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    seed = manifest["data_seed"].encode("utf-8")
    path = root / "working.sqlite"
    if not path.is_file():
        raise FileNotFoundError(path)
    with contextlib.closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA journal_mode=DELETE")
        database.executemany(
            "UPDATE items SET revision=?,payload=? WHERE id=?",
            (
                (step, make_payload(step, row, seed), row)
                for row in (
                    (operation["first_row"] + offset * operation["stride"])
                    % ROWS
                    for offset in range(CHANGED_ROWS)
                )
            ),
        )
        database.commit()
    _write_text(state, json.dumps({"step": step}))
    return step


def check_database(path: Path) -> None:
    """Check database integrity, row count, and payload sizes."""
    uri = path.resolve().as_uri() + "?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True)) as database:
        result = database.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise ValueError(f"SQLite integrity check failed: {path}")
        sizes = database.execute(
            "SELECT count(*),min(length(payload)),max(length(payload)) "
            "FROM items"
        ).fetchone()
        if sizes != (ROWS, PAYLOAD_SIZE, PAYLOAD_SIZE):
            raise ValueError(
                f"SQLite row count or payload size mismatch: {path}"
            )


def verify(root: Path) -> None:
    """Check the initial database against its manifest and format."""
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    path = root / "initial.sqlite"
    if path.stat().st_size != manifest["bytes"]:
        raise ValueError("Initial database size mismatch")
    if file_hash(path) != manifest["sha256"]:
        raise ValueError("Initial database SHA-256 mismatch")
    check_database(path)
    print(
        "Initial file size, SHA-256 and SQLite integrity verified", flush=True
    )


def validate_history(root: Path) -> None:
    """Replay all updates, checking the database every 20 steps.

    Save checkpoint sizes and hashes in history_check.json. These hashes
    describe the replay and are not compared with an external baseline.
    """
    reset(root)
    path = root / "working.sqlite"
    checkpoints = []
    for _ in range(STEPS):
        step = apply_step(root)
        if step % CHECK_INTERVAL == 0:
            check_database(path)
            checkpoints.append(
                {
                    "step": step,
                    "bytes": path.stat().st_size,
                    "sha256": file_hash(path),
                }
            )
            print("Verified history", step, "/", STEPS, flush=True)
    result = {
        "all_updates_replayed": STEPS,
        "format_check_interval": CHECK_INTERVAL,
        "runtime": runtime_info(),
        "checkpoints": checkpoints,
        "performance_results": None,
    }
    _write_text(root / "history_check.json", json.dumps(result, indent=2))


def main() -> None:
    """Read command-line options and run the selected action."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Data directory; defaults to this script's directory",
    )
    parser.add_argument("command", choices=COMMANDS)
    args = parser.parse_args()
    root = args.root.resolve()
    if args.command == "build":
        build(root)
    elif args.command == "verify":
        verify(root)
    elif args.command == "reset":
        reset(root)
    elif args.command == "next":
        print("Prepared version", apply_step(root))
    elif args.command == "validate-history":
        validate_history(root)


if __name__ == "__main__":
    main()
