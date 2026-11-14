#!/usr/bin/env python3
"""Replay 120 bundled Zstandard source changes without network access.

Requires Git and Python 3.12 or later. Set --work to a replay directory.
Set --root to the bundle with commits.json, initial.tar, and patches.
Use reset in an empty directory, apply step --number N in order, and use
verify to check the current tracked files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path

STEP_COUNT = 120
MIB = 1024 * 1024


def run_git(work: Path, *args: str) -> str:
    """Run Git in work and return its output, or report its error text."""
    command = ["git", "-C", str(work), "-c", "core.autocrlf=false", *args]
    try:
        result = subprocess.run(command, check=True, capture_output=True)
    except subprocess.CalledProcessError as error:
        detail = (
            (error.stderr or error.stdout)
            .decode("utf-8", errors="replace")
            .strip()
        )
        raise RuntimeError(
            f"Git command failed: {' '.join(args)}\n{detail}"
        ) from error
    return result.stdout.decode("utf-8").strip()


def check_hash(path: Path, expected: str) -> None:
    """Raise ValueError if a file's SHA-256 differs from the bundle hash."""
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(MIB), b""):
            checksum.update(block)
    if checksum.hexdigest() != expected:
        raise ValueError(f"SHA-256 mismatch: {path}")


def check_tree(work: Path, expected: str, step: int) -> str:
    """Check tracked files and their Git tree, then return the tree hash."""
    run_git(work, "diff", "--exit-code")
    tree = run_git(work, "write-tree")
    if tree != expected:
        raise ValueError(
            f"Git tree mismatch at step {step}: "
            f"expected {expected}, got {tree}"
        )
    return tree


def main() -> None:
    """Reset, advance, or verify the bundled source history."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent / "natural_zstd",
    )
    parser.add_argument("--work", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("reset", help="Extract the initial tracked files")
    step_parser = commands.add_parser("step", help="Apply the next patch")
    step_parser.add_argument("--number", type=int, required=True)
    commands.add_parser("verify", help="Verify the current tracked files")
    args = parser.parse_args()

    root = args.root.resolve()
    work = args.work.resolve()
    manifest = json.loads((root / "commits.json").read_text(encoding="utf-8"))
    state = work / ".git" / "s3sync_history_step"
    if args.command == "reset":
        if work.exists() and any(work.iterdir()):
            raise ValueError("Reset requires a new or empty working directory")
        archive_path = root / "initial.tar"
        check_hash(archive_path, manifest["initial_tar_sha256"])
        work.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path) as archive:
            archive.extractall(work, filter="data")
        run_git(work, "init", "--quiet")
        run_git(work, "add", "--all")
        step = 0
    else:
        step = int(state.read_text(encoding="utf-8"))
        if not 0 <= step <= STEP_COUNT:
            raise ValueError("Saved history step is out of range")
        if args.command == "step":
            if args.number != step + 1 or not 1 <= args.number <= STEP_COUNT:
                raise ValueError(f"Apply steps 1..{STEP_COUNT} in order")
            record = manifest["versions"][args.number]
            patch = root / record["patch"]
            check_hash(patch, record["patch_sha256"])
            check_tree(work, manifest["versions"][step]["tree"], step)
            run_git(work, "apply", "--index", "--binary", str(patch))
            step = args.number

    version = manifest["versions"][step]
    check_tree(work, version["tree"], step)
    temporary = state.with_name(state.name + ".tmp")
    try:
        temporary.write_text(str(step), encoding="utf-8")
        temporary.replace(state)
    finally:
        temporary.unlink(missing_ok=True)
    print(
        json.dumps(
            {
                "step": step,
                "source_commit": version["commit"],
                "verified_git_tree": version["tree"],
            }
        )
    )


if __name__ == "__main__":
    main()
