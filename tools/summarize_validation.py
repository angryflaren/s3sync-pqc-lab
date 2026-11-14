#!/usr/bin/env python3
"""Summarize a version window from completed experiment-2 runs without editing logs."""

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"CSV header is missing: {path}")
        rows = list(reader)
    if not rows or any(None in row or None in row.values() for row in rows):
        raise ValueError(f"CSV is empty or incomplete: {path}")
    return rows


def milliseconds(row, name):
    value = float(row[name])
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid timing value: {name}")
    return value


def find_series(directory):
    if (directory / "summaries.csv").is_file():
        return [directory]
    result = sorted(path.parent for path in directory.glob("experiment2-*/summaries.csv"))
    if not result:
        raise FileNotFoundError(f"No experiment-2 summaries found in {directory}")
    return result


def summarize(series, first, last, fingerprints):
    status = json.loads((series / "status.json").read_text(encoding="utf-8-sig"))
    if status.get("status") != "completed":
        raise ValueError(f"The series is not completed: {series}")
    configuration = json.loads((series / "configuration.json").read_text(encoding="utf-8-sig"))
    network = configuration["Network"]
    idle_seconds = float(network["IdleSecondsPerUpdate"])
    if not math.isfinite(idle_seconds) or idle_seconds < 0:
        raise ValueError(f"Invalid model idle time: {series}")
    summaries = read_csv(series / "summaries.csv")
    if status.get("trials") != len(summaries):
        raise ValueError(f"Trial count differs from the series status: {series}")

    records = []
    names = set()
    for summary in summaries:
        name = summary["run"]
        if Path(name).name != name or name in {".", ".."} or name in names:
            raise ValueError(f"Invalid or repeated trial name: {name}")
        names.add(name)
        trial = series / name
        writes = read_csv(trial / "writes.csv")
        reads = read_csv(trial / "reads.csv")
        group_reads = read_csv(trial / "group_reads.csv")
        updates = int(summary["updates"])
        final = updates if last is None else last
        if not 0 <= first <= final <= updates:
            raise ValueError(f"Window {first}..{final} exceeds versions 0..{updates}: {trial}")
        if [int(row["version"]) for row in writes] != list(range(updates + 1)):
            raise ValueError(f"The write history is incomplete: {trial}")

        upload = sum(int(row["packet_bytes"]) + int(row["head_bytes"]) for row in writes)
        download = sum(int(row["download_body_bytes"]) for row in reads)
        if upload != int(summary["total_upload_bytes"]) or download != int(summary["total_read_body_bytes"]):
            raise ValueError(f"Byte totals differ from the summary: {trial}")

        for row in writes:
            version = int(row["version"])
            if row["kind"] not in {"Full", "Delta"}:
                raise ValueError(f"Unknown packet kind: {trial}, version {version}")
            key = (summary["group"], version)
            fingerprint = (row["target_sha256"], int(row["full_candidate_bytes"]), int(row["delta_candidate_bytes"]))
            if key in fingerprints and fingerprints[key] != fingerprint:
                raise ValueError(f"Input versions or candidate sizes differ: {key}")
            fingerprints[key] = fingerprint

        read_versions = [int(row["version"]) for row in reads]
        if read_versions != sorted(set(read_versions)) or read_versions[0] != 0 or read_versions[-1] != updates:
            raise ValueError(f"The read history is incomplete or unordered: {trial}")
        if [int(row["version"]) for row in group_reads] != read_versions:
            raise ValueError(f"Group reads and container reads differ: {trial}")
        for read, group_read in zip(reads, group_reads):
            version = int(read["version"])
            write = writes[version]
            if read["target_sha256"] != write["target_sha256"]:
                raise ValueError(f"Restored SHA-256 differs: {trial}, version {version}")
            expected_bytes = int(write["chain_bytes"]) + int(write["head_bytes"])
            expected_gets = int(write["delta_count"]) + 2
            if int(read["download_body_bytes"]) != expected_bytes or int(read["get_count"]) != expected_gets:
                raise ValueError(f"Read accounting differs: {trial}, version {version}")
            if int(group_read["download_body_bytes"]) != expected_bytes or int(group_read["get_count"]) != expected_gets:
                raise ValueError(f"Group read accounting differs: {trial}, version {version}")
            group_time = milliseconds(group_read, "total_group_read_ms")
            expected_time = milliseconds(group_read, "restore_ms") + milliseconds(group_read, "extract_verify_ms")
            if not math.isclose(group_time, expected_time, abs_tol=1e-5):
                raise ValueError(f"Group read timings differ: {trial}, version {version}")

        selected_writes = [row for row in writes if first <= int(row["version"]) <= final]
        selected_reads = [row for row in group_reads if first <= int(row["version"]) <= final]
        upload = sum(int(row["packet_bytes"]) + int(row["head_bytes"]) for row in selected_writes)
        download = sum(int(row["download_body_bytes"]) for row in selected_reads)
        write_times = [milliseconds(row, "write_ms") for row in selected_writes if int(row["version"]) > 0]
        read_times = [milliseconds(row, "total_group_read_ms") for row in selected_reads]
        full_versions = [int(row["version"]) for row in selected_writes if int(row["version"]) > 0 and row["kind"] == "Full"]
        records.append({
            "series": series.name,
            "series_directory": str(series),
            "decision_budget_mode": network["DecisionBudgetMode"],
            "model_idle_seconds": idle_seconds,
            "group": summary["group"],
            "read_profile": summary["read_profile"],
            "policy": summary["policy"],
            "repetition": int(summary["repetition"]),
            "first_version": first,
            "last_version": final,
            "write_count": len(selected_writes),
            "full_updates": len(full_versions),
            "full_versions": " ".join(map(str, full_versions)),
            "upload_body_bytes": upload,
            "download_body_bytes": download,
            "combined_body_bytes": upload + download,
            "read_count": len(selected_reads),
            "get_count": sum(int(row["get_count"]) for row in selected_reads),
            "max_delta_count": max(int(row["delta_count"]) for row in selected_writes),
            "write_p50_ms": statistics.median(write_times) if write_times else "",
            "group_read_p50_ms": statistics.median(read_times) if read_times else "",
            "final_sha256": writes[final]["target_sha256"],
        })
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="+", type=Path)
    parser.add_argument("--first-version", type=int, default=61)
    parser.add_argument("--last-version", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.first_version < 0 or args.last_version is not None and args.last_version < args.first_version:
        parser.error("Expected 0 <= first-version <= last-version")

    records = []
    fingerprints = {}
    visited = set()
    for source in args.results:
        for series in find_series(source.resolve(strict=True)):
            if series in visited:
                raise ValueError(f"The series was specified twice: {series}")
            visited.add(series)
            records.extend(summarize(series, args.first_version, args.last_version, fingerprints))

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    print(f"Saved {len(records)} trial summaries: {output}")
    print("Only writes and reads inside the selected version window are counted.")
    print("A read includes its entire downloaded chain, including older objects.")


if __name__ == "__main__":
    main()
