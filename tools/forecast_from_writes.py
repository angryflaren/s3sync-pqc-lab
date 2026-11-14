#!/usr/bin/env python3
(
    """Replay policy choices and byte costs"""
    """ from measured candidate sizes; predict no timings."""
)

import argparse
import csv
import json
import math
from pathlib import Path


def member(mapping, name):
    """Return the value of the unique case-insensitive key in mapping.

    Match name against mapping keys without regard to case. Raise
    ValueError if no key matches or more than one key matches.
    """
    matches = [
        value
        for key, value in mapping.items()
        if key.lower() == name.lower()
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one field: {name}")
    return matches[0]


def head_bytes(version, chain, prefix):
    """Return the UTF-8 byte count of the serialized HEAD document.

    Use version, chain entries, and prefix to reproduce its structure.
    Placeholder hexadecimal strings preserve serialized field lengths.
    """
    # Hexadecimal string values do not change their serialized lengths.
    root = "0" * 96
    objects = [
        {
            "Key": prefix + "/objects/" + root + ".bin",
            "Root": root,
            "Bytes": size,
            "Version": index,
            "Kind": kind,
        }
        for index, kind, size in chain
    ]
    document = {
        "Format": 1,
        "Group": "0" * 32,
        "Version": version,
        "Root": root,
        "Objects": objects,
    }
    return len(
        json.dumps(document, indent=2, ensure_ascii=True).encode("utf-8")
    )


def transfer(tokens, size, capacity, refill, peak):
    """Return the token balance and model duration after a transfer.

    Transfer size bytes with the given tokens, capacity, refill rate,
    and peak rate. The model duration does not cause an actual delay.
    """
    seconds = max(size / peak, max(0.0, size - tokens) / refill)
    return (
        min(capacity, max(0.0, tokens + refill * seconds - size)),
        seconds,
    )


def replay(
    candidates,
    config,
    policy,
    idle,
    prefix,
    read_every,
    read_versions=None,
):
    """Return policy decisions, byte costs, and token model results.

    Replay candidates using config, policy, idle, and object prefix.
    Use read_versions when supplied; otherwise use read_every with
    initial and final reads. Raise ValueError for invalid settings.
    """
    network = member(config, "network")
    capacity = float(member(network, "capacityBytes"))
    refill = float(member(network, "refillBytesPerSecond"))
    peak = float(member(network, "peakBytesPerSecond"))
    minimum = float(member(config, "minimumThreshold"))
    maximum = float(member(config, "maximumThreshold"))
    limit = int(member(config, "maximumDeltas"))

    if not all(
        math.isfinite(x)
        for x in [capacity, refill, peak, minimum, maximum]
    ):
        raise ValueError("Non-finite model parameters")
    if not 0 < refill <= peak or capacity <= 0 or not 2 <= minimum <= maximum:
        raise ValueError("Invalid model parameters")

    tokens = capacity
    chain = []
    upload = download = full_count = read_count = 0
    thresholds, kinds, calculated_heads = [], [], []
    budget_tokens, model_seconds, max_count = [], 0.0, 0

    for version, full, delta in candidates:
        if version:
            tokens = min(capacity, tokens + refill * idle)

        threshold = (
            minimum + (maximum - minimum) * (1 - tokens / capacity)
            if policy == "adaptive"
            else float(policy[-1])
        )
        chain_size = sum(item[2] for item in chain)
        full_choice = (
            version == 0
            or delta >= full
            or chain_size + delta >= threshold * full
            or len(chain) - 1 >= limit
        )
        kind = "Full" if full_choice else "Delta"
        size = full if full_choice else delta
        if full_choice:
            chain = []
        chain.append((version, kind, size))

        head = head_bytes(version, chain, prefix)
        upload += size + head
        for body in [size, head]:
            tokens, duration = transfer(tokens, body, capacity, refill, peak)
            model_seconds += duration

        should_read = (
            version in read_versions
            if read_versions is not None
            else (
                version == 0
                or version % read_every == 0
                or version == len(candidates) - 1
            )
        )
        if should_read:
            download += sum(item[2] for item in chain) + head
            read_count += 1

        if version:
            thresholds.append(threshold)
            full_count += int(full_choice)
        kinds.append(kind)
        calculated_heads.append(head)
        budget_tokens.append(tokens)
        max_count = max(max_count, len(chain) - 1)

    return {
        "policy": policy,
        "idle_seconds": idle,
        "read_every": read_every,
        "full_updates": full_count,
        "read_count": read_count,
        "upload_bytes": upload,
        "download_bytes": download,
        "combined_bytes": upload + download,
        "maximum_deltas": max_count,
        "threshold_min": min(thresholds),
        "threshold_max": max(thresholds),
        "model_transfer_seconds_including_initial": model_seconds,
        "kinds": kinds,
        "head_bytes": calculated_heads,
        "tokens_after": budget_tokens,
    }


def main():
    """Validate a measured trace and write calculated scenarios to CSV.

    Read paths and scenario settings from the command line. Verify the
    recorded decisions, byte costs, and token balances before replaying
    scenarios. Create the output directory and a new output file.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("writes", type=Path)
    parser.add_argument(
        "--idle-seconds",
        nargs="+",
        type=float,
        default=[0.0, 8.0],
    )
    parser.add_argument("--read-every", nargs="+", type=int, default=[20, 240])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if any(not math.isfinite(x) or x < 0 for x in args.idle_seconds):
        parser.error("Idle times must be finite and nonnegative")
    if any(x <= 0 for x in args.read_every):
        parser.error("Read periods must be positive")

    path = args.writes.resolve(strict=True)
    series = path.parent.parent
    config = json.loads(
        (series / "configuration.json").read_text(encoding="utf-8-sig")
    )
    if member(member(config, "network"), "decisionBudgetMode") != "virtual":
        raise ValueError(
            "Only virtual-mode traces can be replayed by this tool"
        )

    with path.open(encoding="utf-8-sig", newline="") as stream:
        original = list(csv.DictReader(stream))
    candidates = [
        (
            int(x["version"]),
            int(x["full_candidate_bytes"]),
            int(x["delta_candidate_bytes"]),
        )
        for x in original
    ]
    if (
        len(candidates) < 2
        or [x[0] for x in candidates] != list(range(len(candidates)))
    ):
        raise ValueError(
            "Expected a complete sequential trace beginning with version 0"
        )

    identity = json.loads(
        (path.parent / "namespace.json").read_text(encoding="utf-8-sig")
    )
    prefix = member(identity, "objectPrefix")
    summary = json.loads(
        (path.parent / "summary.json").read_text(encoding="utf-8-sig")
    )
    original_policy = member(summary, "policy")
    original_idle = float(
        member(member(config, "network"), "idleSecondsPerUpdate")
    )
    with (path.parent / "reads.csv").open(
        encoding="utf-8-sig", newline=""
    ) as stream:
        original_reads = list(csv.DictReader(stream))
    read_versions = {int(row["version"]) for row in original_reads}

    baseline = replay(
        candidates,
        config,
        original_policy,
        original_idle,
        prefix,
        1,
        read_versions,
    )
    if baseline["kinds"] != [x["kind"] for x in original]:
        raise ValueError("Replay does not reproduce recorded decisions")
    if baseline["head_bytes"] != [int(x["head_bytes"]) for x in original]:
        raise ValueError(
            "HEAD serialization differs from this application version"
        )
    if any(
        not math.isclose(
            a,
            float(b["model_tokens_after_bytes"]),
            abs_tol=1e-5,
            rel_tol=1e-11,
        )
        for a, b in zip(baseline["tokens_after"], original)
    ):
        raise ValueError("Replay does not reproduce recorded budget balances")
    if baseline["upload_bytes"] != member(summary, "totalUploadBytes"):
        raise ValueError("Replay aggregate upload does not match summary")
    if baseline["download_bytes"] != sum(
        int(row["download_body_bytes"]) for row in original_reads
    ):
        raise ValueError(
            "Replay aggregate downloads do not match recorded reads"
        )

    print(
        "PASS: original decisions, HEAD sizes, budget balances, "
        "uploads and downloads reproduced."
    )
    print(
        f"Trace contains {len(candidates) - 1} updates; "
        "future versions are not extrapolated."
    )

    results = []
    for idle in args.idle_seconds:
        for period in args.read_every:
            group = [
                replay(candidates, config, name, idle, prefix, period)
                for name in ["fixed-2", "fixed-4", "adaptive"]
            ]
            for item in group:
                item["decisions_different_from_fixed2"] = sum(
                    a != b
                    for a, b in zip(item["kinds"], group[0]["kinds"])
                )
                item["decisions_different_from_fixed4"] = sum(
                    a != b
                    for a, b in zip(item["kinds"], group[1]["kinds"])
                )
                results.append(
                    {
                        key: value
                        for key, value in item.items()
                        if key not in ["kinds", "head_bytes", "tokens_after"]
                    }
                )
                print(
                    f"idle={idle:g}s read={period:3d} {item['policy']:8s} "
                    f"full={item['full_updates']:3d} "
                    f"total_GB={item['combined_bytes'] / 1e9:.4f} "
                    f"theta={item['threshold_min']:.3f}"
                    f"..{item['threshold_max']:.3f}"
                )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    print(
        "Calculated scenarios only. New S3 runs, timings and server "
        "measurements were not performed."
    )
    print(
        "All scenarios use the same object-prefix length, "
        "so policy names add no byte-size bias."
    )


if __name__ == "__main__":
    main()