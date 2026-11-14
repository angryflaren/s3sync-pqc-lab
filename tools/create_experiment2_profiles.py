#!/usr/bin/env python3
(
    """Create separate experiment 2 configurations"""
    """ without editing the source template."""
)

import argparse
import copy
import json
from pathlib import Path


def get_member(mapping, name):
    """Return the value of the unique case-insensitive key in mapping.

    Match name against mapping keys without regard to case. Raise
    ValueError if no key matches or more than one key matches.
    """
    matches = [key for key in mapping if key.lower() == name.lower()]
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one configuration field: {name}")
    return mapping[matches[0]]


def set_member(mapping, name, value):
    """Set a field in mapping, matching name without regard to case.

    Preserve the spelling of an existing key or use name for a new key.
    Raise ValueError if more than one key matches.
    """
    matches = [key for key in mapping if key.lower() == name.lower()]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous configuration field: {name}")
    mapping[matches[0] if matches else name] = value


def main():
    """Write pilot and full profiles for both experiment 2 idle modes.

    Read the template path from the command line. Create JSON profiles
    beside it. Raise FileExistsError if any target profile exists.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("template", type=Path)
    args = parser.parse_args()

    template = args.template.resolve(strict=True)
    original = json.loads(template.read_text(encoding="utf-8-sig"))
    network = get_member(original, "network")
    get_member(network, "capacityBytes")
    get_member(original, "datasetDirectory")

    outputs = []
    # At the original refill rate, 8 s gives 40 MiB of model refill.
    for label, idle in [("b00", 0.0), ("b08", 8.0)]:
        for pilot in [True, False]:
            mode = "pilot" if pilot else "full"
            value = copy.deepcopy(original)
            set_member(value, "repetitions", 1)
            set_member(
                value,
                "outputDirectory",
                f"results-experiment2-{label}-{mode}",
            )
            set_member(value, "updateLimit", 3 if pilot else None)
            if pilot:
                set_member(value, "groups", ["sqlite"])
                set_member(value, "readProfiles", ["read_light"])
            net = get_member(value, "network")
            set_member(net, "decisionBudgetMode", "virtual")
            set_member(net, "idleSecondsPerUpdate", idle)
            storage = get_member(value, "storage")
            if get_member(storage, "provider") != "s3":
                raise ValueError("The template must use storage.provider = s3")
            prefix = get_member(storage, "prefix").rstrip("/")
            set_member(storage, "prefix", prefix + f"/exp2-{label}-{mode}")
            path = template.parent / f"config.experiment2.{label}.{mode}.json"
            outputs.append((path, value))

    existing = [str(path) for path, _ in outputs if path.exists()]
    if existing:
        raise FileExistsError(
            "Existing profiles were retained: " + ", ".join(existing)
        )

    for path, value in outputs:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(
                value,
                stream,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )
            stream.write("\n")
        print(path)

    print(
        "b00: continuous model load; "
        "b08: model idle time before every update."
    )
    print(
        "The idle time is virtual. "
        "It does not add a real sleep or shape the network."
    )
    print(
        "Pilot: sqlite only, read_light, 3 updates, 3 policies, 1 repetition."
    )
    print(
        "Full configurations are prepared, "
        "but no experiment has been launched."
    )


if __name__ == "__main__":
    main()
