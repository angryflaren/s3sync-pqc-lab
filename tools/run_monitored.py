#!/usr/bin/env python3
(
    """Run one existing C# experiment"""
    """ with scoped Linux resource monitoring."""
)

import argparse
import fcntl
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


def utc_now():
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def member(mapping, name, default=None):
    """Return the first case-insensitive match for name, or default.

    Look up name in mapping without requiring a unique matching key.
    """
    return next(
        (v for k, v in mapping.items() if k.lower() == name.lower()),
        default,
    )


def capture(command, destination, environment):
    """Run command and write combined output to destination.

    Use environment and a 15-second timeout. Return the exit code, or -1
    after logging an OS error or a timeout.
    """
    try:
        with destination.open("w", encoding="utf-8") as stream:
            result = subprocess.run(
                command,
                stdout=stream,
                stderr=subprocess.STDOUT,
                env=environment,
                timeout=15,
                check=False,
            )
        return result.returncode
    except (OSError, subprocess.TimeoutExpired) as error:
        with destination.open("a", encoding="utf-8") as stream:
            stream.write(f"\nCapture failed: {error}\n")
        return -1


def stop_group(process, first_signal=signal.SIGTERM, timeout=5):
    """Stop a running process group and wait for its leader to exit.

    Send first_signal initially. After timeout seconds, send SIGKILL.
    """
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, first_signal)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


class ExperimentRun:
    """Coordinate an experiment process, resource monitors, and logs.

    Use run() to launch the experiment and close() to release resources.
    """

    def __init__(self, directory, interval, project_root, environment):
        """Initialize run state and open the event log in directory."""
        self.directory = directory
        self.interval = interval
        self.project_root = project_root
        self.environment = environment
        self.monitors = []
        self.handles = []
        self.application = None
        self.requested_signal = 0
        self.signal_count = 0
        self.monitor_failure = None
        self.finished = threading.Event()
        self.event_lock = threading.Lock()
        self.events = (directory / "events.jsonl").open("w", encoding="utf-8")
        self.started_monotonic = time.monotonic()

    def event(self, kind, **fields):
        """Log and flush a timestamped event with extra fields."""
        row = {
            "utc": utc_now(),
            "elapsed_seconds": time.monotonic() - self.started_monotonic,
            "event": kind,
            **fields,
        }
        with self.event_lock:
            self.events.write(json.dumps(row, ensure_ascii=False) + "\n")
            self.events.flush()

    def request_stop(self, signum, _frame):
        """Record a signal request for the watchdog to handle."""
        # The signal handler only sets flags. The watchdog performs I/O
        # and signal forwarding.
        self.requested_signal = signum
        self.signal_count += 1

    def start_monitors(self):
        """Start monitors and raise RuntimeError on early exit."""
        interval = str(self.interval)
        commands = {
            "pidstat": [
                "pidstat",
                "-H",
                "-h",
                "-u",
                "-r",
                "-d",
                "-w",
                "-p",
                "ALL",
                "-G",
                "dotnet|S3Sync|weed|python",
                interval,
            ],
            "iostat": ["iostat", "-x", "-z", "-y", "-t", "-k", interval],
            "sar": [
                "sar",
                "-u",
                "ALL",
                "-r",
                "ALL",
                "-S",
                "-W",
                "-n",
                "DEV",
                "-o",
                str(self.directory / "sar.bin"),
                interval,
            ],
        }
        for name, command in commands.items():
            stream = (self.directory / f"{name}.txt").open(
                "w", encoding="utf-8"
            )
            self.handles.append(stream)
            process = subprocess.Popen(
                command,
                stdout=stream,
                stderr=subprocess.STDOUT,
                env=self.environment,
                start_new_session=True,
            )
            self.monitors.append((name, process))
            self.event(
                "monitor_started",
                name=name,
                pid=process.pid,
                command=command,
            )

        time.sleep(0.4)
        for name, process in self.monitors:
            if process.poll() is not None:
                raise RuntimeError(
                    f"{name} failed to start; "
                    f"inspect {self.directory / (name + '.txt')}"
                )

    def watch(self):
        """Watch monitor health and handle application cancellation.

        Send SIGINT on the first request. Send SIGKILL on another signal
        or when cancellation has taken more than 30 seconds.
        """
        interrupted_at = None
        while not self.finished.wait(0.2):
            for name, process in self.monitors:
                if (
                    process.poll() is not None
                    and self.monitor_failure is None
                ):
                    self.monitor_failure = name
                    self.event(
                        "monitor_failed",
                        name=name,
                        exit_code=process.returncode,
                    )

            application = self.application
            if application is None or application.poll() is not None:
                continue
            if self.requested_signal or self.monitor_failure:
                if interrupted_at is None:
                    self.event(
                        "cancellation_requested",
                        signal=self.requested_signal,
                        monitor_failure=self.monitor_failure,
                    )
                    try:
                        os.killpg(application.pid, signal.SIGINT)
                    except ProcessLookupError:
                        pass
                    interrupted_at = time.monotonic()
                elif (
                    self.signal_count > 1
                    or time.monotonic() - interrupted_at > 30
                ):
                    self.event("cancellation_timeout")
                    try:
                        os.killpg(application.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def run(self, command):
        """Run command with monitoring and return its exit code.

        Copy application output to the console and logs. Stop the
        watchdog and close the output pipe when processing finishes.
        """
        self.start_monitors()
        self.application = subprocess.Popen(
            command,
            cwd=self.project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=self.environment,
            start_new_session=True,
        )
        self.event(
            "application_started",
            pid=self.application.pid,
            command=command,
        )
        watcher = threading.Thread(
            target=self.watch,
            name="monitor-watchdog",
            daemon=True,
        )
        watcher.start()
        try:
            with (self.directory / "console.log").open(
                "w", encoding="utf-8"
            ) as console:
                for line in self.application.stdout:
                    console.write(line)
                    console.flush()
                    print(line, end="", flush=True)
                    self.event("console", text=line.rstrip("\n"))
            code = self.application.wait()
            self.event("application_exited", exit_code=code)
            if self.monitor_failure:
                return 1
            if self.requested_signal:
                return 128 + self.requested_signal
            return code if code >= 0 else 128 - code
        finally:
            self.finished.set()
            watcher.join()
            if self.application.stdout is not None:
                self.application.stdout.close()

    def close(self):
        """Stop application and monitor groups, then close log files."""
        if self.application is not None:
            stop_group(self.application, signal.SIGINT, timeout=30)
        for _, process in self.monitors:
            stop_group(process)
        for stream in self.handles:
            stream.close()
        self.events.close()


def write_source_hashes(project_root, destination):
    """Write SHA-256 hashes of selected C# project files to destination.

    Walk project_root, excluding output, data, and tool directories.
    """
    excluded = {"bin", "obj", "data", "monitoring", "exports", ".git", ".vs"}
    paths = []
    for directory, folders, files in os.walk(project_root):
        folders[:] = [
            x
            for x in folders
            if x not in excluded and not x.startswith("results")
        ]
        paths.extend(
            Path(directory) / name
            for name in files
            if name.endswith((".cs", ".csproj"))
        )

    with destination.open("w", encoding="utf-8") as stream:
        for path in sorted(paths):
            with path.open("rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            stream.write(
                f"{digest}  {path.relative_to(project_root).as_posix()}\n"
            )


def main():
    """Run the requested experiment and save monitoring artifacts.

    Read command-line paths and settings, check prerequisites, and hold
    the project lock. Return the experiment or monitoring exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment", choices=["run", "run2"])
    parser.add_argument("config", type=Path)
    parser.add_argument(
        "--project",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "S3Sync.csproj",
    )
    parser.add_argument("--interval", type=int, default=2)
    args = parser.parse_args()

    if not 1 <= args.interval <= 60:
        parser.error("--interval must be between 1 and 60 seconds")
    required = [
        name
        for name in ["dotnet", "pidstat", "iostat", "sar", "sadf", "time"]
        if not shutil.which(name)
    ]
    if required:
        raise RuntimeError("Required tools missing: " + ", ".join(required))

    project = args.project.resolve(strict=True)
    config_path = args.config.resolve(strict=True)
    configuration = json.loads(config_path.read_text(encoding="utf-8-sig"))
    storage = member(configuration, "storage", {})
    if member(storage, "provider") == "s3":
        endpoint = urlsplit(member(storage, "serviceUrl"))
        with socket.create_connection(
            (
                endpoint.hostname,
                endpoint.port or (443 if endpoint.scheme == "https" else 80),
            ),
            timeout=3,
        ):
            pass
    if shutil.disk_usage(project.parent).free < 10 * 1024**3:
        raise RuntimeError(
            "Less than 10 GiB free; SeaweedFS may refuse new writes"
        )

    os.umask(0o077)
    with (project.parent / ".s3sync-monitor.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                "Another monitored experiment is already running "
                "in this project"
            ) from None

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        directory = (
            project.parent
            / "monitoring"
            / f"{config_path.stem}-{stamp}-{uuid.uuid4().hex[:8]}"
        )
        directory.mkdir(parents=True)
        shutil.copyfile(config_path, directory / "configuration-input.json")
        for name in [
            "packages.lock.json",
            "global.json",
            "Directory.Build.props",
            "Directory.Build.targets",
            "Directory.Packages.props",
        ]:
            candidate = project.parent / name
            if candidate.is_file():
                shutil.copyfile(candidate, directory / name)

        environment = dict(os.environ)
        environment.update(
            {
                "LC_ALL": "C",
                "TZ": "UTC",
                "S_COLORS": "never",
                "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
                "DOTNET_NOLOGO": "1",
            }
        )
        start = {
            "started_utc": utc_now(),
            "experiment": args.experiment,
            "config": str(config_path),
            "project": str(project),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "sample_interval_seconds": args.interval,
            "free_disk_bytes_before": shutil.disk_usage(project.parent).free,
            "memory_note": (
                "GNU time reports client process-tree maximum RSS, "
                "not total VPS memory; sar records system memory."
            ),
            "budget_mode": member(
                member(configuration, "network", {}), "decisionBudgetMode"
            ),
        }
        (directory / "started.json").write_text(
            json.dumps(start, indent=2) + "\n"
        )
        write_source_hashes(project.parent, directory / "source-hashes.sha256")
        capture(
            ["dotnet", "--info"], directory / "dotnet-info.txt", environment
        )
        capture(
            [
                "dotnet",
                "package",
                "list",
                "--project",
                str(project),
                "--include-transitive",
                "--no-restore",
                "--format",
                "json",
            ],
            directory / "nuget-packages.json",
            environment,
        )
        capture(["uname", "-a"], directory / "kernel.txt", environment)
        capture(["lscpu"], directory / "cpu.txt", environment)
        capture(["free", "-b"], directory / "memory-before.txt", environment)
        capture(["sar", "-V"], directory / "sysstat-version.txt", environment)

        controller = ExperimentRun(
            directory, args.interval, project.parent, environment
        )
        old_handlers = {
            number: signal.signal(number, controller.request_stop)
            for number in [signal.SIGINT, signal.SIGTERM]
        }
        print("Monitoring: " + str(directory), flush=True)
        command = [
            shutil.which("time"),
            "-v",
            "-o",
            str(directory / "client-resources.txt"),
            "dotnet",
            "run",
            "--project",
            str(project),
            "-c",
            "Release",
            "--no-build",
            "--no-launch-profile",
            "--",
            args.experiment,
            str(config_path),
        ]

        code, error = 1, None
        try:
            code = controller.run(command)
        except Exception as failure:
            error = str(failure)
            print("Monitoring runner failed: " + error, file=sys.stderr)
        finally:
            controller.close()
            for number, handler in old_handlers.items():
                signal.signal(number, handler)

            sar_file = directory / "sar.bin"
            if sar_file.is_file() and sar_file.stat().st_size:
                export_code = capture(
                    [
                        "sadf",
                        "-d",
                        str(sar_file),
                        "--",
                        "-u",
                        "ALL",
                        "-r",
                        "ALL",
                        "-S",
                        "-W",
                        "-n",
                        "DEV",
                    ],
                    directory / "sar.csv",
                    environment,
                )
                if code == 0 and export_code != 0:
                    code, error = (
                        1,
                        "Could not export sar.bin to sar.csv; "
                        "inspect the saved monitoring files",
                    )
                    print(error, file=sys.stderr)

            capture(
                ["free", "-b"], directory / "memory-after.txt", environment
            )
            status = (
                "completed"
                if code == 0
                else ("cancelled" if code in [130, 143] else "failed")
            )
            result = {
                "finished_utc": utc_now(),
                "status": status,
                "exit_code": code,
                "error": error,
                "monitor_failure": controller.monitor_failure,
                "application_exit_code": (
                    controller.application.returncode
                    if controller.application
                    else None
                ),
                "free_disk_bytes_after": (
                    shutil.disk_usage(project.parent).free
                ),
            }
            (directory / "result.json").write_text(
                json.dumps(result, indent=2) + "\n"
            )
            print("Monitoring saved: " + str(directory), flush=True)
        return code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)