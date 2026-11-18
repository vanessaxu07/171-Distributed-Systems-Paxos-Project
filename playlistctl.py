import argparse
import fcntl
import json
import math
import os
from pathlib import Path
import random
import secrets
import socket
import subprocess
import sys
import time
from paxos import NODES, RUNTIME, USER_COMMANDS, atomic_write, encode

CONFIG = RUNTIME / "cluster.json"
SCRIPT = Path(__file__).resolve().with_name("paxos.py")


def call(config: dict, student: int, command: str, **fields) -> dict:
    request = {"type": "CONTROL", "token": config["token"], "command": command, **fields}
    delay = config["delay_max"] if command in USER_COMMANDS else 0
    if command in USER_COMMANDS:
        time.sleep(random.uniform(config["delay_min"], delay))
    with socket.create_connection(("127.0.0.1", config["base_port"] + student - 1), 2) as connection:
        connection.settimeout(2 + delay)
        connection.sendall(encode(request))
        with connection.makefile("rb") as reader:
            raw = reader.readline()
        if not raw:
            raise ConnectionError(f"student {student} closed the control connection")
        return json.loads(raw)


def running(student: int) -> bool:
    with (RUNTIME / f"node_{student}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False


def new_config() -> dict:
    base = int(os.environ.get("PAXOS_BASE_PORT", "17100"))
    low = float(os.environ.get("PAXOS_DELAY_MIN", "3"))
    high = float(os.environ.get("PAXOS_DELAY_MAX", "6"))
    if not 1024 <= base <= 65531:
        raise ValueError("base port must be between 1024 and 65531")
    if not math.isfinite(high) or not 0 <= low <= high:
        raise ValueError("expected 0 <= PAXOS_DELAY_MIN <= PAXOS_DELAY_MAX with finite delays")
    return {
        "token": secrets.token_hex(16), "base_port": base,
        "delay_min": low, "delay_max": high,
    }


def start() -> None:
    active = {student for student in NODES if running(student)}
    if active:
        config = read_config()
    else:
        config = new_config()
        atomic_write(CONFIG, json.dumps(config) + "\n")
    children = {}
    try:
        for student in NODES:
            if student in active:
                continue
            with (RUNTIME / f"student_{student}.out").open("ab") as output:
                children[student] = subprocess.Popen(
                    [sys.executable, str(SCRIPT), str(student)],
                    stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                    start_new_session=True,
                )
        deadline = time.monotonic() + 5
        for student in NODES:
            while True:
                try:
                    response = call(config, student, "status")
                    if response["ok"] and response["student"] == student:
                        break
                except OSError:
                    pass
                process = children.get(student)
                if (process and process.poll() is not None) or time.monotonic() >= deadline:
                    raise RuntimeError(f"student {student} did not start; see .paxos/student_{student}.out")
                time.sleep(0.05)
    except (Exception, KeyboardInterrupt):
        for process in children.values():
            if process.poll() is None:
                process.terminate()
        for process in children.values():
            process.wait(timeout=3)
        raise
    print(f"Students 1–5 running on ports {config['base_port']}–{config['base_port'] + 4}")


def read_config() -> dict:
    if not CONFIG.exists():
        raise RuntimeError("cluster is stopped; run make start")
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def stop() -> None:
    active = [student for student in NODES if running(student)]
    if active:
        config = read_config()
        failures = []
        for student in active:
            try:
                call(config, student, "stop")
            except OSError as error:
                if running(student):
                    failures.append(f"student {student}: {error}")
        if failures:
            raise RuntimeError("; ".join(failures))
        deadline = time.monotonic() + 5
        while any(running(student) for student in active):
            if time.monotonic() >= deadline:
                raise RuntimeError("a student did not stop within five seconds")
            time.sleep(0.05)
    print("All students stopped")


def clean() -> None:
    if any(running(student) for student in NODES):
        raise RuntimeError("run make killall before cleaning the cluster")
    for student in NODES:
        for path in (
            Path(f"playlist_{student}.txt"), Path(f"changes_{student}.log"),
            Path(f"messages_{student}.log"), RUNTIME / f"state_{student}.json",
            RUNTIME / f"student_{student}.out", RUNTIME / f"node_{student}.lock",
        ):
            path.unlink(missing_ok=True)
            path.with_suffix(path.suffix + ".tmp").unlink(missing_ok=True)
    CONFIG.unlink(missing_ok=True)
    CONFIG.with_suffix(".json.tmp").unlink(missing_ok=True)
    print("Removed cluster logs, playlist outputs, and saved state")


def main() -> int:
    parser = argparse.ArgumentParser(description="Send socket commands and manage the local five-student cluster.")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("start", "killall", "clean"):
        commands.add_parser(command)
    for command in ("add", "delete", "pause", "resume"):
        subparser = commands.add_parser(command)
        subparser.add_argument("student", type=int, choices=NODES)
        if command in ("add", "delete"):
            subparser.add_argument("song", type=int)
    args = parser.parse_args()
    try:
        if args.command in ("start", "killall", "clean"):
            RUNTIME.mkdir(mode=0o700, exist_ok=True)
            with (RUNTIME / "cluster.lock").open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                {"start": start, "killall": stop, "clean": clean}[args.command]()
            return 0
        config = read_config()
        if args.command in ("add", "delete"):
            response = call(config, args.student, "propose_song", op=args.command.upper(), song=args.song)
        else:
            response = call(config, args.student, args.command)
        print(response["message"], file=sys.stdout if response["ok"] else sys.stderr)
        return 0 if response["ok"] else 1
    except (OSError, ValueError, RuntimeError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
