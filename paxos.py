import asyncio
from datetime import datetime
import fcntl
import json
from pathlib import Path
import random
import sys
from time import monotonic

NODES = range(1, 6)
PREPARE_QUORUM = 2
VOTE_QUORUM = 3
ACCEPT_QUORUM = 4
VOTE_TIMEOUT = 21.0
USER_COMMANDS = {"propose_song", "pause", "resume"}
SYNC_BATCH = 100
RUNTIME = Path(".paxos")

def atomic_write(path: Path, contents: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(contents, encoding="utf-8")
    temporary.replace(path)


def encode(message: dict) -> bytes:
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


def preference(student: int, song: int) -> str | None:
    digit = str(song)[student - 1]
    if digit in "12":
        return "ADD"
    if digit in "34":
        return "DELETE"
    return None


class Student:
    def __init__(self, student: int, config: dict, message_log):
        self.id = student
        self.config = config
        self.message_log = message_log
        self.state_path = RUNTIME / f"state_{student}.json"
        self.decisions = []
        self.playlist = set()
        self.promised = 0
        self.accepted = None
        self.buffered = {}
        self.highest_seen = {}
        self.paused = False
        self.epoch = 0
        self.stopping = asyncio.Event()
        self.outgoing = set()
        self.proposal = None
        self.attempt = None
        self.delay_min = config["delay_min"]
        self.delay_max = config["delay_max"]
        self.round_trip = 2 * self.delay_max + max(0.3, self.delay_max / 2)
        self.recovery_delay = max(self.round_trip + 0.5, 3 * self.delay_max + 1)
        self.recover_at = float("inf")
        if self.state_path.exists():
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.promised = saved["promised"]
            self.accepted = saved["accepted"]
            self.decisions = saved["decisions"]
            for decision in self.decisions:
                self.apply(decision["op"])
        self.write_outputs()

    @property
    def slot(self) -> int:
        return len(self.decisions)

    def record(self, direction: str, endpoint, message: dict) -> None:
        timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
        print(timestamp, direction, endpoint, json.dumps(message, sort_keys=True),
              file=self.message_log, flush=True)

    def save(self) -> None:
        state = {
            "decisions": self.decisions,
            "promised": self.promised,
            "accepted": self.accepted,
        }
        atomic_write(self.state_path, json.dumps(state) + "\n")

    def write_outputs(self):
        playlist = [f"{song}\n" for song in sorted(self.playlist)]
        atomic_write(Path(f"playlist_{self.id}.txt"), "".join(playlist))
        changes = []
        for index, decision in enumerate(self.decisions):
            action, song = decision["op"]
            changes.append(f"{index} {action} {song}\n")
        atomic_write(Path(f"changes_{self.id}.log"), "".join(changes))

    def valid(self, op):
        action, song = op
        if action == "ADD":
            return song not in self.playlist
        return action == "DELETE" and song in self.playlist

    def apply(self, op):
        action, song = op
        if not self.valid(op):
            raise RuntimeError(f"invalid committed operation {op}")
        if action == "ADD":
            self.playlist.add(song)
        else:
            self.playlist.remove(song)

    def learn(self, start, entries):
        for index, decision in enumerate(entries, start):
            if index < self.slot:
                previous = self.decisions[index]
            else:
                previous = self.buffered.get(index)
            if previous and previous["op"] != decision["op"]:
                raise RuntimeError(f"conflicting decisions at slot {index}")
            if index >= self.slot:
                self.buffered[index] = decision
        if self.slot not in self.buffered:
            return
        while self.slot in self.buffered:
            decision = self.buffered.pop(self.slot)
            self.apply(decision["op"])
            self.highest_seen.pop(self.slot, None)
            self.decisions.append(decision)
        self.promised = 0
        self.accepted = None
        self.recover_at = float("inf")
        self.save()
        self.write_outputs()

    def send(self, recipient: int, kind: str, **fields) -> None:
        if self.paused:
            return
        message = {"type": kind, "from": self.id, **fields}
        if recipient == self.id:
            self.record("SEND", recipient, message)
            self.receive(message)
            return
        task = asyncio.create_task(self.deliver(recipient, message))
        self.outgoing.add(task)
        task.add_done_callback(self.outgoing.discard)

    def broadcast(self, kind: str, **fields) -> None:
        for student in NODES:
            self.send(student, kind, **fields)

    async def deliver(self, recipient: int, message: dict) -> None:
        writer = None
        try:
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
            async with asyncio.timeout(2):
                _, writer = await asyncio.open_connection(
                    "127.0.0.1", self.config["base_port"] + recipient - 1
                )
                writer.write(encode({**message, "token": self.config["token"]}))
                await writer.drain()
                self.record("SEND", recipient, message)
        except (OSError, TimeoutError) as error:
            self.record("UNDELIVERED", recipient, {**message, "error": str(error)})
        finally:
            if writer is not None:
                writer.close()

    def sync(self) -> None:
        for student in NODES:
            if student != self.id:
                self.send(student, "SYNC_REQUEST", start=self.slot)

    def send_suffix(self, student: int, start: int) -> None:
        entries = self.decisions[start:start + SYNC_BATCH]
        if entries:
            self.send(student, "SYNC_REPLY", start=start, entries=entries)

    def schedule_recovery(self) -> None:
        stagger = max(0.2, 2 * self.delay_max)
        self.recover_at = monotonic() + self.recovery_delay + random.uniform(0, stagger)

    def receive(self, message: dict) -> None:
        sender = message["from"]
        self.record("RECV", sender, message)
        kind = message["type"]
        if kind == "SYNC_REQUEST":
            self.send_suffix(sender, message["start"])
            return
        if kind == "SYNC_REPLY":
            self.learn(message["start"], message["entries"])
            return

        index = message["slot"]
        ballot = message["ballot"]
        if kind == "DECIDE":
            self.learn(index, [{"ballot": ballot, "op": message["op"]}])
            return
        if index < self.slot:
            if kind == "PREPARE":
                self.send_suffix(sender, index)
            return
        self.highest_seen[index] = max(ballot, self.highest_seen.get(index, 0))
        if kind == "VOTE_REQUEST":
            op = message["op"]
            my_preference = preference(self.id, op[1])
            yes = my_preference is None or my_preference == op[0]
            self.send(sender, "VOTE_REPLY", slot=index, ballot=ballot, op=op, yes=yes)
        if index > self.slot:
            if kind in {"PREPARE", "VOTE_REQUEST", "ACCEPT"}:
                self.send(sender, "SYNC_REQUEST", start=self.slot)
            return
        if kind == "PREPARE" and ballot > self.promised:
            self.promised = ballot
            self.save()
            if self.accepted:
                self.schedule_recovery()
            self.send(sender, "PROMISE", slot=index, ballot=ballot,
                      accepted=self.accepted)
        elif kind == "ACCEPT" and ballot >= self.promised:
            op = message["op"]
            if not self.valid(op):
                return
            if self.accepted and self.accepted["ballot"] == ballot:
                if self.accepted["op"] != op:
                    raise RuntimeError(f"conflicting accepts at slot {index}, ballot {ballot}")
            self.promised = ballot
            self.accepted = {"ballot": ballot, "op": op}
            self.save()
            self.schedule_recovery()
            self.send(sender, "ACCEPTED", slot=index, ballot=ballot, op=op)
        elif kind in ("PROMISE", "VOTE_REPLY", "ACCEPTED"):
            self.receive_reply(message)

    def receive_reply(self, message):
        attempt = self.attempt
        if attempt is None or not self.current(attempt):
            return
        if message["slot"] != attempt["slot"] or message["ballot"] != attempt["ballot"]:
            return
        if message["type"] != attempt["expected_reply"]:
            return
        if message["type"] != "PROMISE" and message["op"] != attempt["op"]:
            return
        # Using the sender as the key means a student only counts once.
        attempt["replies"][message["from"]] = message

    def current(self, attempt):
        return self.slot == attempt["slot"] and not self.paused and self.epoch == attempt["epoch"]

    async def wait_for_replies(self, attempt, quorum, timeout):
        deadline = monotonic() + timeout
        replies = attempt["replies"]
        while self.current(attempt):
            if attempt["expected_reply"] == "VOTE_REPLY":
                yes = 0
                no = 0
                for reply in replies.values():
                    if reply["yes"]:
                        yes += 1
                    else:
                        no += 1
                if yes >= quorum:
                    return "yes"
                if no >= quorum:
                    return "no"
            elif len(replies) >= quorum and self.id in replies:
                return "quorum"
            if monotonic() >= deadline:
                return "timeout"
            await asyncio.sleep(0.05)
        return "interrupted"

    async def run_attempt(self, index, ballot, requested):
        attempt = {
            "slot": index, "ballot": ballot, "epoch": self.epoch,
            "expected_reply": "PROMISE", "op": None, "replies": {},
        }
        self.attempt = attempt
        self.broadcast("PREPARE", slot=index, ballot=ballot)
        result = await self.wait_for_replies(attempt, PREPARE_QUORUM, self.round_trip)
        if result != "quorum":
            return "retry"
        previous = None
        for promise in attempt["replies"].values():
            entry = promise["accepted"]
            if entry and (previous is None or entry["ballot"] > previous["ballot"]):
                previous = entry
        if previous:
            op = previous["op"]
        elif requested is None:
            return "nothing to recover"
        else:
            op = requested
            attempt["op"] = op
            attempt["expected_reply"] = "VOTE_REPLY"
            attempt["replies"].clear()
            self.broadcast("VOTE_REQUEST", slot=index, ballot=ballot, op=op)
            result = await self.wait_for_replies(attempt, VOTE_QUORUM, VOTE_TIMEOUT)
            if result == "no":
                return "vote rejected"
            if result == "timeout":
                return "vote timed out"
            if result == "interrupted":
                return "retry"
        attempt["op"] = op
        attempt["expected_reply"] = "ACCEPTED"
        attempt["replies"].clear()
        self.broadcast("ACCEPT", slot=index, ballot=ballot, op=op)
        result = await self.wait_for_replies(attempt, ACCEPT_QUORUM, self.round_trip)
        if result != "quorum":
            return "retry"
        self.broadcast("DECIDE", slot=index, ballot=ballot, op=op)
        return "decided"

    def begin(self, requested):
        self.proposal = asyncio.create_task(self.run_proposal(self.slot, requested))

    async def run_proposal(self, index, requested):
        result = "slot filled"
        try:
            while self.slot == index:
                if self.paused:
                    await asyncio.sleep(0.1)
                    continue
                seen = max(self.promised, self.highest_seen.get(index, 0))
                round_number = seen // len(NODES) + 1
                ballot = round_number * len(NODES) + self.id
                result = await self.run_attempt(index, ballot, requested)
                self.attempt = None
                if result != "retry":
                    break
                if self.slot == index and not self.paused:
                    await asyncio.sleep(random.uniform(0.2, max(0.3, 2 * self.delay_max)))
            if self.slot > index:
                winner = self.decisions[index]["op"]
                if requested is None or winner == requested:
                    result = "decided"
                else:
                    result = "another operation won"
            self.record("EVENT", "proposal", {"slot": index, "op": requested, "result": result})
        finally:
            self.attempt = None
            self.proposal = None

    def control(self, message: dict) -> dict:
        command = message["command"]
        if command == "propose_song":
            return self.propose_song(message.get("song"), message.get("op"))
        if command == "status":
            return {"ok": True, "student": self.id, "paused": self.paused, "next_slot": self.slot}
        if command == "stop":
            return {"ok": True, "message": f"student {self.id} stopping"}
        if command == "pause":
            if not self.paused:
                self.paused = True
                self.epoch += 1
                for task in self.outgoing:
                    task.cancel()
            return {"ok": True, "message": f"student {self.id} paused"}
        if command == "resume":
            if self.paused:
                self.paused = False
                if self.accepted:
                    self.schedule_recovery()
                self.sync()
            return {"ok": True, "message": f"student {self.id} online"}
        return {"ok": False, "message": "unknown command"}

    def propose_song(self, song, action):
        if self.paused:
            return {"ok": False, "message": f"student {self.id} is paused"}
        if type(song) is not int or not 10000 <= song <= 99999 or action not in ("ADD", "DELETE"):
            return {"ok": False, "message": "expected ADD or DELETE and a five-digit song"}
        if preference(self.id, song) != action:
            return {"ok": False, "message": f"student {self.id} cannot propose {action} {song} by preference"}
        op = [action, song]
        if not self.valid(op):
            return {"ok": False, "message": f"{action} {song} is invalid for student {self.id}'s playlist"}
        if self.proposal is not None:
            return {"ok": False, "message": f"student {self.id} already has a proposal in progress"}
        self.begin(op)
        return {"ok": True, "message": f"student {self.id} started {action} {song} at slot {self.slot}"}

    async def handle_socket(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        epoch = self.epoch
        try:
            async with asyncio.timeout(2):
                raw = await reader.readline()
            message = json.loads(raw)
            if message.pop("token", None) != self.config["token"]:
                return
            if message["type"] == "CONTROL":
                self.record("RECV", "control", message)
                response = self.control(message)
                if message["command"] in USER_COMMANDS:
                    await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
                writer.write(encode(response))
                await writer.drain()
                self.record("SEND", "control", response)
                if message["command"] == "stop":
                    self.stopping.set()
            elif not self.paused and epoch == self.epoch and message["from"] in NODES:
                self.receive(message)
        except (OSError, TimeoutError, ValueError, KeyError, AttributeError, TypeError):
            pass
        finally:
            writer.close()

    async def maintenance(self) -> None:
        next_sync = monotonic() + random.uniform(0.1, 1)
        sync_interval = max(0.5, self.delay_max)
        while True:
            now = monotonic()
            if not self.paused:
                if now >= next_sync:
                    self.sync()
                    next_sync = now + random.uniform(sync_interval, 1.3 * sync_interval)
                if self.accepted and self.proposal is None and now >= self.recover_at:
                    self.schedule_recovery()
                    self.begin(None)
            await asyncio.sleep(0.1)

    async def run(self) -> None:
        if self.accepted:
            self.schedule_recovery()
        server = await asyncio.start_server(self.handle_socket, "127.0.0.1", self.config["base_port"] + self.id - 1)
        async with server:
            maintenance = asyncio.create_task(self.maintenance())
            try:
                await self.stopping.wait()
            finally:
                maintenance.cancel()


def main(student: int) -> None:
    config = json.loads((RUNTIME / "cluster.json").read_text(encoding="utf-8"))
    with (RUNTIME / f"node_{student}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with Path(f"messages_{student}.log").open("a", encoding="utf-8") as message_log:
            asyncio.run(Student(student, config, message_log).run())


if __name__ == "__main__":
    main(int(sys.argv[1]))
