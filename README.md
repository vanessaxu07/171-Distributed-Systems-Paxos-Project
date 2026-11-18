# The Paxos Playlist Predicament

Run from the project directory on macOS or Linux with Python 3.11 or newer and Make. The five students are separate processes that communicate over TCP on localhost. By default they listen on ports 17100–17104. Each peer message and each direction of a user command has an independent random delay of 3–6 seconds. Local self-messages count toward quorums immediately.

```sh
make start
make add NODE=1 SONG=12345
make delete NODE=3 SONG=12345
make pause NODE=5
make resume NODE=5
make killall
make clean
```

Each `add` or `delete` command reports whether the student started a proposal. Wait for the operation to appear in `changes_k.log` before issuing a command that depends on it. A successful isolated proposal can take tens of seconds to reach all five students. Vote rejection and the 21-second vote timeout appear as `EVENT` records in the proposer's message log. Pause and resume take effect when the target receives the command, and pause suspends peer traffic while the process retains its state.

| Per-student file | Contents |
| --- | --- |
| `playlist_k.txt` | Current songs in ascending order |
| `changes_k.log` | Committed changes in ascending slot order, as `index OP song` |
| `messages_k.log` | Timestamped sends, receives, delivery failures, and proposal outcomes |

`make killall` stops all five processes, including paused students. `make start` restores saved promises, acceptances, and decisions and starts any missing processes. Run `make clean` after `make killall` to remove the output files and saved state for a fresh demo. Set `PAXOS_BASE_PORT` before starting a stopped cluster to use a different range of five ports. For quicker local testing, set `PAXOS_DELAY_MIN` and `PAXOS_DELAY_MAX` in seconds before starting a stopped cluster.

## Demo checks

- **Vote and apply.** From a clean start, propose `ADD 12345` as student 1. Students 1, 2 and 5 vote yes. Wait for all five change logs to contain `0 ADD 12345`. Student 3 can then propose `DELETE 12345`, with yes votes from students 3, 4 and 5.
- **Reject or time out.** Student 1 can propose `ADD 13335`, which students 2, 3 and 4 vote against. On an empty playlist with student 5 paused, student 1’s proposal to add `12345` gets two yes and two no votes and times out. Both cases leave the log unchanged.
- **Compete for a slot.** Run `make add NODE=1 SONG=15555 & make add NODE=2 SONG=52555 & wait` when both songs are absent. Both students can start at the same slot. Compare the five change logs after convergence to see the one winning operation.
- **Continue through a pause.** Pause student 5, then have student 1 add `15555` when absent. The other four can commit it. Compare student 5's log before resuming it and after it catches up.

## Consensus and recovery

In `paxos.py`, read `run_attempt` for the prepare, vote, accept, and decide steps. The `receive` method handles each message, and `learn` updates the log and playlist. The `playlistctl.py` script sends user commands and starts and stops the processes.

Each student proposes at the first missing log slot. A ballot combines a growing round number with the student ID. After two promises, the proposer selects the highest-ballot accepted operation it learned about. When both promises report no acceptance, it needs three yes votes before sending `ACCEPT`. Four distinct acceptances are required before broadcasting `DECIDE`. Any set of two promises intersects every set of four acceptances, so a later proposer must preserve an operation that was already chosen. An interrupted attempt can retry its original slot with a higher ballot, and a committed slot ends all attempts for that slot.

Students persist promises and acceptances before replying and save learned decisions before updating the display files. A student with an unresolved accepted value can start a recovery ballot after a randomized wait. This lets the four remaining students finish an accepted operation if the original proposer goes offline before broadcasting its decision. Students also periodically ask peers for committed entries starting at their first missing slot. They apply learned entries in order and hold later decisions until preceding slots arrive.
