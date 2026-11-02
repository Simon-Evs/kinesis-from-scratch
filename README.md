# Build a Kinesis ingestion lab from scratch

This is the walkthrough from our learning session, collected into one reusable
guide. You write the files yourself and run each small step before moving on.

The flow covered here is:

```text
Python producer → Kinesis Mock → Python consumer → JSON file
```

The guide starts with one event, then adds polling, separate batch files and
checkpoints. PySpark and marimo come later. Each step builds on the previous
one so you can understand and write each piece yourself.

## 1. Create an independent project

Use a **host terminal**: the normal terminal showing `simon@pop-os`, rather
than a shell inside Docker. If you are inside a container, type `exit` first.

For a new project, run:

```bash
cd ~/code
mkdir kinesis-from-scratch
cd kinesis-from-scratch
code -n .
```

If the project already exists, just open it:

```bash
cd ~/code/kinesis-from-scratch
code -n .
```

Docker and Docker Compose must be installed and running. You do not need
Python, uv or AWS credentials installed on the host for this walkthrough.

## 2. Run Kinesis Mock

Create a plain text file named `compose.yml` in VS Code:

```yaml
services:
  kinesis:
    image: ghcr.io/etspaceman/kinesis-mock:0.6.2
    ports:
      - "127.0.0.1:4568:4568"
```

- `services` contains the containers Compose will run.
- `kinesis` is our service name.
- `image` selects Kinesis Mock and pins its version.
- The port mapping forwards host port 4568 to container port 4568. Binding
  to `127.0.0.1` keeps host access local to your computer.
- Port 4568 is the emulator's HTTP API, not a browser interface.

If the earlier completed lab is still using that port, stop it first:

```bash
docker compose -f ~/code/kinesis-python-lab/compose.yaml down
```

That stops the original lab; its in-memory Kinesis records are lost, while
its host JSON/Parquet files remain.

From the new project's host terminal, run:

```bash
docker compose up -d
docker compose ps
```

`-d` runs containers in the background. The service should show **Up**.
The emulator is running, but we have not created a stream yet.

## 3. Add Python and uv

Create a file named exactly `Dockerfile` (capital D, no extension):

```dockerfile
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /usr/local/bin/

WORKDIR /workspace

CMD ["sleep", "infinity"]
```

- `FROM` supplies Python 3.12 and a small Linux environment.
- `COPY` adds the uv executables from their official image.
- `WORKDIR` selects the directory where container commands start.
- `CMD` keeps the container running so we can open a shell inside it.

Watch the spelling: `3.12`, not `3:12`, and `bookworm`, not `bookwork`.

Update `compose.yml` to contain both services:

```yaml
services:
  kinesis:
    image: ghcr.io/etspaceman/kinesis-mock:0.6.2
    ports:
      - "127.0.0.1:4568:4568"

  python:
    build: .
    volumes:
      - .:/workspace
```

Both `kinesis` and `python` must have two spaces before their names, beneath
`services`. Otherwise Compose reports that the additional property `python`
is not allowed.

`build: .` builds the Dockerfile in this folder. The volume shares the project
folder with `/workspace`: files you edit in VS Code appear inside Docker,
and files written there appear on your host.

Check the configuration, then build and start the containers:

```bash
docker compose config --quiet
docker compose up -d --build
```

No output from `config --quiet` means the configuration is valid.

## 4. Open your Python terminal

In that same host terminal, run:

```bash
docker compose exec python bash
```

The prompt now looks like `root@...:/workspace#`. You are inside the running
Python container. Check the tools:

```bash
python --version
uv --version
```

Expect Python 3.12.x and uv 0.11.8. Use this **container terminal** for all
Python commands in the remaining steps.

Type `exit` to return to the host; that closes the shell without stopping the
containers. To re-enter, run `docker compose exec python bash` from the host.

For a second container terminal, open another host tab, change to the project
directory and run the same `docker compose exec python bash` command. It opens
another shell in the same container, sharing the same files and emulator.

## 5. Initialise the Python project

Inside the container, run these once:

```bash
uv init --bare
uv add boto3
```

- `pyproject.toml` describes the project and its dependencies.
- `uv.lock` records the resolved dependency versions.
- `.venv` contains the installed packages and project Python environment.

Because `/workspace` is shared, these also appear in VS Code. The container
currently runs as root, so files it creates have root ownership on a Linux
host; matching the host user is a separate setup improvement from the earlier lab.

Verify boto3:

```bash
uv run python -c "import boto3; print(boto3.__version__)"
```

Our session printed `1.43.121`. A fresh dependency resolution may select a
different version. `uv run` uses the project environment so Python can import
boto3. Once a lockfile exists, `uv sync --locked` reinstalls its recorded versions.

If uv names the project `workspace`, that is because it was initialised in
the container's `/workspace` directory; it does not affect this exercise.

## 6. Connect to the emulator

Create `client.py` in VS Code:

```python
import boto3

kinesis = boto3.client(
    "kinesis",
    endpoint_url="http://kinesis:4568",
    region_name="us-east-1",
    aws_access_key_id="local",
    aws_secret_access_key="local",
)
```

boto3 is the AWS Python SDK. This client points it at the local emulator using
the `kinesis` Compose service name. Inside the Python container, `localhost`
would refer to the Python container itself, not the Kinesis container.

The credentials are dummy values for the emulator. No real AWS account is used.

Keep client setup separate from stream creation: importing a Python module
executes its top-level statements. Importing a file that creates a stream would
try to create that stream each time a new Python process imports it.

## 7. Create a stream

Create `create_stream.py`:

```python
from client import kinesis

response = kinesis.create_stream(
    StreamName="orders",
    ShardCount=1,
)

print(response["ResponseMetadata"]["HTTPStatusCode"])
```

Run it inside the container:

```bash
uv run python create_stream.py
```

Expect **200**: the creation request succeeded. Creation can be asynchronous;
the next step requires the stream to become active. The emulator normally
finishes quickly. If needed, wait explicitly:

```bash
uv run python -c 'from client import kinesis; kinesis.get_waiter("stream_exists").wait(StreamName="orders"); print("Stream ready")'
```

Run stream creation once per fresh emulator. A second run against the same
stream raises `ResourceInUseException` because `orders` already exists; this
is expected and does not mean the first request failed.

## 8. Send one event

Create `producer.py`:

```python
import json

from client import kinesis

event = {
    "order_id": "order-001",
    "product": "book",
    "quantity": 1,
}

response = kinesis.put_record(
    StreamName="orders",
    PartitionKey=event["order_id"],
    Data=json.dumps(event).encode("utf-8"),
)

print(response["ShardId"])
print(response["SequenceNumber"])
```

Run:

```bash
uv run python producer.py
```

You should see a shard ID, such as `shardId-000000000000`, and a long sequence
number. The shard identifies where the event went; the sequence number records
its position within that shard.

The data conversion is:

```text
Python dictionary → json.dumps → JSON text → encode("utf-8") → bytes
```

The partition key influences which shard receives a record. With one shard,
all records go to the same shard. Running this example again sends another
record, even though its payload still contains the same `order_id`.

## 9. Read the event back

Create `consumer_01_print.py`. We keep numbered learning snapshots so you can
compare each new feature with the previous version:

```python
import json

from client import kinesis

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

response = kinesis.get_records(
    ShardIterator=iterator,
    Limit=10,
)

for record in response["Records"]:
    event = json.loads(record["Data"].decode("utf-8"))
    print(event)
```

Use the shard ID printed by your producer if it differs. Run:

```bash
uv run python consumer_01_print.py
```

A shard iterator is a temporary token describing where to start reading.
`TRIM_HORIZON` starts at the earliest retained record. The conversion reverses
what the producer did:

```text
bytes → decode("utf-8") → JSON text → json.loads → Python dictionary
```

This version makes one read request for at most ten records. An empty response
does not prove the stream is empty: a real consumer must keep polling using
`NextShardIterator`. We will add that in a later step.

Reading a record does not delete it. Every run currently starts from the
earliest retained record again; there is no saved progress yet.

## 10. Save the received events as JSON lines

Copy `consumer_01_print.py` to `consumer_02_save.py` in VS Code. Leave the first
version unchanged, then add file writing to the copy. The complete second
version is:

```python
import json
from pathlib import Path

from client import kinesis

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

response = kinesis.get_records(
    ShardIterator=iterator,
    Limit=10,
)

output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

with (output_dir / "orders.jsonl").open("w", encoding="utf-8") as output:
    for record in response["Records"]:
        event = json.loads(record["Data"].decode("utf-8"))
        output.write(json.dumps(event) + "\n")
        print(event)
```

Run and inspect the file:

```bash
uv run python consumer_02_save.py
cat data/orders.jsonl
```

The file also appears in your host project folder. `.jsonl` means one JSON
object per line, rather than one JSON array containing all the records.

`Path` handles filesystem paths. `mkdir(exist_ok=True)` creates the directory
without failing if it already exists. `with` closes the file when the block
finishes. Mode `"w"` overwrites the previous file on every run, including if
the read returns no records. It is a first file-writing exercise, not yet a
reliable ingestion consumer.

Compare the two files to see what changed: the `Path` import, directory
creation, the `with` block and `output.write`. Both versions read Kinesis in
the same way. These are separate learning snapshots; run one at a time.
Because each starts at `TRIM_HORIZON`, reading with one does not prevent the
other from reading the same retained records.

If following the earlier instructions with a file named `consumer.py`, rename
that print-only version to `consumer_01_print.py` before making the copy.
Section 11 adds polling in `consumer_03_poll.py`.

## 11. Read repeatedly with a polling consumer

The current consumer asks Kinesis for records once and exits. A polling
consumer keeps asking: an empty response only means that request returned no
records; more records can arrive later.

Copy `consumer_01_print.py` to `consumer_03_poll.py`, keeping the earlier
snapshots. Add `import time` alongside `import json`. We start from the
print-only version to focus on repeated reads before combining them with
file writing.

Keep the initial iterator setup unchanged. Replace everything after
`iterator = response["ShardIterator"]` with:

```python
while True:
    response = kinesis.get_records(
        ShardIterator=iterator,
        Limit=10,
    )

    for record in response["Records"]:
        event = json.loads(record["Data"].decode("utf-8"))
        print(event)

    iterator = response["NextShardIterator"]
    time.sleep(1)
```

`while True` repeats the indented block until you stop the program. Each
response provides `NextShardIterator`, which advances the next request's
reading position, including when `Records` is empty. The one-second pause
avoids a tight loop of requests.

The iterator update and pause belong inside `while`, but outside `for`:
they must happen even when there are no records. Keep the initial
`get_shard_iterator` call outside the loop so each request continues from the
previous position instead of starting over. This exercise assumes one open
shard; handling closed shards comes later.

Run in the **Python container terminal** (`root@...:/workspace#`):

```bash
uv run python consumer_03_poll.py
```

Expect retained events to print, followed by the program waiting for more.
No output is expected during empty reads. Leave it running. In VS Code,
change `producer.py`'s order ID to `order-002` and save.

Open a second **host terminal**, then enter the same Python container:

```bash
cd ~/code/kinesis-from-scratch
docker compose exec python bash
```

In that second **container terminal**, run:

```bash
uv run python producer.py
```

The first terminal should print `order-002` without restarting the consumer.
Stop the consumer with Ctrl+C when finished; a `KeyboardInterrupt` traceback
is expected.

**Completed and verified:** the code was reviewed. You confirmed that the
consumer printed the retained order, kept waiting, and received `order-002`
from the second terminal without restarting.

## 12. Save separate batches

Now combine polling with file writing, saving each nonempty response as a
separate JSON lines file. A batch means the records returned by one
`get_records` request. Separate files keep later responses from overwriting
earlier ones, as the single-file `"w"` approach in section 10 would do.

After stopping the polling consumer, copy `consumer_03_poll.py` to
`consumer_04_batches.py`. Keep the earlier snapshots. Add these imports:

```python
from pathlib import Path
from uuid import uuid4
```

Immediately after `iterator = response["ShardIterator"]`, before the loop, add:

```python
output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)
```

`Path` represents the destination folder. `parents=True` creates `data` too
if it is missing, and `exist_ok=True` allows the folder to exist already.
This setup runs once when the program starts.

Inside `while True`, replace the existing `for` block with:

```python
    records = response["Records"]

    if records:
        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"

        with batch_path.open("w", encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                output.write(json.dumps(event) + "\n")
                print(event)
```

Keep iterator advancement and `time.sleep(1)` inside `while`, outside `if`.
An empty list is false in an `if` condition, so empty reads create no files.

`uuid4().hex` generates a random identifier, giving each batch a new filename
with negligible collision risk, including across restarts. The `f` string
inserts the identifier into the name, and `/` joins it to the directory path.
Constructing a path does not create the file.

The filename is chosen once per nonempty response, outside `for`, so all its
records share one file. `with` closes that file after the record loop finishes.
Each event is written as one JSON line.

From the **host terminal** in this project folder, run:

```bash
docker compose exec python uv run python consumer_04_batches.py
```

Inspect the resulting files under `data/batches` in VS Code. One file can
contain several events. Restarting this consumer reads retained events again
because it still starts at `TRIM_HORIZON`; new filenames preserve the old
files but do not prevent duplicate output. Stop it with Ctrl+C before moving
on to checkpoints.

**Completed and reviewed:** all batch-writing code was reviewed. The initial
run printed orders 001 and 002, and the generated file contained both as valid
JSON lines. A later restart printed all six retained records
(001, 002, 002, 003, 003, 003), saved a new valid batch and preserved the original.

During testing, the batch consumer was found suspended while the print-only
consumer was still active. Order 003 had reached Kinesis three times through
repeated producer runs. Restarting only the batch consumer resolved the
confusion. A second nonempty batch during one uninterrupted run was not
verified; saving a separate file across restarts was verified. Prefer one host
terminal for subsequent exercises where possible.

## 13. Save a checkpoint and resume after it

Separate files preserve output, but the batch consumer does not remember its
reading position. A checkpoint records the last successfully saved record's
sequence number for our single shard. On restart, `AFTER_SEQUENCE_NUMBER`
lets the consumer continue after it.

`NextShardIterator` is a temporary token for continued polling. The sequence
number is the position we store for resuming in a later process.

Copy `consumer_04_batches.py` to `consumer_05_checkpoint.py`, preserving the
earlier snapshot. Move the existing directory setup directly below the
imports and add the checkpoint path there:

```python
output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)
checkpoint_path = Path("data/orders.checkpoint")
```

Keep this setup in one place, before initial iterator creation. Constructing
`checkpoint_path` names the file without creating it. It stores one position
for this exercise's single shard.

After the entire batch-writing `with` block, still inside `if records:`, add:

```python
        last_sequence_number = records[-1]["SequenceNumber"]
        checkpoint_path.write_text(last_sequence_number, encoding="utf-8")
```

`records[-1]` selects the last record in the nonempty batch, and
`["SequenceNumber"]` retrieves its position. `write_text` creates or overwrites
the checkpoint and closes it. These lines align with `with`, outside both
`with` and `for`: save progress only after the whole batch file closes
successfully. Keep iterator advancement and sleep outside `if`.

Replace the initial `get_shard_iterator` request with:

```python
if checkpoint_path.exists():
    last_sequence_number = checkpoint_path.read_text(encoding="utf-8").strip()
    response = kinesis.get_shard_iterator(
        StreamName="orders",
        ShardId="shardId-000000000000",
        ShardIteratorType="AFTER_SEQUENCE_NUMBER",
        StartingSequenceNumber=last_sequence_number,
    )
else:
    response = kinesis.get_shard_iterator(
        StreamName="orders",
        ShardId="shardId-000000000000",
        ShardIteratorType="TRIM_HORIZON",
    )
```

Keep `iterator = response["ShardIterator"]` below the whole `if`/`else`,
without indentation, followed by the polling loop. If a checkpoint exists,
read it and request a position after that record; otherwise start at the
earliest retained record. `strip()` removes surrounding whitespace.

This assumes a valid checkpoint for the current stream and shard. Recovery
from stream resets or invalid checkpoints remains a later exercise.

The checkpoint tests used one **host terminal** in the project folder:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

**Completed and verified:** the full code was reviewed, and all three tests
passed:

1. With no checkpoint, the consumer read all six retained records, saved a
   valid batch and created the checkpoint.
2. After Ctrl+C and a restart with no new sends, it waited silently. The batch
   count stayed at three and the checkpoint did not change.
3. After stopping the consumer, changing the producer to `order-004` and
   sending it once, restarting the consumer read only order 004. The fourth
   batch contained that event, and the checkpoint advanced to its sequence
   number: `49679726421125354762108583299303495955745473079002791938`.

You then confirmed stopping consumer 05 with Ctrl+C. These are the recorded
session results; they do not establish whether containers are running now.

## 14. Understand the failure window

The consumer saves the batch first and updates its checkpoint second. Those
are two separate file writes, so a failure between them can cause replay.

Imagine the checkpoint is at order 004. The consumer saves order 005's batch,
then crashes before updating the checkpoint. On restart, it reads order 005
again. The old batch remains, and a new randomly named batch saves the same
record again. You correctly identified this as replay and duplicate output.

Now reverse the operations: advance the checkpoint to order 005, then crash
before saving its batch. Restarting skips order 005, even though it was never
saved. You correctly identified that the output would be missing that event.

This explains the output-first ordering: the remaining failure window allows
possible duplicates rather than skipping unsaved output. The two writes do
not guarantee exactly-once processing. A checkpoint also cannot remove
separate records that the producer has already sent with the same order ID.

**Completed:** both failure scenarios were answered correctly. No crash test
or deduplication code has been implemented.

## Returning to the project

From your **host terminal**:

```bash
cd ~/code/kinesis-from-scratch
docker compose ps
```

If the containers are running, you can enter the Python container with
`docker compose exec python bash`, then use `uv run python ...`. Alternatively,
run a script directly from the host with
`docker compose exec python uv run python <filename>.py`.

You do not need to rerun `uv init`, reinstall boto3 or recreate an existing
stream to reopen a terminal. Inside the Python container, the prompt looks
like `root@...:/workspace#`; type `exit` to return to the host.

To pause and resume the emulator without losing its in-memory records, use
these **host commands**:

```bash
docker compose pause
docker compose unpause
```

To stop and remove the containers:

```bash
docker compose down
```

The emulator has no persistence configured: stopping or restarting its
process loses streams and their records. Python files, batch files and the
checkpoint remain in the host folder. Start the containers again with:

```bash
docker compose up -d --build
```

After an emulator reset, recreate the stream and send a new event before
consuming. The old checkpoint belongs to the previous stream instance;
consumer 05 does not yet handle that reset automatically. Check stream and
checkpoint state before resuming that consumer.

## Where we got to

Sections 1–14 describe the work completed so far. The five numbered consumer
snapshots have been written and reviewed. Polling, saving batches across
restarts, and checkpoint resume have been demonstrated, with the live-batch
test limitation recorded in section 12. Both failure scenarios in section 14
are understood.

The last confirmed session state was: consumer 05 stopped, `producer.py` set
to `order-004`, four saved batch files, and the checkpoint at order 004's
sequence number. Container and emulator state must be checked when returning.

The project now contains:

```text
kinesis-from-scratch/
├── compose.yml
├── Dockerfile
├── pyproject.toml
├── uv.lock
├── client.py
├── create_stream.py
├── producer.py
├── consumer_01_print.py
├── consumer_02_save.py
├── consumer_03_poll.py
├── consumer_04_batches.py
├── consumer_05_checkpoint.py
└── data/
    ├── orders.jsonl
    ├── orders.checkpoint
    └── batches/
        └── orders-<random-id>.jsonl
```

**Resume here:** the next small learning step is conceptual: distinguish a
replayed Kinesis record from separate producer sends carrying the same order
ID. Introduce record identity using shard ID and sequence number within this
stream before adding another consumer snapshot. This topic has not yet been
taught or checked. No new command or code edit is currently assigned.

## What remains to do

1. Understand record identity and how it differs from the business order ID.
2. Use that understanding to explore recognising replay and handling duplicate
   output, with another small learner-written snapshot when ready.
3. Complete a live test of separate nonempty batches during one uninterrupted
   consumer run.
4. Explore failure recovery, including invalid or stale checkpoints after a
   stream reset, and malformed records.
5. Extend beyond the single-shard assumptions, including multiple and closed
   shards.
6. Add PySpark batch processing, marimo and Parquet after understanding ingestion.
   Structured Streaming and watermarking remain longer-term interests. Just
   was discussed and deferred too.

## Guidance for continuing this walkthrough

Teach one small step at a time, in the order above. Explain the purpose and
code in plain language, let the learner write it, then review when asked.
Do not implement future exercises unless requested. Preserve the numbered
consumer snapshots and keep the original completed lab in
`~/code/kinesis-python-lab` intact as a reference.

Keep this README in learning order. Update the relevant exercise with its
confirmed result, then update “Where we got to” and “What remains to do”;
replace superseded status notes rather than accumulating competing current
instructions. Distinguish reviewed code, supplied exercises and confirmed runs.
Clearly label host and container commands, preferring one terminal where
possible.

For a new guidance session:

> Read README.md and review the existing Python files. Resume from “Where we
> got to,” one small learning step at a time, updating the README as we go.
> Let me write the exercise code unless I ask you to implement it.
