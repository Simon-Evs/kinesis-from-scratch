# Build a Kinesis ingestion lab from scratch

This is the walkthrough from our learning session, collected into one reusable
guide. You write the files yourself and run each small step before moving on.

The flow covered here is:

```text
Python producer → Kinesis Mock → Python consumer → JSON file
```

PySpark, marimo, polling loops and checkpoints come later. This first version
is deliberately small enough to understand line by line.

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
Section 11 adds polling in `consumer_03_poll.py`; that snapshot has now been
reviewed and successfully run by the learner.

## 11. Read repeatedly with a polling consumer

We are starting this exercise in small steps. Preparation has been reviewed:
`consumer_03_poll.py` copies the print-only consumer and adds `time` using
`import json, time`, which is valid Python. The learner has also written the
polling loop below. It has been reviewed: iterator setup is outside the loop,
and the iterator update and pause are inside `while` but outside `for`.
The learner confirmed that a run printed `order-001` and stayed waiting.
The learner also confirmed that `order-002` appeared after running the producer
in a second terminal, without restarting the consumer. The polling exercise
is complete.

**First step in VS Code:** copy `consumer_01_print.py` to a new file named
`consumer_03_poll.py`. Keep the earlier snapshots. Add `import time` alongside
`import json` at the top of the new file, then save it. Leave the rest of the
copy unchanged for now.

We start from the print-only consumer so we can focus on repeated reads before
combining them with file writing. The `time` module will let us pause between
requests using `time.sleep(1)`.

The current consumer asks Kinesis for records once and exits. A polling
consumer keeps asking: an empty response only means that request returned no
records; more records can arrive later. In the next small step, we will add
a loop and use each response's `NextShardIterator` for the following request.

**Add the loop in VS Code:** keep the iterator setup above
`iterator = response["ShardIterator"]` unchanged. Replace everything after
that assignment with:

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
previous position instead of starting over.

This exercise assumes our existing single open shard; handling closed shards
comes later.

Run in the Python container terminal (the prompt shows
`root@...:/workspace#`):

```bash
uv run python consumer_03_poll.py
```

With retained events in `orders`, expect those events to print, followed by
the program staying running while it waits for more. No new printed output
is expected during empty reads. Leave it running and report what you see;
the next step will send an event from a second terminal. You can stop the
consumer with Ctrl+C; a `KeyboardInterrupt` traceback is expected.

If your terminal is on the host, enter the container first with
`docker compose exec python bash` from the project directory. If the run
reports that the stream does not exist, the emulator may have restarted;
report that error before continuing.

**Test a new event while the consumer waits:** leave the
consumer running in its first terminal. In VS Code, change only the
`order_id` value in `producer.py` from `order-001` to `order-002` and save.
This makes the new event easy to recognise.

Open a second **host terminal** and enter the same Python container:

```bash
cd ~/code/kinesis-from-scratch
docker compose exec python bash
```

Then, in that second **container terminal**, run:

```bash
uv run python producer.py
```

Check the first terminal: the running consumer should print `order-002`
without restarting. Report what appears there. This confirms that polling
continues through empty responses and picks up a newly arriving record.

## 12. Prepare to save separate batches — current exercise

Next we will combine polling with file writing, saving each nonempty response
as a separate JSON lines file. A batch here means the records returned by one
`get_records` request. Separate files will keep later responses from overwriting
earlier ones, as the single-file `"w"` approach in section 10 would do.

**Preparation:** stop the polling consumer with Ctrl+C in its container
terminal. A `KeyboardInterrupt` traceback is expected. In VS Code, copy
`consumer_03_poll.py` to `consumer_04_batches.py` and add:

```python
from pathlib import Path
```

The learner created the copy and added `Path`; the file has been reviewed
and still contains the original polling loop. Keep all earlier snapshots.

**Directory setup (reviewed):** immediately after
`iterator = response["ShardIterator"]`, before `while True:`, add:

```python
output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)
```

`Path` represents the destination folder. `parents=True` creates `data` too
if it is missing, and `exist_ok=True` allows the folder to exist already.
This setup belongs outside the polling loop because it only needs to run
once when the program starts.

The learner added these lines in the correct position; directory setup has
been reviewed. The completed version was run later, as recorded below.

**Nonempty-batch condition (reviewed):** inside `while True`, replace the existing
`for` block with:

```python
    records = response["Records"]

    if records:
        for record in records:
            event = json.loads(record["Data"].decode("utf-8"))
            print(event)
```

Keep the iterator update and `time.sleep(1)` at their existing indentation,
inside `while` but outside `if`. An empty list is false in an `if` condition,
so this block runs only when the response contains records. We will put file
creation inside this condition too, to avoid creating files for empty reads.
For now the consumer still only prints events.

The learner's condition and indentation have been reviewed and are correct,
including iterator advancement and sleep outside `if`.

**Filename setup (reviewed):** add this import at the top:

```python
from uuid import uuid4
```

Inside `if records:`, before the `for` loop, add:

```python
        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"
```

`uuid4().hex` generates a random identifier, giving each batch a new filename
with negligible collision risk, including across program restarts. The `f`
string inserts the identifier into the name, and `/` joins that name to the
`Path` for the output directory. This line only constructs a path; it does
not create a file yet. It belongs outside `for` so all records from one
response share one batch file.

Initial review found the `uuid4` import missing. The learner added it, and
the correction has been reviewed. Filename construction and its placement
are now correct.

**File-writing block (reviewed):** keep `batch_path` where it is. Replace the `for`
block below it with:

```python
        with batch_path.open("w", encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                output.write(json.dumps(event) + "\n")
                print(event)
```

The `with` block opens one file for the batch and closes it after the `for`
loop finishes. Each event is saved as one JSON line. The filename is generated
once per nonempty response, so its records share a file and later batches get
new files. Keep iterator advancement and sleep outside `if`, as before.

The learner's file-writing block has been reviewed: the file opens once per
nonempty response, the record loop writes JSON lines inside `with`, and
iterator advancement and sleep remain outside `if`. The learner ran this
snapshot and confirmed that both `order-001` and `order-002` printed. The
agent inspected `data/batches/orders-88dd6f9a1a164c238a17f03198bd6720.jsonl`
and parsed both lines successfully: it contains those two events.

**Initial run — Python container terminal:** run:

```bash
uv run python consumer_04_batches.py
```

Leave it running. Expect retained events to print. In VS Code, expand
`data/batches` and open the generated `.jsonl` file(s). Confirm that the
events appear as one JSON object per line, then report what you see. One
file can contain several events because a batch is one nonempty response,
not one event. Testing a later batch will follow after this first check.

**Test a later batch:** leave `consumer_04_batches.py`
running. In VS Code, change the order ID in `producer.py` to `order-003`
and save. In the second Python container terminal, run:

```bash
uv run python producer.py
```

Expect `order-003` to print in the consumer terminal and a second `.jsonl`
file to appear in `data/batches`. Open it in VS Code: it should contain the
new event, while the earlier file should still contain orders 001 and 002.
Report the result before moving on. This later-batch test is supplied but
not yet confirmed.

The learner reported seeing another order 002. Inspection found that the
saved `producer.py` still contains `"order_id": "order-002"`; the batch
directory still contained only the original file at inspection time.
Current correction: change that value to `"order-003"` in VS Code, save
with Ctrl+S, then rerun `uv run python producer.py` in the second container
terminal while the batch consumer remains running. A new producer run sends
a new record even if its order ID matches an earlier record; the producer
does not increment the ID automatically.

The learner subsequently reported that order 003 was there and the work was
done. Inspection confirmed the producer now uses `order-003`, and the original
batch file remains intact. However, the workspace still shows only that first
batch file, so a separate saved batch for order 003 has not been verified.

**Current small step — check saved files:** in the second Python container
terminal, run:

```bash
ls -l data/batches
```

Report the output before continuing. If there is only one file, next check
which consumer is running and where its output is being saved.

The learner's host-terminal listing confirmed only the original file.
`docker compose top` then confirmed that both `consumer_03_poll.py` and
`consumer_04_batches.py` are running. The print-only consumer does not save
files, but it does not prevent the batch consumer from reading the same events.
The producer is saved with order 003; whether it was run after that save
has not been established.

**Current step — host terminal** (`simon@pop-os`, in this project folder):

```bash
docker compose exec python uv run python producer.py
```

This runs the saved producer inside the container directly from the host.
Leave both consumers running for now. Wait a couple of seconds, then inspect
`data/batches` in VS Code for the new file and report whether it appears.
The command sends another record even if order 003 was sent previously.

**Diagnosis and current step:** the learner still saw one file. Inspection
inside the container confirmed that code and paths match the workspace.
Read-only process inspection found `consumer_04_batches.py` in state
`T (stopped)`: it is suspended, not actively polling. The print-only consumer
is still active. Reading retained Kinesis events confirmed order 003 reached
the stream (three copies are retained after repeated sends). No further
producer run is needed.

In the same **container terminal where the batch consumer was started**, run:

```bash
fg
```

This resumes the suspended foreground job. Ctrl+Z commonly causes this state;
the actual cause was not confirmed. Expect the batch consumer to read the
waiting events and create a new file. Report the output and whether the file
appears. If `fg` says there is no current job, report that rather than starting
another consumer. This resume step is supplied but not yet confirmed.

**Current step — recover with one terminal:** after the learner pressed Ctrl+C
and reported losing track of terminals, fresh Docker process inspection
confirmed neither consumer is running. The `fg` instruction is superseded.
In one **host terminal** (`simon@pop-os`, in this project folder), run:

```bash
docker compose exec python uv run python consumer_04_batches.py
```

Leave the other terminals alone for now. This starts the batch consumer
directly. It will replay retained orders, including duplicate order 003
records from repeated producer runs, and save them in a new batch file.
Await the learner's output before proceeding; no new producer run is needed.

**Recovery result:** the learner confirmed that the batch consumer printed
all six retained records. Workspace inspection verified a new file,
`data/batches/orders-029f3b8c4d0d417cbca263659415122c.jsonl`, containing six
valid JSON lines (001, 002, 002, 003, 003, 003). The original two-record file
is still intact. Saving to a new filename across restarts is verified; a
second nonempty response during the same uninterrupted run has not yet been
verified because the earlier batch process was suspended.

**Current small step:** press Ctrl+C once in the terminal running the batch
consumer to return to the host prompt. A `KeyboardInterrupt` traceback is
expected. Do not restart or send more events yet. This gives us one clear
terminal before continuing. Next guidance should explain replay and saved
progress, keeping any further live-batch test to one small step at a time.

This consumer still starts at `TRIM_HORIZON` on each run: separate
files preserve batches, but restarting can save the same retained events again.
Checkpoints come later.

## 13. Prepare a checkpoint consumer — current exercise

The learner confirmed pressing Ctrl+C after the successful batch run and
returning to the prompt. No consumer restart is requested at this point.

The batch consumer saves events, but does not save its reading position.
Every restart asks for `TRIM_HORIZON`, so it reads retained events again and
writes them to new files. A checkpoint will record the last successfully
saved record's sequence number for our single shard. On restart, we will
request an iterator with `AFTER_SEQUENCE_NUMBER` to continue after it.
`NextShardIterator` is a temporary token for continued polling; the sequence
number is the position we will store for resuming later.

Writing output must succeed before saving its checkpoint. A failure between
these operations can still cause replay, so this learning exercise will not
guarantee exactly-once output. Also, a checkpoint cannot remove duplicate
events already sent as separate Kinesis records.

**Preparation (reviewed):** copy `consumer_04_batches.py` to
`consumer_05_checkpoint.py`. Save it unchanged, preserving the earlier
snapshot. The learner created the copy; comparison confirmed it matches
consumer 04 exactly.

**Checkpoint path (reviewed):** in `consumer_05_checkpoint.py`, immediately
after `output_dir.mkdir(...)` and before `while True:`, add:

```python
checkpoint_path = Path("data/orders.checkpoint")
```

This names the file where we will store the last saved sequence number.
It is separate from the batch files and stores one position for this
exercise's single shard. Constructing a `Path` does not create the file.
The learner added this path in the correct position; it has been reviewed.

**Saving progress (reviewed):** after the entire `with` block, still inside
`if records:`, add:

```python
        last_sequence_number = records[-1]["SequenceNumber"]
        checkpoint_path.write_text(last_sequence_number, encoding="utf-8")
```

`records[-1]` selects the last record in this nonempty batch, and
`["SequenceNumber"]` retrieves its position within the shard. `write_text`
creates or overwrites the checkpoint file with that position and closes it.
These lines must align with `with`, outside both `with` and `for`, so the
checkpoint is written only after the whole batch file has successfully
closed. Keep iterator advancement and sleep outside `if`.

The learner's checkpoint-writing lines have been reviewed: they are outside
`with` and inside `if`, as required. `records[-1] ["SequenceNumber"]` is valid
Python; the space between the indexing operations is optional.

**Current small step:** move these existing lines together to immediately
after the imports, before `response = kinesis.get_shard_iterator(...)`:

```python
output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)
checkpoint_path = Path("data/orders.checkpoint")
```

Move them rather than copying, so they appear only once. The startup code
will need `checkpoint_path` defined before it chooses where to start reading.
Leave the rest unchanged. Save for review; do not run yet. Startup still uses
`TRIM_HORIZON`; reading the checkpoint is the next change after this move.

Review found the setup correctly added above iterator creation, but the
original three lines still remain below `iterator = response["ShardIterator"]`.
**Current correction:** delete only that second setup block, keeping the
one below the imports. `while True:` should then follow the iterator
assignment, separated by a blank line. Save for review; do not run yet.

The duplicate setup has now been removed and reviewed. Setup appears once,
before iterator creation.

**Current step — choose the starting position:** replace the initial
`response = kinesis.get_shard_iterator(...)` block with:

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
without indentation. If a checkpoint exists, read its sequence number and
request a position after that record. Otherwise start from the earliest
retained record. `strip()` removes surrounding whitespace from the saved
text. This assumes a valid checkpoint for the current stream and shard;
stream resets and invalid checkpoints will be handled separately.

Save for review; do not run yet. Startup selection is supplied but not yet
implemented or reviewed.

Startup review found two corrections needed in the `if` branch:
`encoding="uft-8"` must be `encoding="utf-8"`, and the iterator request must
use `AFTER_SEQUENCE_NUMBER` with `StartingSequenceNumber=last_sequence_number`.
The `else` branch, iterator extraction and checkpoint-writing placement are
correct. No checkpoint file was present at inspection time.
**Current step:** correct only the `if` branch to match the example above,
then save for review without running. As written, it would fail to read an
existing checkpoint due to the encoding typo and does not yet resume after it.

Follow-up review confirmed `utf-8` and `AFTER_SEQUENCE_NUMBER` are corrected.
The `if` branch still lacks `StartingSequenceNumber=last_sequence_number`.
**Current correction:** add that argument directly below `ShardIteratorType`
in the checkpoint branch, then save for review. This supplies the actual
saved position to accompany the chosen iterator type. Do not run yet.

Final code review confirmed that `StartingSequenceNumber` has been added
correctly. Startup now selects the saved position when a checkpoint exists,
and progress is written after the batch file closes. No checkpoint file was
present at review time. Both first-run and resume behaviour await live testing.

**Current step — first checkpoint run:** from the **host terminal** showing
`simon@pop-os`, in this project folder, run:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

Use this one terminal and leave the program running after events print.
With no checkpoint yet, it should read retained events, save a new batch,
and create `data/orders.checkpoint`. Report what prints before continuing.
Next inspect that checkpoint, then test a restart separately. Do not send
another event or start an earlier consumer for this step.

**First-run result:** the learner confirmed all six retained events printed.
The agent verified `data/orders.checkpoint` contains sequence number
`49679726421125354762108583299302287029925738877938565122` and the new batch
`orders-f81edefa7a7041599ea848d3fc59af05.jsonl` contains six valid JSON lines
(001, 002, 002, 003, 003, 003). There are three batch files at this point.
The first-run save is verified; resume behaviour is not yet verified.

**Current step — restart in the same terminal:** press Ctrl+C once to stop
consumer 05, then run the same command again from the returned host prompt:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

With no new events sent, expect it to stay running without printing old
orders and without creating another batch file. Leave it running and report
what happens. A `KeyboardInterrupt` traceback while stopping is expected.

**Resume result:** the learner confirmed the restarted consumer stays running
with no printed events. The agent verified the batch count remains three and
the checkpoint is unchanged. Restarting with no new records has passed: old
orders were not saved again.

**Current small step — prepare a new event using one terminal:** stop the
consumer with Ctrl+C once. In VS Code, change `producer.py`'s order ID to
`order-004` and save. Do not run anything yet; report completion. Next run
the producer and restart the checkpoint consumer sequentially in the same
host terminal, to verify new events are read after the saved position without
needing multiple active terminals.

The learner confirmed stopping the consumer and saving the producer change;
inspection verified `producer.py` now contains `order-004`.
**Current step — send once from the same host terminal:**

```bash
docker compose exec python uv run python producer.py
```

Expect a shard ID and sequence number, then the host prompt returns. Report
that result before restarting consumer 05. Sending order 004 has not yet
been confirmed; do not repeat the producer command unnecessarily.

The learner confirmed sending order 004: shard `shardId-000000000000`,
sequence number `49679726421125354762108583299303495955745473079002791938`.
**Current step — resume in the same host terminal:**

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

Expect only order 004 to print, then the consumer to wait. It should create
one new batch and advance the checkpoint to the sequence number above.
Leave it running and report the output. Do not run the producer again.

**New-event resume verified:** the learner confirmed that only order 004
printed. The agent inspected the fourth batch file,
`orders-26fa117fff4f4af683c2bd7c6d10ba94.jsonl`, and verified it contains only
order 004 as valid JSON. The checkpoint advanced to
`49679726421125354762108583299303495955745473079002791938`, matching the
producer's returned sequence number. The checkpoint exercise's initial save,
restart without replay, and reading a later event are all verified.

**Current small step:** press Ctrl+C once to stop consumer 05 and return to
the host prompt. Do not send another event or change code yet. Next explain
the remaining failure window: output saved before checkpoint update can be
replayed after a crash. Keep subsequent exercises small and learner-written.

The learner confirmed stopping consumer 05 with Ctrl+C after the successful
order 004 test.

**Current exercise — reason about a failure before adding code:** our consumer
first closes the batch file, then writes its checkpoint. Imagine the saved
checkpoint is at order 004. The consumer receives order 005 and successfully
saves its batch, but crashes before updating the checkpoint. On restart,
which order will it read again, and what happens to the already-saved batch?
Ask the learner to explain before introducing another change. No command or
code edit is requested for this step.

The learner correctly identified replay and duplication: order 005 would be
read again because the checkpoint still points to order 004. The existing
batch remains, and the replay creates a new batch filename with the same
event. This illustrates why saving output before progress can produce
duplicates after a failure.

**Current reasoning step:** imagine reversing those operations: advance the
checkpoint to order 005 first, then crash before saving order 005's batch.
On restart, would order 005 be read again, and would it be in the saved output?
Await the learner's answer before introducing more code. No commands needed.

The learner correctly answered that order 005 would be missing: the advanced
checkpoint makes restart skip it, even though its batch was never saved.
This explains the current ordering: save output first, then checkpoint.
The remaining crash window can produce duplicate output; the two separate
file writes do not guarantee exactly-once processing.

**Current learning checkpoint:** both failure scenarios have been answered
correctly. No consumer is expected to be running, and no code change or
command is currently requested. Next guidance should introduce identifying
replayed Kinesis records using shard ID and sequence number, distinguishing
replay of one record from separate producer sends with the same order ID.
Keep this conceptual before adding another learner-written snapshot.

## Where we reached in the session

- Kinesis Mock and the Python/uv container are running.
- boto3 is installed and the `orders` stream was created successfully.
- Producing an event and reading it back were confirmed working.
- The print-only and file-writing consumers are now saved separately as
  `consumer_01_print.py` and `consumer_02_save.py`.
- Both files were reviewed and checked with a simulated Kinesis response;
  printing and saving behaved as intended. The existing `data/orders.jsonl`
  was also checked and contains valid JSON. These checks do not run the real
  emulator or change your source files.
- The learner created `consumer_03_poll.py` and completed its polling loop.
  Review confirmed correct loop placement, use of `NextShardIterator` and
  the one-second pause. The learner confirmed the consumer printed the first
  order and stayed waiting, then received `order-002` without restarting.
  The polling exercise is complete. The initial `consumer_04_batches.py` copy
  and `Path` import have been reviewed, as has directory setup before the loop.
  The nonempty-batch condition and indentation have also been reviewed.
  Filename construction and the corrected `uuid4` import have been reviewed.
  The file-writing block has also been reviewed and is correct. Section 12
  records a successful learner run printing orders 001 and 002. The agent
  inspected the generated batch file and parsed both JSON lines successfully.
  Current supplied step: send order 003 while the consumer stays running and
  verify a second file appears while the first remains intact; result pending.
  The learner instead saw another order 002; inspection found the saved
  producer still uses order 002 and only the original batch file was present.
  Guidance supplied: change to order 003, save, then rerun the producer.
  The learner subsequently reported completion; inspection confirmed the
  producer now uses order 003 but only the original batch file is visible.
  Current supplied step is `ls -l data/batches` in the second container
  terminal to resolve the saved-file discrepancy before moving on.
  The host listing confirmed one file. Docker process inspection confirmed
  both consumers 03 and 04 are running. Current supplied step: run the saved
  producer from the host with `docker compose exec python uv run python producer.py`,
  then check for a new batch file. Do not assume sending after saving occurred.
  Subsequent diagnosis found the batch consumer suspended (`T` process state),
  while the print-only consumer remains active. Retained stream records
  include three copies of order 003, so production succeeded. Current supplied
  step: `fg` in the container terminal that launched the batch consumer;
  await confirmation of resumed consumption and a new file.
  Recovery update: after both consumers stopped, the learner restarted only
  consumer 04 from a host terminal. It printed all six retained events; the
  agent verified a new six-line JSON batch and the intact original file.
  Current step: Ctrl+C once to return to the host prompt, then explain replay
  and saved progress. Live arrival during this run remains untested.
  The learner confirmed stopping consumer 04 with Ctrl+C. Section 13 explains
  checkpoints and supplies an unchanged copy to `consumer_05_checkpoint.py`
  as the next small step, pending creation and review.
  The copy has since been reviewed and matches consumer 04. Current supplied
  step: define `checkpoint_path = Path("data/orders.checkpoint")` after
  directory creation and before the polling loop; implementation pending.
  The checkpoint path has now been reviewed in the correct position. Current
  supplied step: write the last record's sequence number after the batch's
  `with` block, inside `if records:`; implementation and review pending.
  Checkpoint writing has since been reviewed and is correctly placed after
  file closure. Current supplied step: move directory and checkpoint-path
  setup above initial iterator creation, pending learner completion and review.
  Review found it copied rather than moved: setup is duplicated. Current
  correction is to remove the lower three-line setup block, preserving the
  one below imports. Startup checkpoint reading remains to be added.
  The duplicate has since been removed and reviewed. Current supplied step:
  replace initial iterator creation with checkpoint-existence branches, using
  `AFTER_SEQUENCE_NUMBER` when saved progress exists and `TRIM_HORIZON`
  otherwise. Await implementation and review before running.
  The agent has not edited exercise code.

## Handover for the next guidance session

This section preserves the conversation context when opening a new agent
session with `~/code/kinesis-from-scratch` as its workspace.

- The learner completed a separate ready-made lab in
  `~/code/kinesis-python-lab`, including marimo, local PySpark batch processing,
  Parquet and Spark monitoring. They then chose to rebuild independently to
  understand the Python and Kinesis ingestion steps themselves.
- The active learning project is now `kinesis-from-scratch`. Keep the original
  completed lab intact as a reference.
- Teach one small step at a time. Explain the code and purpose in plain
  language, let the learner write it, then review when asked. Do not implement
  future exercises or build the whole pipeline for them unless requested.
- Update this README directly as instructions and progress change. Distinguish
  supplied instructions, reviewed code and user-confirmed successful runs.
- Preserve numbered consumer snapshots: `consumer_01_print.py` and
  `consumer_02_save.py` exist and have been reviewed. The saved JSON contains
  a valid order. No further changes were required in those scripts.
- `consumer_03_poll.py` is complete and reviewed. The learner confirmed it
  printed the retained order, stayed waiting, and received `order-002` sent
  from a second terminal without restarting. The learner also discussed that
  `response` is a dictionary and `NextShardIterator` is a Kinesis-supplied token
  used for the following read within the shard.
- Current exercise: section 12, `consumer_04_batches.py`. The polling copy,
  `Path` import, directory setup and nonempty-batch condition have been reviewed.
  Filename construction and the corrected `uuid4` import have been reviewed.
  The completed file-writing block has also been reviewed and is correct.
  The learner ran it and saw orders 001 and 002 print; the agent confirmed the
  first batch file contains both valid JSON lines. Current supplied step:
  change the producer to order 003 and run it in the second container terminal
  while the consumer stays running; verify a second batch file appears and
  the first remains intact. Await the learner's result. Restarting still replays
  retained events because there is no checkpoint. Do not implement the
  exercise for the learner.
- Latest troubleshooting supersedes the pending step above: learner reported
  order 003 appeared and work was done. The saved producer now uses order 003,
  but workspace inspection still shows only the original batch file containing
  orders 001 and 002. Current step: ask learner to run `ls -l data/batches` in
  the second container terminal and report output. Separate batch saving for
  order 003 has not been verified. The learner's host listing also confirmed
  only one file. Docker inspection found both consumers 03 and 04 running.
  Current step now supersedes the listing request: from the host, run
  `docker compose exec python uv run python producer.py`, then inspect for a
  new batch file. The producer is saved as order 003, but execution after saving
  was not established. Resolve this before advancing.
- Confirmed diagnosis superseding the earlier troubleshooting: container
  process inspection found consumer 04 suspended (`T`), while consumer 03 is
  active. Read-only Kinesis inspection confirmed order 003 was sent three
  times. Code and container output path are correct. Current step: `fg` in
  the same container terminal that launched consumer 04, then check for a new
  batch file. Do not send another event or start another consumer yet. If
  `fg` reports no current job, resolve terminal context next. Resume result
  is pending; the agent has not resumed processes or changed exercise code.
- Latest terminal recovery supersedes the `fg` instruction: the learner lost
  track of the terminals and pressed Ctrl+C on the waiting consumer. A fresh
  `docker compose top` confirmed that neither consumer is running now; the
  emulator and Python container remain running. Use one host terminal for
  the next step, running
  `docker compose exec python uv run python consumer_04_batches.py`.
  This starts only the batch consumer without entering another shell.
  It will replay retained events, including repeated sends of order 003,
  because there is no checkpoint. Await output before adding any further step.
- Latest recovery result supersedes the pending recovery above: consumer 04
  restarted successfully and printed all six retained events. The agent parsed
  the new batch file (001, 002, 002, 003, 003, 003) and confirmed the original
  two-event file is intact. Current step is Ctrl+C once to return to the host
  prompt; no new producer run. Keep terminal guidance simple. Saving a new
  file across restarts is verified; a later nonempty batch within the same
  uninterrupted run is not yet verified. Next explain replay and checkpoints
  before introducing further code, without implementing it for the learner.
- Current guidance now supersedes that stop instruction: the learner confirmed
  Ctrl+C is done. Section 13 explains replay, sequence-number checkpoints and
  saving output before progress. Current supplied step is an unchanged copy
  of consumer 04 to `consumer_05_checkpoint.py`, with no run yet. Await creation
  and review before guiding checkpoint code. Keep earlier snapshots intact.
- Checkpoint preparation update: consumer 05 exists and was compared with
  consumer 04; they match. Current step in section 13 is defining
  `checkpoint_path = Path("data/orders.checkpoint")` after directory setup,
  before the loop. Await review, then guide saving progress after successful
  file closure and finally reading it at startup. No checkpoint code has
  been run and no exercise source has been edited by the agent.
- Latest checkpoint step supersedes the path-definition instruction: the path
  has been added and reviewed. Current supplied step is
  `last_sequence_number = records[-1]["SequenceNumber"]` followed by
  `checkpoint_path.write_text(last_sequence_number, encoding="utf-8")`, aligned
  with `with` inside `if`, after the batch file closes. Await implementation
  and review. Do not run yet; startup checkpoint reading has not been added.
- Latest checkpoint review: saving the last sequence number is correctly
  placed after `with`, inside `if`. The space in `records[-1] ["SequenceNumber"]`
  is valid. Current supplied step supersedes the previous one: move the three
  directory/checkpoint-path setup lines immediately after imports, before
  initial iterator creation. Await review, then guide startup checkpoint
  reading and iterator selection. Do not run yet.
- Latest review found the setup now exists above iterator creation but is
  duplicated below `iterator = response["ShardIterator"]`. Current correction:
  remove only the lower three-line setup block and keep the top one. Await
  review before introducing startup checkpoint reading. No run yet.
- Latest checkpoint review supersedes duplicate-removal guidance: setup now
  appears once and is correctly placed. Current supplied step in section 13
  replaces initial iterator creation with `if checkpoint_path.exists()`:
  read saved progress and request `AFTER_SEQUENCE_NUMBER`, otherwise request
  `TRIM_HORIZON`. Keep iterator extraction outside both branches. Await review
  before a first run; checkpoint recovery for stream resets is not yet added.
- Latest startup review: the `if` branch has an encoding typo (`uft-8`) and
  still requests `TRIM_HORIZON`, without `StartingSequenceNumber`. Current
  correction is to use `utf-8`, `AFTER_SEQUENCE_NUMBER` and
  `StartingSequenceNumber=last_sequence_number` in that branch, as shown in
  section 13. The `else` branch and checkpoint-writing order are correct.
  Await correction and review; no checkpoint file was present and no run
  has been requested yet.
- Latest checkpoint correction review: encoding and iterator type are now
  correct, but `StartingSequenceNumber=last_sequence_number` is still missing
  from the `if` branch request. Current step is to add that one argument below
  `ShardIteratorType="AFTER_SEQUENCE_NUMBER"`. Await review before running.
- Latest checkpoint review supersedes the missing-argument correction:
  consumer 05 now includes `StartingSequenceNumber` and the full code has
  been reviewed. No checkpoint file was present. Current supplied step:
  from one host terminal run
  `docker compose exec python uv run python consumer_05_checkpoint.py`,
  leave it running and report output. Await live first-run result, then
  inspect the saved checkpoint and guide a separate restart test. No live
  checkpoint run or resume has yet been confirmed.
- Latest checkpoint test result supersedes that pending first run: the learner
  saw all six records, and the agent verified a new six-record JSON batch and
  a checkpoint containing sequence number
  `49679726421125354762108583299302287029925738877938565122`. There are three
  batch files. Current supplied step: Ctrl+C once, rerun consumer 05 in the
  same host terminal, and report whether it waits without replaying orders.
  No new producer run. Resume behaviour remains unverified until that result.
- Latest resume result: learner confirmed consumer 05 waits silently after
  restarting; agent verified there are still three batch files and the
  checkpoint is unchanged. No replay on restart with no new events is verified.
  Current supplied step: Ctrl+C once, change producer order ID to order 004
  in VS Code and save, with no run yet. Next guide producer execution followed
  by restarting consumer 05 in the same host terminal, one small step at a
  time. This avoids the earlier confusion from multiple active terminals.
- Latest preparation: learner confirmed stopping consumer 05 and changing the
  producer; inspection verified order 004 is saved. Current supplied command
  in the same host terminal is
  `docker compose exec python uv run python producer.py`, once. Await shard ID
  and sequence number output, then guide restarting consumer 05. New-event
  consumption after a checkpoint has not yet been tested.
- Latest send confirmed: order 004 was sent to `shardId-000000000000` with
  sequence number `49679726421125354762108583299303495955745473079002791938`.
  Current supplied step: run consumer 05 in the same host terminal; expect
  only order 004, a new batch and an advanced checkpoint. Await output and
  inspect files before marking new-event resume verified. No further send.
- Latest verified result supersedes the pending resume test: learner saw only
  order 004. Agent verified the fourth batch contains only order 004 and the
  checkpoint matches its sequence number
  `49679726421125354762108583299303495955745473079002791938`. Checkpoint
  initial save, no replay with no new events, and later-event resume have
  passed. Current supplied step: Ctrl+C once to return to the host prompt;
  no more sends or code changes yet. Next explain the output/checkpoint crash
  window and potential replay, keeping one-terminal guidance where possible.
- Current learning step supersedes the stop instruction: learner confirmed
  consumer 05 is stopped. Section 13 now poses a failure scenario: checkpoint
  at order 004, order 005 batch saved, crash before checkpoint update. Ask
  which order is replayed and what happens to the already-saved batch. Await
  the learner's reasoning; no code or command is currently requested.
- Latest reasoning result: learner correctly answered that a crash after
  batch save but before checkpoint update causes replay and duplicate output.
  Current question reverses the order: checkpoint advances to order 005,
  then crash before batch save. Ask whether restart reads order 005 and
  whether it is in the output. Await answer; no code or commands requested.
- Latest reasoning result supersedes that question: learner correctly answered
  that advancing the checkpoint before saving output can leave order 005
  missing. Both failure scenarios are understood. Explain output-first ordering
  as allowing possible duplicates instead of skipping unsaved output. Next
  introduce record identity (shard plus sequence number) versus business order
  ID before new code. Consumer 05 was stopped; no command is requested now.
- Use Docker Compose for container lifecycle on the host. Within the
  `python` container, use `uv run python ...`; this project uses a different
  environment from the original lab's preinstalled `/opt/venv` setup.
- The learner sometimes needs help distinguishing host and container terminals.
  Clearly label where commands run. The current container shell is root at
  `/workspace`; another host tab must enter with
  `docker compose exec python bash` to use that same container.
- Docker, uv, boto3, stream creation, sending an event and reading it back
  were confirmed working by the learner. Stream recreation raises an expected
  `ResourceInUseException` when `orders` already exists.
- Kinesis Mock is in-memory. If it has restarted, recreate `orders` and send
  an event before testing consumption; do not assume old records still exist.
- Keep focus on ingestion. PySpark/marimo/Parquet can follow later. Structured
  Streaming and watermarking are longer-term interests, currently deferred.
  Just was discussed and explicitly deferred too.

Suggested first message in the new session:

> Read README.md, including the handover section, and review the existing
> Python files. Continue guiding me through the next small learning step,
> updating README.md as we go. Let me write the exercise code unless I ask
> you to implement it.

Expected project layout after finishing this guide:

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
└── data/
    └── orders.jsonl
```

## Returning to the project

If the containers are still running, from your host terminal:

```bash
cd ~/code/kinesis-from-scratch
docker compose ps
docker compose exec python bash
```

You do not need to rerun `uv init`, reinstall boto3 or recreate an existing
stream to reopen a terminal.

To pause and resume the running emulator without losing its in-memory records,
use these host commands:

```bash
docker compose pause
docker compose unpause
```

To stop and remove the containers, run from the host:

```bash
docker compose down
```

This emulator has no persistence configured: stopping/restarting its process
loses streams and their records. Your Python files, uv project files and landed
JSON remain in the host folder. To start again:

```bash
docker compose up -d --build
docker compose exec python bash
```

Then recreate the stream, send an event and consume it, in that order.

## Next learning steps

1. Compare the two numbered consumers and explain the file-writing additions.
2. Create `consumer_03_poll.py` to add a polling loop using `NextShardIterator`
   and a delay between requests, keeping both earlier snapshots.
3. Save distinct batches instead of overwriting one file.
4. Save the last processed sequence number and resume after it.
5. Explore duplicates, failures, malformed records and multiple shards.
6. Add PySpark batch processing, marimo and Parquet after understanding ingestion.

Keep the earlier completed lab as a reference. The aim here is to understand
and write each piece yourself before moving on.
