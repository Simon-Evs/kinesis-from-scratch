# Build a local Kinesis ingestion lab: complete walkthrough

This guide takes a new learner from an empty folder to a Python consumer that
saves JSON batches, resumes from a checkpoint, processes the landed data with
PySpark, writes Parquet and explores the results in marimo. It includes complete
code, commands and checks for the full workflow, including stages beyond the
original learner's current progress.

Follow the steps in order. Type or copy the code into the named files, save
before running, and check the result before continuing. Keep every numbered
consumer so you can compare how each feature changes the program.

The finished flow is:

```text
producer → boto3 → Kinesis Mock → boto3 → consumer → raw JSON files
                                            │              │
                                       checkpoint       PySpark
                                                           │
                                                        Parquet
                                                           │
                                                  marimo exploration
```

boto3 is the Python client library that sends API requests. Kinesis Mock is the
local service that stores the stream records. With real AWS Kinesis, boto3
would connect to AWS instead of this emulator.

The first half builds ingestion through checkpoint tests. The second half
adds source metadata, replay detection, local PySpark batch transformations,
Parquet snapshots, an interactive marimo notebook, and Spark monitoring.
The lab uses one open shard and small datasets throughout; multi-shard
coordination and Structured Streaming are separate topics.

[RESOURCES.md](RESOURCES.md) provides documentation links for the tools and
Python features. [README.md](README.md) records the original learner's progress;
this guide can be followed independently of those session notes.

## 1. Prepare a fresh workspace

You need Docker Engine with the Docker Compose plugin, a text editor, and an
internet connection to download images and dependencies. This guide uses a
Linux host and Bash. Python and uv will run inside Docker; a real AWS account
and host AWS credentials are not required.

All command blocks in this guide run in a **host terminal**, from the project
folder, unless explicitly stated otherwise. Python commands are sent into the
container through `docker compose exec python`.

Check Docker:

```bash
docker --version
docker compose version
docker info
```

The commands should succeed and `docker info` should reach the Docker engine.
If they fail, finish your Docker installation or start the engine first.

Create a fresh folder, using a name that does not already exist:

```bash
mkdir -p ~/code
mkdir ~/code/kinesis-learning-guide
cd ~/code/kinesis-learning-guide
```

Open this folder in your editor. If you use VS Code and have its command
installed, run `code .`.

Use a fresh folder so old checkpoints and output files cannot affect the
results below. If another Kinesis lab is running on host port 4568, stop that
lab before starting this one. Stopping its emulator loses its in-memory
streams and records; its saved host files remain.

## 2. Define the containers

Create `Dockerfile` with this content:

```dockerfile
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /usr/local/bin/

WORKDIR /workspace

CMD ["sleep", "infinity"]
```

The Python image provides Python 3.12 on a small Debian base. The `COPY` line
adds the uv executables from the pinned uv image; it does not create another
running service. `WORKDIR` sets the working directory. `sleep infinity` keeps
the Python container available for our commands.

Now create `compose.yml`:

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

`kinesis` and `python` are service names. Compose lets the Python container
reach the emulator using the hostname `kinesis`. The port mapping also exposes
the emulator API on the host at `127.0.0.1:4568`.

The bind mount `.:/workspace` shares your project folder with the Python
container. Files saved in your editor appear inside it; JSON files written
inside it appear in your host folder. The emulator's records are held
separately in memory.

Preserve YAML indentation: both service names belong beneath `services`.

## 3. Start the environment

From the **host terminal** in the project folder:

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

The first command should produce no output. After the build, both services
should be running. `-d` leaves them running in the background.

Check the Python tools:

```bash
docker compose exec python python --version
docker compose exec python uv --version
```

Expect Python 3.12.x and uv 0.11.8. The Python patch version may vary because
the base image tag specifies a Python minor version, rather than an immutable
image digest.

If the emulator is still starting, wait briefly before creating the stream.
You can inspect its output with `docker compose logs kinesis`.

## 4. Initialise Python dependencies

Run these once, through the Python container:

```bash
docker compose exec python uv init --bare
docker compose exec python uv add "boto3==1.43.121"
docker compose exec python uv run python -c "import boto3; print(boto3.__version__)"
```

The last command should print `1.43.121`. This guide pins the SDK version used
in the exercise. The original project's dependency declaration permits newer
versions, with its resolved version recorded in `uv.lock`.

You should now have:

- `pyproject.toml`: project metadata and the boto3 dependency.
- `uv.lock`: the resolved dependency versions.
- `.venv`: the project Python environment.

uv may name the project `workspace` because it runs in `/workspace`. That is
fine. Use `uv run` for scripts so they can import the project's dependencies.
There is no need to activate `.venv` manually.

## 5. Connect boto3 to Kinesis Mock

Create `client.py`:

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


The endpoint uses the Compose service hostname, `kinesis`. Inside the Python
container, `localhost` would point to the Python container itself.

The credentials are dummy local values for the emulator. `region_name` supplies
a region for the SDK. This setup directs the client to the local API.

Keep client creation separate from stream creation. Importing a Python module
executes its top-level statements, so importing a module that creates a stream
would attempt creation every time a new process imports it.

## 6. Create the stream

Create `create_stream.py`:

```python
from client import kinesis

response = kinesis.create_stream(
    StreamName="orders",
    ShardCount=1,
)


print(response["ResponseMetadata"]["HTTPStatusCode"])
```


Run:

```bash
docker compose exec python uv run python create_stream.py
```

Expect `200`, indicating the create request succeeded. Stream creation can be
asynchronous, so wait for the stream to exist before sending data:

```bash
docker compose exec python uv run python -c 'from client import kinesis; kinesis.get_waiter("stream_exists").wait(StreamName="orders"); print("Stream ready")'
```

Expect `Stream ready`. Our stream has one shard. Run creation once per fresh
emulator; running it again for an existing `orders` stream raises
`ResourceInUseException`.

## 7. Send the first order

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


Run it once:

```bash
docker compose exec python uv run python producer.py
```

Expect two lines: a shard ID, normally `shardId-000000000000` in this lab,
and a long sequence number. Keep the shard ID for the consumers below; if your
emulator returns a different ID, replace the example ID in every consumer.
Sequence numbers vary between runs; use the values returned by your emulator.

The conversion is:

```text
Python dictionary → json.dumps → JSON text → encode("utf-8") → bytes
```

`PartitionKey` influences which shard receives the record. With one shard,
all our records go to the same shard. A successful call stores a new record.
Running the producer again sends another record, even if `order_id` is unchanged.

**Check before continuing:** the producer printed a shard ID and sequence
number without an error.

## 8. Read the order once

Create `consumer_01_print.py`:

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


Run:

```bash
docker compose exec python uv run python consumer_01_print.py
```

Expect a printed dictionary containing `order-001`.

`TRIM_HORIZON` asks to start at the earliest retained record. The returned
`ShardIterator` is a temporary token used by `get_records`. `Limit=10` asks
for at most ten records in that response.

Reading reverses the producer's conversion:

```text
bytes → decode("utf-8") → JSON text → json.loads → Python dictionary
```

This snapshot makes one read request and exits. An empty response does not
prove the stream has no records; try it again after a short wait if needed.
A continuous consumer must poll, which we add in step 10.

Reading does not delete records. Running this consumer again starts at
`TRIM_HORIZON` and can print the same order again.

## 9. Save records as JSON lines

Copy `consumer_01_print.py` to `consumer_02_save.py`, keeping the first file.
Update the copy to contain:

```python
import json
from client import kinesis
from pathlib import Path

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


Run it, then inspect the saved file:

```bash
docker compose exec python uv run python consumer_02_save.py
docker compose exec python cat data/orders.jsonl
```

Expect a JSON line containing `"order_id": "order-001"`. It also appears
in your editor under `data/orders.jsonl`.

`Path` represents a filesystem path. `mkdir(exist_ok=True)` creates the
folder without failing if it already exists. The `with` block closes the
file when its body finishes. `json.dumps(event) + "\n"` writes one JSON object
per line, which is the JSON Lines layout.

Mode `"w"` overwrites the file on every run, even if the read returns no
records. This snapshot still reads only once. Compare it with consumer 01:
the API calls are the same; directory creation and file writing are new.

**Check before continuing:** `data/orders.jsonl` exists and contains the order.

## 10. Poll for new records

Copy `consumer_01_print.py` to `consumer_03_poll.py`. We build on the print-only
snapshot to learn polling before combining it with file writing.

Use this complete code:

```python
import json
import time
from client import kinesis

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

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


The initial iterator request remains outside `while True`. Each read supplies
`NextShardIterator` for the next request. Advance the iterator even when
`Records` is empty. `time.sleep(1)` pauses between calls.

The iterator update and sleep belong inside `while`, outside `for`. Putting
them inside `for` would skip them during empty reads.

In **host terminal A**, run:

```bash
docker compose exec python uv run python consumer_03_poll.py
```

Expect the retained order to print, then the program stays running. Silence
while waiting is normal.

In your editor, change only the producer's order ID to `order-002` and save.
Open **host terminal B** in the same project folder:

```bash
cd ~/code/kinesis-learning-guide
docker compose exec python uv run python producer.py
```

Terminal B should print the producer's shard ID and sequence number, then
return to the prompt. Terminal A should print `order-002` without restarting.

Stop the consumer with **Ctrl+C in terminal A**. A `KeyboardInterrupt` traceback
is expected. Ctrl+Z suspends a program; use Ctrl+C to stop it.

**Check before continuing:** consumer 03 picked up order 002 while it was
already running. No consumer should remain active now.

## 11. Save each nonempty batch to a separate file

Copy `consumer_03_poll.py` to `consumer_04_batches.py`. Use this complete code:

```python
import json
import time
from client import kinesis
from pathlib import Path
from uuid import uuid4

response = kinesis.get_shard_iterator(
    StreamName="orders",
    ShardId="shardId-000000000000",
    ShardIteratorType="TRIM_HORIZON",
)

iterator = response["ShardIterator"]

output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)

while True:
    response = kinesis.get_records(
        ShardIterator=iterator,
        Limit=10,
    )

    records = response["Records"]

    if records:

        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"

        with batch_path.open("w", encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                output.write(json.dumps(event) + "\n")
                print(event)

    iterator = response["NextShardIterator"]
    time.sleep(1)
```


A batch is the list of records returned by one `get_records` request. It may
contain one record or several; request boundaries determine the grouping.

`if records:` prevents empty reads from creating empty files. `uuid4().hex`
generates a random batch identifier. The filename is chosen once per response,
so every record in that batch goes into the same file. New responses get new
filenames, preserving earlier output.

The file-writing `with` block belongs inside `if`. Iterator advancement and
sleep remain outside it so polling continues through empty responses.

In **host terminal A**, run:

```bash
docker compose exec python uv run python consumer_04_batches.py
```

Wait for orders 001 and 002 to print. Open `data/batches` in your editor and
inspect the generated JSONL file or files. The two orders may share a file or
arrive in separate responses; both should be present in the output overall.
Keep the consumer running.

Change the producer's order ID to `order-003` and save. In **host terminal B**,
from the same project directory, run once:

```bash
docker compose exec python uv run python producer.py
```

Expect order 003 to print in terminal A and a new file to appear under
`data/batches`. The earlier files should remain intact. This checks a later
nonempty batch within the same uninterrupted consumer run.

Stop consumer 04 with Ctrl+C in terminal A. Restart it with the same consumer
command. It starts at `TRIM_HORIZON`, so the retained orders print again and
are saved under new filenames. Stop it again with Ctrl+C.

**Check before continuing:** you have demonstrated both a new batch during
polling and replay after restart. Separate files preserve earlier output,
but do not save the consumer's reading position.

## 12. Add a checkpoint

Copy `consumer_04_batches.py` to `consumer_05_checkpoint.py`. A checkpoint
stores the last successfully saved record's sequence number for our one shard.
On restart, the consumer can continue after that position.

Use this complete code:

```python
import json
import time
from client import kinesis
from pathlib import Path
from uuid import uuid4

output_dir = Path("data/batches")
output_dir.mkdir(parents=True, exist_ok=True)
checkpoint_path = Path("data/orders.checkpoint")

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

iterator = response["ShardIterator"]

while True:
    response = kinesis.get_records(
        ShardIterator=iterator,
        Limit=10,
    )

    records = response["Records"]

    if records:

        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"

        with batch_path.open("w", encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                output.write(json.dumps(event) + "\n")
                print(event)
        last_sequence_number = records[-1]["SequenceNumber"]
        checkpoint_path.write_text(last_sequence_number, encoding="utf-8")

    iterator = response["NextShardIterator"]
    time.sleep(1)
```


There are three changes to understand:

1. Directory and checkpoint-path setup now happen before initial iterator
   selection. Keep this setup in one place, directly below the imports.
2. If the checkpoint exists, read its sequence number and request
   `AFTER_SEQUENCE_NUMBER` with `StartingSequenceNumber`. Otherwise request
   `TRIM_HORIZON`. Extract `response["ShardIterator"]` after either branch.
3. After the entire batch file closes, write the last record's sequence number
   to the checkpoint. These lines align with `with`, inside `if records:`.

`records[-1]` selects the final record of a nonempty batch. The checkpoint
must be saved after the batch file closes successfully. Advancing it earlier
could skip output that was never written.

The saved sequence number survives a process restart. `NextShardIterator` is
the temporary token used between reads during a running process.

This version assumes the checkpoint belongs to the current stream and shard.
It does not handle a stale, invalid or partially written checkpoint.

## 13. Verify the first checkpoint save

Use **one host terminal** for the remaining tests. Ensure consumer 04 has
stopped and that `data/orders.checkpoint` does not exist in this fresh project.

Run:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

Expect retained orders 001, 002 and 003 to print. They may be grouped into one
or several batches. Wait until all have appeared, then stop with Ctrl+C.

Inspect the files:

```bash
ls -1 data/batches
cat data/orders.checkpoint
```

Expect new batch output and a checkpoint containing a long sequence number.
Compare it with the producer's sequence number for order 003: once that order
is saved, the checkpoint should match it. There is no trailing newline in the
checkpoint, so the next shell prompt may appear immediately after the number.

Record the number of batch files in your editor or from the listing. Do not
expect the original session's batch count: grouping and earlier test runs can
change it.

**Check before continuing:** all three orders were saved by consumer 05 and
the checkpoint points to the last one.

## 14. Verify restart without replay

Without sending anything else, start consumer 05 again:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

It should stay running silently because it starts after the checkpoint.
Leave it running for a few seconds, then stop with Ctrl+C.

Inspect the batch listing and checkpoint again:

```bash
ls -1 data/batches
cat data/orders.checkpoint
```

There should be no new batch files, and the checkpoint should be unchanged.
The old output from consumer 04 still exists; this test checks that consumer
05 has added no further copies on this restart.

**Check before continuing:** restarting consumer 05 did not print or save the
old orders again.

## 15. Verify a new event after the checkpoint

Keep using the same **host terminal**, with the consumer stopped. In your
editor, change the producer's order ID to `order-004` and save.

Send it once:

```bash
docker compose exec python uv run python producer.py
```

Note the returned sequence number. Now start the checkpoint consumer:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

Expect only order 004 to print, then the consumer waits. Stop it with Ctrl+C.
Inspect the newest batch in your editor and read the checkpoint:

```bash
cat data/orders.checkpoint
```

The new batch should contain order 004, and the checkpoint should equal the
sequence number just returned by the producer. Old batch files remain intact.

**Check before continuing:** the consumer read and saved the new record while
skipping previously checkpointed records.

## 16. Understand the remaining failure window

Our consumer performs two separate writes:

```text
Save and close batch file → update checkpoint
```

Imagine the checkpoint is at order 004. The consumer receives order 005,
saves its batch, then crashes before updating the checkpoint.

On restart it reads order 005 again because the checkpoint still points to
order 004. The existing batch remains, and another randomly named file stores
the replayed record. The result is duplicate output.

Now imagine reversing the writes: update the checkpoint to order 005, then
crash before saving its batch. Restarting skips order 005 even though it never
reached the output. The result is missing output.

We save output first because replay is preferable to skipping unsaved data.
These separate writes do not provide exactly-once processing. This is a
reasoning exercise; you do not need to edit the code or inject a crash here.

Also distinguish replay from repeated production: if the producer sends the
same `order_id` twice, it creates separate stream records. The checkpoint
tracks stream progress, rather than uniqueness of business order IDs.

## 17. Preserve record identity for the processing stage

So far each saved JSON line contains only the business event. That loses the
metadata needed to distinguish a replayed record from a second producer send.
Before processing with Spark, preserve that metadata in a new snapshot.

Within this stream instance, a record's shard ID and sequence number identify
its position. A replay has the same source identity. Sending the producer
again creates a different sequence number, even with the same order ID.

Create `consumer_06_envelopes.py`:

```python
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from client import kinesis

stream_name = "orders"
shard_id = "shardId-000000000000"
output_dir = Path("data/raw")
output_dir.mkdir(parents=True, exist_ok=True)
checkpoint_path = Path("data/orders_enveloped.checkpoint")

iterator_args = {
    "StreamName": stream_name,
    "ShardId": shard_id,
    "ShardIteratorType": "TRIM_HORIZON",
}
if checkpoint_path.exists():
    iterator_args["ShardIteratorType"] = "AFTER_SEQUENCE_NUMBER"
    iterator_args["StartingSequenceNumber"] = checkpoint_path.read_text(
        encoding="utf-8"
    ).strip()

iterator = kinesis.get_shard_iterator(**iterator_args)["ShardIterator"]

while True:
    response = kinesis.get_records(ShardIterator=iterator, Limit=10)
    records = response["Records"]

    if records:
        batch_path = output_dir / f"orders-{uuid4().hex}.jsonl"
        with batch_path.open("w", encoding="utf-8") as output:
            for record in records:
                event = json.loads(record["Data"].decode("utf-8"))
                envelope = {
                    "stream": stream_name,
                    "shard_id": shard_id,
                    "sequence_number": record["SequenceNumber"],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload": event,
                }
                output.write(json.dumps(envelope) + "\n")
                print(event, flush=True)

        checkpoint_path.write_text(
            records[-1]["SequenceNumber"], encoding="utf-8"
        )

    iterator = response["NextShardIterator"]
    time.sleep(1)
```

This wraps the event in an envelope: `stream`, `shard_id`, `sequence_number`,
`ingested_at` and `payload`. The payload is still our order dictionary;
`ingested_at` records when this consumer saved it, rather than the business
event time. Sequence numbers stay as strings because they can be very large.

`**iterator_args` passes dictionary entries as keyword arguments. This lets us
add `StartingSequenceNumber` only when a saved checkpoint exists.

The new snapshot has its own output directory, `data/raw`, and its own
checkpoint, `data/orders_enveloped.checkpoint`. This is intentional: a new
output format needs a complete backfill. Reusing consumer 05's checkpoint
would skip the existing orders and leave `data/raw` empty.

Make sure consumer 05 is stopped. With the original emulator still running,
run from the **host terminal**:

```bash
docker compose exec python uv run python consumer_06_envelopes.py
```

Wait until retained orders 001–004 print, then stop with Ctrl+C. Open the
JSONL files under `data/raw` and inspect their envelope fields. Starting the
new consumer a second time with no new sends should wait silently; stop it
again before continuing.

If you followed the sends exactly, this stream has four records. Repeated
producer invocations may have created more; preserve them and use your actual
record count for later checks.

**Check before continuing:** the four original records are represented in
`data/raw`, their source identities are saved, and the new checkpoint exists.
The earlier payload-only batches remain as learning output. Spark will read
only the envelopes under `data/raw`.

## 18. Add Java, PySpark and marimo

PySpark runs Spark's JVM engine through Python. Install Java in the Python
image, then add PySpark and marimo as Python dependencies. This guide uses
PySpark 4.0.2 and marimo 0.19.11, matching the earlier full lab's versions.

Replace `Dockerfile` with:

```dockerfile
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /usr/local/bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

CMD ["sleep", "infinity"]
```

Java is installed on the container's command path. This Debian package setup
lets Spark locate `java` without a hard-coded architecture-specific
`JAVA_HOME`. If you adapt the image, follow the
[PySpark installation requirements](https://spark.apache.org/docs/4.0.2/api/python/getting_started/install.html).

Replace `compose.yml` with:

```yaml
services:
  kinesis:
    image: ghcr.io/etspaceman/kinesis-mock:0.6.2
    ports:
      - "127.0.0.1:4568:4568"

  python:
    build: .
    environment:
      SPARK_LOCAL_IP: 0.0.0.0
      PYSPARK_PYTHON: /workspace/.venv/bin/python
      PYSPARK_DRIVER_PYTHON: /workspace/.venv/bin/python
      PYTHONPATH: /workspace
    volumes:
      - .:/workspace
    ports:
      - "127.0.0.1:8080:8080"
      - "127.0.0.1:4040:4040"
      - "127.0.0.1:18080:18080"
```

The new ports are for marimo, the live Spark UI and Spark History Server.
`SPARK_LOCAL_IP` lets the Spark UI listen on the container interface; the batch
code below keeps driver RPC traffic on loopback. The Python settings ensure
Spark workers use the same uv environment. `PYTHONPATH` lets the notebook
import our root-level `batch.py` module.

With all consumers stopped, rebuild **only the Python service**:

```bash
docker compose config --quiet
docker compose up -d --build --no-deps python
docker compose exec python java -version
docker compose exec python uv add "pyspark==4.0.2" "marimo==0.19.11"
docker compose exec python uv run python -c "import pyspark, marimo; print(pyspark.__version__, marimo.__version__)"
```

Expect Java 17 and `4.0.2 0.19.11`. The PySpark download is large and can take
several minutes. `--no-deps python` preserves the running emulator and its
stream while replacing the Python container. Do not run `docker compose down`
at this point.

Add this configuration at the end of `pyproject.toml`, outside `[project]`:

```toml
[tool.marimo.runtime]
auto_instantiate = false
on_cell_change = "lazy"
```

This gives you control over when expensive Spark cells run. Start cells
manually and run dependent cells after changes. Dependency versions are
recorded in `uv.lock`; keep that file with the project.

## 19. Build the Spark batch transformations

Create `batch.py` in the project root:

```python
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

DATA_DIR = Path(__file__).resolve().parent / "data"


def create_spark():
    event_dir = DATA_DIR / "spark-events"
    event_dir.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Orders from Kinesis")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.port", "4040")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_dir.as_uri())
        .config("spark.eventLog.compress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark):
    raw_files = sorted((DATA_DIR / "raw").glob("*.jsonl"))
    if not raw_files:
        raise FileNotFoundError("Run consumer_06_envelopes.py before this batch.")

    event_schema = StructType([
        StructField("order_id", StringType()),
        StructField("product", StringType()),
        StructField("quantity", LongType()),
    ])
    envelope_schema = StructType([
        StructField("stream", StringType()),
        StructField("shard_id", StringType()),
        StructField("sequence_number", StringType()),
        StructField("ingested_at", StringType()),
        StructField("payload", event_schema),
    ])
    return (
        spark.read.schema(envelope_schema)
        .option("mode", "FAILFAST")
        .json([str(path) for path in raw_files])
    )


def clean_orders(raw):
    invalid = raw.filter(
        F.col("stream").isNull()
        | F.col("shard_id").isNull()
        | F.col("sequence_number").isNull()
        | F.col("payload.order_id").isNull()
        | F.col("payload.product").isNull()
        | F.col("payload.quantity").isNull()
        | (F.col("payload.quantity") <= 0)
    )
    if invalid.limit(1).count():
        raise ValueError("Invalid record: inspect data/raw; no outputs were written.")

    return (
        raw.dropDuplicates(["stream", "shard_id", "sequence_number"])
        .select("stream", "shard_id", "sequence_number", "payload.*")
    )


def summarise(orders):
    return orders.groupBy("product").agg(
        F.count("*").alias("record_count"),
        F.sum("quantity").alias("units"),
    )


def write_outputs(orders, summary):
    orders.write.mode("overwrite").parquet(str(DATA_DIR / "parquet" / "orders"))
    summary.write.mode("overwrite").parquet(str(DATA_DIR / "parquet" / "product_totals"))


def main():
    spark = create_spark()
    try:
        raw = read_raw(spark)
        orders = clean_orders(raw).cache()
        summary = summarise(orders)
        print(f"Raw rows: {raw.count()}; unique stream records: {orders.count()}")
        orders.orderBy("order_id", "sequence_number").show(truncate=False)
        summary.orderBy("product").show(truncate=False)
        write_outputs(orders, summary)
        saved = spark.read.parquet(str(DATA_DIR / "parquet" / "orders"))
        print(f"Read back {saved.count()} Parquet rows.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

Read the functions in execution order:

1. `create_spark` starts one local Spark application with two worker threads.
   It enables event logs for later inspection and uses two shuffle partitions
   to keep this small lab manageable.
2. `read_raw` reads all envelope JSONL files with an explicit nested schema.
   `FAILFAST` stops on malformed JSON or incompatible values rather than
   silently accepting corrupt records.
3. `clean_orders` checks required fields and positive quantities, removes
   replayed source identities, and flattens `payload` into ordinary columns.
4. `summarise` groups records by product and calculates record counts and units.
5. `write_outputs` rebuilds two Parquet datasets from all current raw files.

The schema includes the fields used here. Spark does not automatically reject
all unknown fields or enforce every business rule; validation is deliberately
small and explicit. A validation failure stops this batch and leaves raw JSON
available for inspection.

`dropDuplicates` uses stream, shard and sequence number. It removes repeated
copies of one source record. It does not merge separate producer sends merely
because their order IDs match. Therefore `record_count` means unique stream
records, not guaranteed unique business orders.

Spark transformations are lazy: defining a DataFrame builds a plan. Actions
such as `count`, `show`, `collect` and writing files execute it. `cache` allows
the CLI batch to reuse its cleaned dataset across subsequent actions.

Run from the **host terminal**:

```bash
docker compose exec python uv run python batch.py
```

For four original sends, expect:

```text
Raw rows: 4; unique stream records: 4
```

The displayed orders should contain IDs 001–004. Because all four original
orders contain `product="book"` and `quantity=1`, the product summary should be:

| product | record_count | units |
| --- | --- | --- |
| book | 4 | 4 |

The final message should say `Read back 4 Parquet rows.` If you sent extra
records, counts should reflect them. Spark startup warnings and progress bars
may accompany these results.

See the official [JSON source guide](https://spark.apache.org/docs/4.0.2/sql-data-sources-json.html)
for the input format and [Parquet guide](https://spark.apache.org/docs/4.0.2/sql-data-sources-parquet.html)
for the output format.

## 20. Inspect Parquet and verify repeatable processing

Open these directories in your editor:

```text
data/parquet/orders/
data/parquet/product_totals/
```

A Spark Parquet dataset is a directory containing part files and metadata such
as `_SUCCESS`. Parquet is a typed, column-oriented binary format; inspect it
through Spark rather than expecting readable JSON text.

Run `batch.py` again without changing the raw input. Counts and totals should
remain the same. Output uses `mode("overwrite")` because each run reads all raw
files and rebuilds a complete snapshot. Appending the same full input on each
run would duplicate output.

This overwrites only the derived Parquet directories; raw JSON remains intact.
The two output writes are not a single transaction. If one write fails, fix
the problem and rerun the batch before treating both snapshots as consistent.

Now demonstrate replay handling without sending another business event. Create
`simulate_replay.py`:

```python
from pathlib import Path
from shutil import copyfile
from uuid import uuid4

raw_dir = Path("data/raw")
source = next(raw_dir.glob("orders-*.jsonl"))
copy = raw_dir / f"replay-{uuid4().hex}.jsonl"
copyfile(source, copy)
print(f"Copied {source.name} to {copy.name}")
```

Run:

```bash
docker compose exec python uv run python simulate_replay.py
docker compose exec python uv run python batch.py
```

Raw row count should increase by the number of records in the copied file.
Unique stream-record count, product totals and Parquet row count should remain
unchanged. This deliberately copies landed envelopes to reproduce duplicate
input; it does not inject an actual consumer crash.

**Check before continuing:** Parquet can be read back, rerunning with unchanged
input preserves counts, and copying raw records does not inflate cleaned totals.

The identity key assumes one stream instance. If you reset the emulator,
archive the old `data/raw` directory and both checkpoints before landing the
new stream. This schema does not include a stream-generation identifier for
mixing multiple incarnations of the same named stream.

## 21. Explore the pipeline in marimo

marimo is the interactive notebook layer. It calls the same functions as the
command-line batch, so processing logic stays in one place.

Create the notebook directory from the **host terminal**:

```bash
mkdir -p notebooks
```

Create `notebooks/orders.py` with the following complete notebook code. Keep
`batch.py` in the project root:

```python
import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from batch import DATA_DIR, create_spark, read_raw, clean_orders, summarise, write_outputs
    return DATA_DIR, clean_orders, create_spark, mo, read_raw, summarise, write_outputs


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # From Kinesis to Parquet

    Run the cells in order. This notebook reads landed JSON files;
    it does not poll Kinesis. The Spark UI is at http://127.0.0.1:4040.
    """)
    return


@app.cell
def _(create_spark):
    spark = create_spark()
    return (spark,)


@app.cell
def _(mo, read_raw, spark):
    raw = read_raw(spark)
    raw.printSchema()
    mo.ui.table([row.asDict(recursive=True) for row in raw.limit(20).collect()])
    return (raw,)


@app.cell
def _(clean_orders, mo, raw):
    orders = clean_orders(raw)
    mo.ui.table([row.asDict() for row in orders.orderBy("order_id").limit(20).collect()])
    return (orders,)


@app.cell
def _(mo, orders, summarise):
    summary = summarise(orders)
    mo.ui.table([row.asDict() for row in summary.orderBy("product").limit(100).collect()])
    return (summary,)


@app.cell
def _(mo):
    minimum_quantity = mo.ui.slider(1, 5, value=1, label="Minimum quantity")
    minimum_quantity
    return (minimum_quantity,)


@app.cell
def _(minimum_quantity, mo, orders):
    filtered = orders.filter(orders.quantity >= minimum_quantity.value)
    mo.ui.table([row.asDict() for row in filtered.orderBy("order_id").limit(100).collect()])
    return


@app.cell
def _(mo):
    write_button = mo.ui.run_button(label="Rebuild Parquet snapshots")
    write_button
    return (write_button,)


@app.cell
def _(DATA_DIR, mo, orders, summary, write_button, write_outputs):
    mo.stop(not write_button.value, mo.md("Click the button to write both datasets."))
    write_outputs(orders, summary)
    written_path = str(DATA_DIR / "parquet" / "orders")
    mo.md("Parquet snapshots written. Run the next cell to read them back.")
    return (written_path,)


@app.cell
def _(mo, spark, written_path):
    parquet_orders = spark.read.parquet(written_path)
    mo.ui.table([row.asDict() for row in parquet_orders.orderBy("order_id").limit(100).collect()])
    return


if __name__ == "__main__":
    app.run()
```

Make sure the CLI batch has finished before starting a notebook Spark session.
From **host terminal A**, run:

```bash
docker compose exec python uv run marimo edit notebooks/orders.py --headless --host 0.0.0.0 --port 8080
```

Open the URL printed by marimo in your host browser. Use host address
`127.0.0.1:8080`; preserve any access-token query string in the printed URL.
The process keeps running in terminal A. `--host 0.0.0.0` makes the server
reachable through Docker's published port, which is bound to host loopback.

In the editor:

1. Run cells from top to bottom with Shift+Enter or the cell run controls.
   The lazy runtime may mark dependent cells stale; run them when prompted.
2. Inspect the raw envelope table and printed schema.
3. Inspect cleaned orders and product totals.
4. Run the slider cell, then its dependent table cell. With our initial
   quantities of one, minimum quantity two produces an empty table.
5. Return the slider to one. The filter affects its display table; the write
   button rebuilds snapshots from the full cleaned `orders` DataFrame.
6. Run the button cell, click **Rebuild Parquet snapshots**, and run its
   dependent write cell if needed. Then run the final read-back cell.

The write button gates a filesystem side effect. Do not rerun the Spark-creation
cell repeatedly; reuse its session. Stop the marimo process with Ctrl+C before
switching back to a CLI Spark batch. Run one Spark application at a time in
this lab so the published 4040 port refers to the application you expect.

The notebook uses bounded `collect` calls to display small tables. `collect`
transfers rows into the driver process; keep limits when adapting it to larger
data. All input and derived files remain in the host project folder.

For more detail, see the [marimo quickstart](https://docs.marimo.io/getting_started/quickstart/),
[table widget reference](https://docs.marimo.io/api/inputs/table/), and
[run button reference](https://docs.marimo.io/api/inputs/run_button/).

**Check before continuing:** tables render in the browser, the quantity filter
works, the button writes Parquet, and the last cell reads the dataset back.

## 22. Inspect Spark jobs and completed application history

While the notebook's Spark session is active, open
[the live Spark UI](http://127.0.0.1:4040). Run a table or write cell to generate
work, then inspect:

- **Jobs**: actions submitted by your notebook or batch.
- **Stages**: how Spark splits work, including shuffles for grouping or deduplication.
- **SQL / DataFrame**: executed query plans.
- **Environment**: settings such as the local master and shuffle partitions.

Jobs finish quickly on four records. The live UI exists while its Spark
application is running. A CLI batch stops its session in `finally`, so its
4040 UI closes when the script ends.

Our Spark setup also saves event logs under `data/spark-events`. To inspect
completed applications, create `history_server.py` in the project root:

```python
import os
from pathlib import Path

import pyspark

from batch import DATA_DIR

event_dir = DATA_DIR / "spark-events"
event_dir.mkdir(parents=True, exist_ok=True)
spark_home = Path(pyspark.__file__).resolve().parent
os.environ["SPARK_HOME"] = str(spark_home)
os.environ["SPARK_HISTORY_OPTS"] = (
    f"-Dspark.history.fs.logDirectory={event_dir.as_uri()} "
    "-Dspark.history.ui.port=18080"
)
launcher = str(spark_home / "bin" / "spark-class")
os.execv(launcher, [launcher, "org.apache.spark.deploy.history.HistoryServer"])
```

From **host terminal B**, in the project directory, run:

```bash
cd ~/code/kinesis-learning-guide
docker compose exec python uv run python history_server.py
```

Open [Spark History Server](http://127.0.0.1:18080). Allow a few seconds for the
logs to be discovered. Completed CLI runs should appear there; click an
application to inspect its jobs and stages. A running notebook application may
be listed as incomplete until its Spark session ends.

The script locates the Spark distribution installed by PySpark and starts its
History Server with our event-log directory. History Server reads logs; it
does not run the transformations again. Stop it with Ctrl+C in terminal B when
finished. See [Spark monitoring documentation](https://spark.apache.org/docs/4.0.2/monitoring.html)
for the live UI and history behaviour.

## 23. Run the complete workflow with a new order

This exercise proves the pieces work together beyond the initial backfill.
Stop marimo and History Server with Ctrl+C in their respective host terminals.
Use one **host terminal** for the following commands.

Change the event in `producer.py` to:

```python
event = {
    "order_id": "order-005",
    "product": "pen",
    "quantity": 3,
}
```

Save it and send it exactly once:

```bash
docker compose exec python uv run python producer.py
```

Run the envelope consumer:

```bash
docker compose exec python uv run python consumer_06_envelopes.py
```

Expect only order 005 to print. Once it appears and the checkpoint advances,
stop with Ctrl+C. Then run:

```bash
docker compose exec python uv run python batch.py
```

If there were four original records, expect five unique stream records and
these product totals, even with the simulated replay file still present:

| product | record_count | units |
| --- | --- | --- |
| book | 4 | 4 |
| pen | 1 | 3 |

Start marimo again using step 21's command and run the cells. You should see
the new order and updated totals. Set the minimum quantity to two; the filtered
table should now show order 005 with quantity three.

If the notebook was already open while another process landed files, its
existing `raw` DataFrame does not automatically discover them. Rerun the
`read_raw` cell and downstream cells to build a fresh file listing before
rebuilding outputs. Stop the notebook when finished.

The end-to-end path is now:

```text
Send order → checkpointed landing → raw envelopes → Spark validation
          → source deduplication → product totals → Parquet → notebook tables
```

## 24. Review the finished project

Your project should contain:

```text
kinesis-learning-guide/
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
├── consumer_06_envelopes.py
├── batch.py
├── simulate_replay.py
├── history_server.py
├── notebooks/
│   └── orders.py
└── data/
    ├── orders.jsonl
    ├── orders.checkpoint
    ├── orders_enveloped.checkpoint
    ├── batches/
    ├── raw/
    ├── parquet/
    │   ├── orders/
    │   └── product_totals/
    └── spark-events/
```

Each component has a distinct responsibility:

| Component | Responsibility |
| --- | --- |
| Producer and boto3 client | Send events to the local Kinesis service |
| Consumers 01–05 | Build understanding of reading, polling, batch files and checkpointing |
| Consumer 06 | Land events with source identity and resume its own output stream |
| `batch.py` | Validate, deduplicate source records, aggregate and rebuild Parquet |
| marimo notebook | Explore those transformations and their outputs interactively |
| Spark UI and History Server | Inspect execution while running and after completion |

The core exercise is complete when you have demonstrated polling, separate
batches, checkpoint resume, raw metadata, replay deduplication, repeatable
Parquet writes, notebook interaction and the new-order workflow in step 23.

The limits remain explicit: this is local batch processing over landed files,
with one open shard. Checkpoints and output writes are not transactional;
validation errors stop processing; Parquet snapshots are rebuilt from all raw
input. Multiple shards, automatic recovery, evolving business schemas,
production table formats and Structured Streaming can follow separately.

## 25. Pause, return or start fresh

Run these commands from the **host terminal** in the project folder.

To leave the environment running, stop foreground consumers, marimo and
History Server with Ctrl+C in their respective terminals. You can return later
with:

```bash
cd ~/code/kinesis-learning-guide
docker compose ps
```

With the same emulator process and valid checkpoint, resume landing through
`consumer_06_envelopes.py`. Use `batch.py` to rebuild Parquet after new files
arrive, or launch the marimo notebook again. Existing raw files can be processed
with Spark even when the Kinesis stream is unavailable.

To freeze and later resume containers without restarting the emulator process:

```bash
docker compose pause
docker compose unpause
```

To stop and remove containers:

```bash
docker compose down
```

That loses the emulator's in-memory streams and records. Source files, raw JSON,
Parquet, event logs and checkpoints remain in the shared project folder.

For an intentional new stream instance, archive the old data together so old
source identities and checkpoints do not get mixed with a newly created stream.
With all foreground processes stopped, choose an unused archive directory name:

```bash
mv -i data data.previous-run
docker compose up -d --build
docker compose exec python uv sync --locked
```

Do this after stopping the previous emulator with `down`. If the archive name
already exists, choose another name before moving anything. Skip the move if
there is no data directory. The Python environment lives in the shared `.venv`;
`uv sync --locked` restores dependencies if needed.

Recreate `orders` using step 6, send an event, then run consumer 06. It starts
without a checkpoint and creates a fresh `data/raw` directory. Run the Spark
batch and notebook again. The earlier data remains available in the archive.

For a complete repeat with the exact initial expectations, use another fresh
project folder and follow the guide from step 1.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| A published port is already allocated | Stop the competing local service or change the host side of the port mapping. Browser URLs must use the new host port. |
| `python` service is not running | Run `docker compose ps`, then `docker compose up -d --build`. |
| Connection refused when calling Kinesis | Inspect `docker compose logs kinesis`; wait for startup and check the client endpoint. |
| `ModuleNotFoundError` | Run through `docker compose exec python uv run ...`; check the dependency-install steps. |
| `ResourceInUseException` | The stream already exists; do not recreate it unnecessarily. |
| Stream not found or resume fails after a reset | Recreate the stream and start a new data generation as described in step 25. |
| Consumer stays silent | It may be waiting on empty reads or caught up to its checkpoint. |
| Event prints but no raw envelope appears | Run consumer 06 and inspect `data/raw`; earlier snapshots use different output formats or only print. |
| Consumer suspended | In its original terminal, use `fg`, then Ctrl+C to stop deliberately. |
| Raw contains repeated order IDs | Producer sends are separate source records; source deduplication does not imply unique business order IDs. |
| Spark reports Java missing or gateway exited | Rebuild the Python image from step 18 and verify `docker compose exec python java -version`. |
| Spark or a worker uses a different Python version | Check both `PYSPARK_PYTHON` settings and use the container's uv environment. |
| Spark says no raw input | Consumer 06 needs its own backfill and checkpoint; payload-only `data/batches` is not its input. |
| Validation fails | Inspect the raw file and payload types. Preserve the original; correct or quarantine invalid input before retrying. This guide does not implement an automatic dead-letter queue. |
| Spark UI unreachable | Keep one Spark application active; check port 4040, `SPARK_LOCAL_IP`, and that the session has not stopped. |
| History Server shows no completed apps | Finish a batch, wait for log discovery, and check `data/spark-events` plus its configured path. |
| marimo browser cannot connect | Use host port 8080 and the printed access token. Check the server binds `0.0.0.0` inside the container. |
| Notebook shows old data or stale cells | Rerun `read_raw` and dependent cells after landing new files; lazy mode needs explicit execution. |
| Parquet results have unexpected totals | Compare raw counts with unique source counts. The batch reads all raw generations in its directory; keep separate stream instances archived separately. |
| Files are owned by root | The learning container runs as root; matching host ownership is a separate environment improvement. |

## Validation of the added processing stages

The Java/PySpark/marimo setup and the exact Python examples in steps 17–22
were checked in isolated Docker containers on 2 November 2026, using the
versions specified above. Checks covered live Kinesis landing and checkpoint
resume, Spark validation and aggregation, rejection of a zero quantity,
Parquet read-back and repeatable writes, replay deduplication, a new order,
notebook execution and HTML export, and HTTP responses from the marimo editor,
live Spark UI and History Server. The test setup used different host ports
to isolate it from other local labs. These checks do not mark the original
learner's pending exercises as completed.
