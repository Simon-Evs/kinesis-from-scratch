# Project resources and documentation

Use this as a reference while writing or troubleshooting the lab. The
[README](README.md) contains the learning sequence and progress; this file
maps the tools and Python features to their documentation.

Links checked on 2 November 2026. Versions below come from the project files;
documentation labelled `latest` can describe a newer release.

## What we use

| Resource | Purpose in this project | Configuration or code |
| --- | --- | --- |
| Docker and Docker Compose | Run and connect the Python and Kinesis containers | [Dockerfile](Dockerfile), [compose.yml](compose.yml) |
| Python 3.12 | Run the producer and numbered consumers | Base image: `python:3.12-slim-bookworm` |
| uv 0.11.8 | Manage Python dependencies and run scripts in the project environment | [Dockerfile](Dockerfile), [pyproject.toml](pyproject.toml), [uv.lock](uv.lock) |
| Kinesis Mock 0.6.2 | Provide a local Kinesis emulator | Image: `ghcr.io/etspaceman/kinesis-mock:0.6.2` in [compose.yml](compose.yml) |
| boto3 1.43.121 | Call the emulator through the AWS Python SDK | Version recorded in [uv.lock](uv.lock); client in [client.py](client.py) |
| Python standard library | Convert JSON, handle files, generate batch names and pause polling | `json`, `pathlib`, `uuid`, `time` in the consumers |

## Docker and Compose: running the environment

Start with the [Compose overview](https://docs.docker.com/compose/) for how
services fit together. Use the following references for individual settings:

| Documentation | What to look up in our project |
| --- | --- |
| [Dockerfile reference](https://docs.docker.com/reference/dockerfile/) | `FROM`, `COPY --from`, `WORKDIR` and `CMD` |
| [Compose service settings](https://docs.docker.com/reference/compose-file/services/) | `image`, `build`, `ports` and `volumes` |
| [Bind mounts](https://docs.docker.com/engine/storage/bind-mounts/) | Why `.:/workspace` shares source files and saved data between host and container |
| [Compose command reference](https://docs.docker.com/reference/cli/docker/compose/) | `up`, `ps`, `logs`, `top`, `pause`, `unpause` and `down` |
| [Compose exec](https://docs.docker.com/reference/cli/docker/compose/exec/) | Entering the Python container or running a script inside it |

Run Compose commands in a **host terminal**, from this project directory:

```bash
docker compose ps
docker compose exec python bash
```

After the second command, you are in the **container terminal** at
`/workspace`. Run Python there with `uv run python <filename>.py`. Type `exit`
to return to the host. You can also run a script directly from the host:

```bash
docker compose exec python uv run python consumer_05_checkpoint.py
```

The `kinesis` service name is the hostname used by Python inside the Compose
network. Our client connects to `http://kinesis:4568`. The published host
address is `http://127.0.0.1:4568`; this is an API endpoint.

## uv: dependencies and script execution

The [project guide](https://docs.astral.sh/uv/guides/projects/) explains
`pyproject.toml`, `.venv`, `uv.lock`, dependency management and project commands.
The [running commands reference](https://docs.astral.sh/uv/concepts/projects/run/)
explains how `uv run` selects and prepares the project environment.

| Command, inside the Python container | Use |
| --- | --- |
| `uv init --bare` | Initialise the project once; already done here |
| `uv add boto3` | Add the SDK dependency; already done here |
| `uv sync --locked` | Recreate or synchronise the environment using the existing lockfile |
| `uv run python producer.py` | Run the producer with the project's dependencies |
| `uv run python consumer_05_checkpoint.py` | Run the checkpoint consumer with those dependencies |

See [uv in Docker](https://docs.astral.sh/uv/guides/integration/docker/) for the
image-copy approach used in our Dockerfile. Our source configuration pins uv
to 0.11.8; examples in current documentation may use a newer version.

## Kinesis Mock: the local service

The [Kinesis Mock repository](https://github.com/etspaceman/kinesis-mock)
contains its setup instructions, configuration options and supported API
information. Consult it for emulator behaviour and differences from AWS.

Our Compose file pins image version 0.6.2. The lab uses an `orders` stream with
one shard. No emulator persistence is configured here: restarting its process
loses streams and records, while the files written into our shared project
directory remain. An existing checkpoint can therefore outlive its stream.

For the service concepts behind the emulator, read the AWS
[Kinesis terminology and concepts](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html)
page. It explains streams, records, shards, partition keys, sequence numbers,
producers and consumers. Use AWS documentation for the API model and the
emulator's documentation for its implementation.

## boto3: sending and reading records

The [Kinesis client reference](https://docs.aws.amazon.com/boto3/latest/reference/services/kinesis.html)
lists the available methods. For our connection settings, the
[session client reference](https://docs.aws.amazon.com/boto3/latest/reference/core/session.html#boto3.session.Session.client)
documents `endpoint_url`, `region_name` and credential arguments.
Our [client.py](client.py) points at the emulator with dummy local credentials.

These are the API methods used so far, in learning order:

| Documentation | Where used | What to inspect |
| --- | --- | --- |
| [`create_stream`](https://docs.aws.amazon.com/boto3/latest/reference/services/kinesis/client/create_stream.html) | [create_stream.py](create_stream.py) | `StreamName`, `ShardCount` and stream creation behaviour |
| [`put_record`](https://docs.aws.amazon.com/boto3/latest/reference/services/kinesis/client/put_record.html) | [producer.py](producer.py) | `Data`, `PartitionKey`, returned `ShardId` and `SequenceNumber` |
| [`get_shard_iterator`](https://docs.aws.amazon.com/boto3/latest/reference/services/kinesis/client/get_shard_iterator.html) | All consumers | `TRIM_HORIZON`, `AFTER_SEQUENCE_NUMBER`, `StartingSequenceNumber` and returned `ShardIterator` |
| [`get_records`](https://docs.aws.amazon.com/boto3/latest/reference/services/kinesis/client/get_records.html) | All consumers | `Limit`, returned `Records`, record `Data`, `SequenceNumber` and `NextShardIterator` |

When reading a boto3 method page, start with **Request Syntax**, then read the
parameter descriptions. Next find **Response Syntax** and the response field
descriptions. This connects the keyword arguments in our calls to the keys
we read from the returned dictionaries. Not every parameter shown in the
reference is required for our example.

For checkpoint resume, follow this path through
[consumer_05_checkpoint.py](consumer_05_checkpoint.py): read the saved sequence
number, supply it as `StartingSequenceNumber`, obtain `ShardIterator`, then
use each response's `NextShardIterator` while polling. The checkpoint itself
is our local file-writing logic, rather than a boto3 checkpoint method.

## Python: understanding the code around the API calls

These links use Python 3.12 documentation to match our container. The modules
below come with Python; only boto3 is added as a project dependency.

| Documentation | Features we use | Example in this lab |
| --- | --- | --- |
| [`json`](https://docs.python.org/3.12/library/json.html) | `dumps`, `loads` | Convert an event dictionary to JSON text and back |
| [`str` and `bytes`](https://docs.python.org/3.12/library/stdtypes.html) | `str.encode`, `bytes.decode` | Convert JSON text to UTF-8 bytes for sending, then decode received bytes |
| [`pathlib`](https://docs.python.org/3.12/library/pathlib.html) | `Path`, `/`, `mkdir`, `open`, `exists`, `read_text`, `write_text` | Build batch paths and read or write the checkpoint |
| [`uuid`](https://docs.python.org/3.12/library/uuid.html) | `uuid4` and `.hex` | Generate a new batch filename |
| [`time`](https://docs.python.org/3.12/library/time.html#time.sleep) | `sleep` | Pause for one second between polling requests |
| [Control flow tutorial](https://docs.python.org/3.12/tutorial/controlflow.html) | `if` and `for`; links to `while` | Handle nonempty batches and loop over records |
| [Input and output tutorial](https://docs.python.org/3.12/tutorial/inputoutput.html) | f-strings, file modes, `with` and file writing | Construct filenames and close each batch file after writing |
| [Built-in types](https://docs.python.org/3.12/library/stdtypes.html) | Dictionaries, lists, indexing and truth values | `response["Records"]`, `records[-1]` and `if records:` |

Our `.jsonl` output contains one JSON object per line. We produce it by
writing `json.dumps(event) + "\n"` for each event. It is our chosen file layout,
separate from the format of a boto3 response.

## Finding the right reference

| If you are asking… | Start here |
| --- | --- |
| Which terminal should I use, or is the container running? | Docker and Compose section |
| Why can Python not import boto3? | uv section; check that the script runs through `uv run` inside the container |
| What does a shard or sequence number mean? | Kinesis concepts link |
| Which arguments does this Kinesis call accept? | The specific boto3 method page |
| What keys are present in the returned dictionary? | Response Syntax on that method page |
| Why is my file path, JSON conversion or loop behaving this way? | The relevant Python reference |
| What have I completed, and what exercise comes next? | “Where we got to” near the end of [README.md](README.md#where-we-got-to) |

PySpark, marimo and Parquet are deferred in this project. Add their resources
when those exercises begin so this reference continues to reflect the tools
actually used here.
