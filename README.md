# ZipLog (TCP)

This repo is a TCP/sockets-based implementation of ZipLog, a totally ordered shared log. ZipLog is described in the paper:

**Linearizable Shared Log API Considered Harmful**

Yu-Ju Huang<sup>1</sup>, Shubham Chaudhary<sup>1</sup>, Rafael Soares<sup>2</sup>, Shir Cohen Gahtan<sup>1</sup>, Sabrina Johnson<sup>1</sup>, Arnav Kaul<sup>1</sup>, Lorenzo Alvisi<sup>1</sup>, Luis Rodrigues<sup>2</sup>, and Robbert van Renesse<sup>1</sup>

<sup>1</sup> Cornell University
<sup>2</sup> INESC-ID, Instituto Superior Técnico, Universidade de Lisboa

## Requirements

Before building or running ZipLog, make sure you have:

- **A POSIX system** — the networking layer is built on standard BSD sockets (`sys/socket.h`), so any Linux (or macOS) host works. No special NICs or drivers are required.

## Dependencies

- **Compiler**: `g++` — the project uses C++17 and is built with GCC (see `Makefile`).
- **Build tools**: `make`, `curl` (used by the Makefile to fetch the vendored JSON header the first time you build).
- **Vendored dependencies** (fetched automatically by `make`, no manual setup required):
  - [`nlohmann/json`](https://github.com/nlohmann/json) — single-header JSON parser, downloaded into `third_party/json.hpp` for config file parsing.

### Installing dependencies (Ubuntu/Debian)

```bash
sudo apt install -y \
  build-essential \
  curl \
  git
```

## Building

```bash
make          # build main executable (./ziplog)
make clean    # remove obj/ and executables
```

## Service Descriptions

ZipLog is made up of four services — the **Zipper**, **storage servers**, **proxies**, and **subscribers** — plus a **client** used to append records. All of them are built into the single `ziplog` binary, dispatched by mode:

Clients send append requests to a proxy, which batches and replicates them in FIFO order to a quorum of storage servers for its shard. The Zipper measures each proxy's request rate and uses it to pre-assign and deterministically interleave log slots; storage servers then deliver the resulting totally-ordered log to subscribers.

### Zipper

- Global sequencer — allocates ordered sequence numbers to proxies based on their estimated request load, and manages proxy/subscriber membership and proxy failure recovery.
- Docs: [`docu/zipper.md`](docu/zipper.md).

### Servers

- The durable storage layer. Receive replicated batches from proxies, maintain an ordered message log, broadcast committed entries to subscribers, and participate in proxy failure recovery.
- Docs: [`docu/server.md`](docu/server.md).

### Proxies

- The main interface for appending records to the log. Accept client append requests, batch them, and replicate each batch to a quorum of storage servers in the sequence assigned by the Zipper.
- Docs: [`docu/proxy.md`](docu/proxy.md).

### Subscribers

- The main interface for consuming records from the log. Receive broadcasts from all servers and assemble them into a single, deduplicated, totally-ordered log once a quorum of servers has confirmed each entry.
- Docs: [`docu/subscriber.md`](docu/subscriber.md).

### Client

- Sends `APPEND` requests to a proxy. Used both interactively (`client` mode) and for benchmarking (`benchmark` mode, driven by `benchmark-f0.sh`/`benchmark-f1.sh` for multi-machine runs).

## Running Each Module

```bash
./ziplog zipper     config/setup1.json
./ziplog proxy      config/setup1.json 0
./ziplog server     config/setup1.json 0
./ziplog subscriber config/setup1.json 0
./ziplog client     config/setup1.json 0
```

`zipper` takes no ID; `proxy`, `server`, `subscriber`, and `client` all require the ID of the node (or, for `client`, the proxy) they correspond to within the config file's arrays.

`client` mode reads lines from stdin and sends each as an `APPEND`:

```bash
$ ./ziplog client config/setup1.json 0
Type string value to send APPENDs and hit Enter. Enter 'quit' to shutdown...
hello world
Sent successfully
quit
```

### Config File Format

```json
{
  "f": 0,
  "max_retries": 3,
  "max_epoch_history": 10,
  "epoch_duration": 150,
  "zipper": "127.0.0.1:8000",
  "proxies": ["127.0.0.1:8001"],
  "servers": ["127.0.0.1:8010"],
  "subscribers": ["127.0.0.1:8020"]
}
```

- Addresses are `"ip:port"` strings, not objects — all nodes must be on reachable addresses. For local development everything runs on `127.0.0.1`/`localhost` with different ports.
- `f` is the fault-tolerance factor; quorum size is `f + 1`, and `servers` must contain at least `2f + 1` entries.
- `max_epoch_history` and `epoch_duration` are optional and fall back to built-in defaults (`api/common.h`) if omitted.
- There is currently a single shard (shard id `0` is implicit) — the `shard_id` field some older configs reference is not read by the parser.

## Local Benchmark

There is currently no automated test suite — to exercise all modules together end-to-end, run `benchmark-local.sh`, which deploys a single zipper, proxy, server, and subscriber on `127.0.0.1`, then drives them with one or more concurrent `benchmark` clients:

```bash
./benchmark-local.sh                # defaults: 10 clients x 1000 commands each
./benchmark-local.sh 1000 10        # [num_commands] [num_clients]
```

It reports total throughput and per-command latency percentiles gathered from the subscriber's log. For multi-machine runs over `ssh`, see `benchmark-f0.sh`/`benchmark-f1.sh`.

## Tuning

| Parameter | Location | Effect |
|---|---|---|
| `epoch_duration` | config JSON | Length of one epoch, in `EpochDurationUnit` (microseconds). Shorter = lower latency, higher overhead |
| `max_epoch_history` | config JSON / `ProxyConfig` | More history = smoother slot estimates, slower adaptation to load spikes |
| `f` | config JSON | Fault tolerance — quorum is `f + 1`. Must have at least `2f + 1` servers |

# Acknowledgments

This work was co-funded by the European Union through the Lisboa 2030 Programme (ERDF) and by national funds through FCT, I.P., under projects no. 16539, UID/50021/2025, UID/PRR/50021/2025, and LISBOA2030-FEDER-00771200 and under grant UI/BD/153590/2022.
