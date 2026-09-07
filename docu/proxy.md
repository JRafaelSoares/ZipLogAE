## Proxy

### Responsibility
A Proxy accepts append requests from clients, batches them, and replicates each batch to a quorum of storage servers in the globally ordered sequence assigned by the Zipper.

### Configuration (`ProxyConfig`)
| Field | Type | Description |
|---|---|---|
| `servers` | `vector<Address>` | Storage server addresses |
| `zipper` | `Address` | Zipper address |
| `epoch_duration_ms` | `Timestamp` | Must match Zipper's epoch duration |
| `max_epoch_history` | `size_t` | Epochs of history used for slot estimate |
| `shard_id` | `ShardId` | Shard this proxy belongs to |

### Lifecycle
```cpp
// Pre-registered proxy — starts immediately
ProxyConfig cfg = ...;
Proxy proxy(cfg);

// Joining proxy — must register with Zipper first
Proxy proxy(cfg, false);
// epoch timer starts automatically after INCLUDE_PROXY is received
```

### Epoch Cycle
Managed by `ProxyEpochTimer` on its own thread with two trigger conditions:

**Every `epoch_duration_ms`** — `update_slot_estimate()`:
- Computes rolling average of `request_count` over last `max_epoch_history` epochs
- Resets `request_count` to 0
- Sends `ZIP_REQUEST` to Zipper with the computed estimate

**When `now_ms() >= next_send`** — `send_out_batch()`:
- Pops next `(seq, timestamp)` pair from `SlotScheduler`
- Drains client buffers round-robin up to 60KB via `ClientBufferManager::drain_batch`
- If no pending data → message type set to `SKIP`, empty payload
- If data available → message type set to `APPEND`, serialized `CommandBatch` as payload
- Replicates via `BatchReplicator` — blocks until f+1 server acks received
- Sends `SUCCESS` or `FAILURE` back to all participating client sockets

### Messages Handled
| Type | Action |
|---|---|
| `APPEND` | Push command into client's `CircularBuffer`, increment request count |
| `ZIP_RESPONSE` | Load `[timestamp, seq, ...]` pairs into `SlotScheduler` |
| `INCLUDE_PROXY` | Set `registered_ = true`, start epoch timer |
| `FREEZE` | Set `registered_ = false`, pause epoch timer, call `attempt_join(false)` |

### Components

#### `SlotScheduler`
Manages allocated sequence numbers, send timestamps, and rolling slot estimates.

| Method | Description |
|---|---|
| `load_slots(ordering_values)` | Loads interleaved `[timestamp, seq, ...]` from `ZIP_RESPONSE` |
| `record_request()` | Increments request count for current epoch |
| `compute_estimate()` | Rolls history, returns new estimate, resets count |
| `pop_next_slot(seq, time)` | Pops next `(seq, timestamp)` pair, returns false if empty |
| `next_send()` | Returns timestamp of next scheduled send, 0 if no slots |

#### `BatchReplicator`
Maintains one persistent thread and socket per storage server.

| Method | Description |
|---|---|
| `start()` | Spawns one `ServerWorker` thread per server, establishes persistent connections |
| `replicate(msg)` | Pushes message to all worker queues, blocks until f+1 acks received |
| `shutdown()` | Signals all workers to stop, joins threads |

Each `ServerWorker`:
- Holds a persistent TCP connection to its assigned server
- Waits on a queue for `WorkItem`s
- Sends message, waits for ack, signals shared `ReplicationState`
- On send/recv failure — marks itself as shutdown

#### `ClientBufferManager`
Per-socket `CircularBuffer<PendingRequest>` with blocking push.

| Method | Description |
|---|---|
| `push(socket, cmd)` | Blocking push into socket's buffer — blocks if buffer full |
| `remove(socket)` | Removes buffer when client disconnects |
| `drain_batch()` | Round-robin drain across all buffers up to 60KB, returns `(CommandBatch, set<int>)` |
| `buffer_size(socket)` | Returns current size of a socket's buffer |

#### `ProxyEpochTimer`
Runs on its own thread, triggers epoch callbacks independently of message handling.

| Method | Description |
|---|---|
| `start()` | Starts the timer thread |
| `stop()` | Stops and joins the timer thread |
| `pause()` | Suppresses callbacks without stopping the thread (used during FREEZE) |
| `resume()` | Re-enables callbacks after rejoin |
