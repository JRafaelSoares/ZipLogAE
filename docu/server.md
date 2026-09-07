## Server

### Responsibility
A Server is the durable storage layer for a shard. It receives replicated batches from Proxies, maintains an ordered message log, broadcasts committed entries to Subscribers, and participates in proxy failure recovery by tracking liveness and coordinating freeze/reconfiguration with the Zipper.

### Configuration (`ServerConfig`)
| Field | Type | Description |
|---|---|---|
| `zipper` | `Address` | Zipper address (for sending REPORT) |
| `proxies` | `vector<Address>` | Initial proxy addresses |
| `servers` | `vector<Address>` | Other server addresses (for TRANSFER_REQUEST during freeze) |
| `subscribers` | `vector<Address>` | Initial subscriber addresses |
| `epoch_duration_ms` | `Timestamp` | Used to set liveness timeout windows |
| `shard_id` | `ShardId` | Shard this server belongs to |

### Lifecycle
```cpp
ServerConfig cfg = ...;
Server server(cfg);   // starts listening automatically
// ...
server.shutdown();
```

### Messages Handled
| Type | Handler | Description |
|---|---|---|
| `APPEND` | store + broadcast + ack | Stores batch, removes liveness timeout, broadcasts to subscribers |
| `SKIP` | store + remove_timeout + ack | Stores gap-filler, removes liveness timeout |
| `ZIP_RESPONSE` | `liveness_.update_timeouts` | Records which sequences a proxy was assigned this epoch |
| `FREEZE` | `freeze_handler_.handle_freeze` | Participates in reconfiguration protocol |
| `TRANSFER_REQUEST` | `freeze_handler_.handle_transfer_request` | Shares stored messages with peer servers during freeze |
| `FREEZE_COMPLETE` | `liveness_.block_proxy` | Marks proxy as blocked, stops expecting its sequences |
| `INCLUDE_PROXY` | `introduce_proxy` + ack | Registers a new or rejoining proxy, unblocks it in liveness tracker |
| `INCLUDE_SUBSCRIBER` | `introduce_subscriber` + ack | Registers a new subscriber for broadcast |

### Components

#### `MessageStore`
Append-only per-proxy message log stored in memory.

| Method | Description |
|---|---|
| `store(proxy_id, msg)` | Appends message to proxy's deque |
| `get(proxy_id)` | Returns all stored messages for a proxy |
| `snapshot()` | Returns full copy of the log map |
| `has(proxy_id)` | Returns true if any messages stored for proxy |
| `mutex()` | Exposes internal mutex for coordinated access |

#### `ProxyLivenessTracker`
Runs a background failure detection thread. Tracks expected sequence arrivals and reports overdue proxies to the Zipper.

| Method | Description |
|---|---|
| `update_timeouts(proxy_id, ordering_values)` | On `ZIP_RESPONSE` — records assigned sequences and their expected arrival times |
| `remove_timeout(proxy_id, seq)` | On `APPEND`/`SKIP` — clears a sequence from the pending timeout set |
| `block_proxy(proxy_id)` | On `FREEZE_COMPLETE` — stops tracking sequences for this proxy |
| `unblock_proxy(proxy_id)` | On `INCLUDE_PROXY` rejoin — resumes tracking |
| `set_reconfiguring(proxy_id)` | Suppresses reporting while freeze is in progress |
| `is_blocked(proxy_id)` | Returns true if proxy is in BLOCKED state |
| `last_seq(proxy_id)` | Returns last sequence number seen from a proxy |

The detection thread wakes every `epoch_duration_ms`, checks all pending timeouts, and sends `REPORT` to the Zipper for any proxy whose sequences are overdue.

#### `SubscriberBroadcaster`
Fire-and-forget per-subscriber worker threads.

| Method | Description |
|---|---|
| `start(subscribers)` | Spawns one worker thread per subscriber |
| `broadcast(msg)` | Pushes message to all worker queues |
| `add_subscriber(idx, addr)` | Adds a late-joining subscriber |
| `shutdown()` | Signals all workers to stop, joins threads |

Each worker holds a persistent connection and sends messages in order from its queue. Delivery is best-effort — a failed subscriber does not block others.

#### `FreezeHandler`
Coordinates the server's role in proxy reconfiguration.

| Method | Description |
|---|---|
| `handle_freeze(msg, from_zipper)` | Marks proxy as reconfiguring, collects missing messages from peer servers via `TRANSFER_REQUEST`, then sends `FREEZE_RESPONSE` to Zipper |
| `handle_transfer_request(socket, msg)` | Sends stored messages above `req_last_seq` to requesting peer, acks |

Freeze flow per server:
1. Receives `FREEZE` from Zipper with proxy ID and freeze round
2. Calls `liveness_.set_reconfiguring(proxy_id)`
3. Sends `TRANSFER_REQUEST` to all peer servers to collect any messages it may have missed
4. Once transfers complete, sends `FREEZE_RESPONSE` to Zipper with its last known sequence for that proxy
