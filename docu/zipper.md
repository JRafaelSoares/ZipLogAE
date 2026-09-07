## Zipper

### Responsibility
The Zipper is the global sequencer for a shard. It allocates ordered sequence numbers to proxies based on their estimated request load, and manages proxy membership and failure recovery.

### Configuration (`ZipperConfig`)
| Field | Type | Description |
|---|---|---|
| `servers` | `vector<Address>` | Storage server addresses |
| `proxies` | `vector<Address>` | Initial proxy addresses |
| `subscribers` | `vector<Address>` | Subscriber addresses |
| `epoch_duration_ms` | `Timestamp` | Length of one epoch in milliseconds |
| `shard_id` | `ShardId` | Shard this zipper is responsible for |

### Lifecycle
```cpp
ZipperConfig cfg = ...;
Zipper zipper(cfg);   // starts listening + epoch timer automatically
// ...
zipper.shutdown();    // stops epoch timer, closes all connections
```

### Epoch Cycle
Managed by `EpochTimer`, which runs on its own thread with two trigger points:

**At 75% of epoch duration** — `allocate_slots()`:
- Reads each active proxy's estimated request count from `ProxyRegistry`
- Distributes sequence numbers evenly across the epoch window using interleaved timestamps
- Sends `ZIP_RESPONSE` to each proxy with their assigned `[timestamp, seq, ...]` pairs
- Broadcasts allocations to all servers
- Skips proxies in `RECONFIGURING` or `BLOCKED` state

**At 100% of epoch duration** — `introduce_proxies()`:
- Admits any proxies that completed registration during the epoch
- Adds them to `ProxyRegistry` and `config_.proxies`
- Sends `INCLUDE_PROXY` confirmation to each new proxy

### Messages Handled
| Type | Handler | Description |
|---|---|---|
| `ZIP_REQUEST` | `update_slot_estimate` | Proxy reports how many slots it needs next epoch |
| `REPORT` | `reconfig_manager_.handle_report` | Server reports a proxy as failed |
| `FREEZE_RESPONSE` | `reconfig_manager_.handle_freeze_response` | Server responds to freeze broadcast |
| `REGISTER_PROXY` | `add_proxy(msg, true)` | New proxy joining the system |
| `REJOIN_PROXY` | `add_proxy(msg, false)` | Previously failed proxy rejoining |
| `REGISTER_SUBSCRIBER` | `introduce_subscriber` | New subscriber joining |

### Proxy Registration Flow
1. Proxy sends `REGISTER_PROXY` with its `ip:port` as payload
2. Zipper broadcasts `INCLUDE_PROXY` to all servers and waits for quorum acks
3. On quorum — proxy is queued in `joining_proxies_`
4. At next epoch boundary — proxy is admitted, added to registry, sent `INCLUDE_PROXY`

### Proxy Failure & Reconfiguration
Managed by `ReconfigManager`:

1. Servers detect a failed proxy and send `REPORT` to the Zipper
2. Once a quorum of servers have reported the same proxy as failed:
   - Proxy status set to `RECONFIGURING`
   - Zipper broadcasts `FREEZE` to all servers
3. Servers respond with `FREEZE_RESPONSE` containing the last sequence number they saw from the proxy
4. If quorum responses **agree** on last sequence number:
   - Sends `SKIP` for any allocated-but-unused sequence numbers above last seen
   - Broadcasts `FREEZE_COMPLETE` to all servers
   - Proxy status set to `BLOCKED`
5. If quorum responses **disagree** → start a new freeze round with incremented round number
6. Proxy remains `BLOCKED` until it sends `REJOIN_PROXY`, which resets its state to `ACTIVE`

### Proxy State
Each proxy tracked in `ProxyRegistry` with a `ProxyState` struct:

| Field | Description |
|---|---|
| `address` | Proxy's `ip:port` |
| `status` | `ACTIVE`, `RECONFIGURING`, or `BLOCKED` |
| `estimate` | Slot count requested for current epoch |
| `allocated_sequences` | Sequences assigned this epoch (used for SKIP detection) |
| `freeze_round` | Current freeze round number |
| `freeze_responders` | Servers that have responded to current freeze |
| `reporters` | Servers that have reported this proxy as failed |
| `last_sequences` | Last sequence numbers reported by servers during freeze |
