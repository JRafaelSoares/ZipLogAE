## Subscriber

### Responsibility
A Subscriber reads the committed log by receiving broadcast messages from all Servers and assembling them into a globally ordered, deduplicated log. It waits for a quorum of servers to deliver each sequence number before applying it.

### Configuration (`SubscriberConfig`)
| Field | Type | Description |
|---|---|---|
| `zipper` | `Address` | Zipper address (for registration) |
| `servers` | `vector<Address>` | Server addresses to connect to |
| `shard_id` | `ShardId` | Shard this subscriber reads from |

### Lifecycle
```cpp
SubscriberConfig cfg = ...;
Subscriber sub(cfg);          // connects to servers, starts listening

sub.wait_for_log_size(10);    // blocks until 10 entries committed
const LogTracker& log = sub.log();
```

### Components

#### `LogTracker`
Tracks received messages across servers and assembles the final log once quorum is reached.

| Method | Description |
|---|---|
| `observe(sender, seq, data, quorum)` | Records a message from one server; applies it once `quorum` distinct servers have delivered the same seq |
| `wait_for_size(n)` | Blocks until `n` entries have been committed to the log |
| `wait_for_seq(seq)` | Blocks until a specific sequence number has been committed |
| `expand_unraveled()` | Returns `vector<vector<Command>>` — each inner vector is one committed batch, SKIPs excluded |
| `raw()` | Returns the raw committed log as `vector<Command>` including SKIP entries |
| `size()` | Returns number of committed entries |
| `next_seq()` | Returns the next expected sequence number |

Internally, `LogTracker` maintains:
- `log_` — committed entries in order
- `out_of_order_` — entries received out of order, applied when gap is filled
- `pending_quorum_` — per-sequence server delivery tracking
- `applied_` — set of already-committed sequence numbers (deduplication)

### Log Shape
After calling `expand_unraveled()`, the result is indexed by sequence number. Each entry is a batch (a `vector<Command>`) from one proxy's `send_out_batch()` call. SKIPs are excluded — they appear as empty inner vectors and are filtered out.

```cpp
vector<vector<Command>> log = sub.log().expand_unraveled();
// log[0] = first committed batch (one or more commands)
// log[1] = second committed batch
// ...
```

---

## Key Invariants

- A server never acks an `APPEND` or `SKIP` until it has durably stored the message
- A server only broadcasts to subscribers after storing — subscribers never see uncommitted data
- `ProxyLivenessTracker` only reports a proxy after it has missed sequences past the timeout window — transient delays do not trigger false positives
- `LogTracker` never applies a sequence number twice — `applied_` provides idempotent deduplication across duplicate server broadcasts
- Out-of-order delivery is handled transparently — `LogTracker` buffers early arrivals and applies them in order as gaps are filled
- A subscriber never reads a sequence until a quorum of servers have confirmed it — this provides the same consistency guarantee as the replication layer