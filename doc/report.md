# DHT Implementation Report

## Architecture

This project implements two classic Distributed Hash Table protocols — **Chord** and **Kademlia** — under a shared `DhtNode` interface defined in `node/interface.go`. Both protocols use SHA-1 to map node addresses and keys into a 160-bit identifier space, and communicate via Go's `net/rpc` over TCP.

```
node/interface.go          — DhtNode interface (Run, Create, Join, Put, Get, Delete, Quit, ForceQuit)
├── chord/chord.go         — Chord protocol implementation
└── kademlia/kademlia.go   — Kademlia protocol implementation
```

The `factory.go` selects which protocol to use at compile time via import. Tests in `node/` exercise each protocol through the common interface, using disjoint port ranges to allow parallel execution.

### Chord

Chord organizes nodes into a **circular identifier ring**. Each node maintains:

- **Successor / Predecessor**: immediate neighbors on the ring.
- **Successor List** (length 4): provides fault tolerance — if the primary successor fails, the node falls back to the next live successor.
- **Finger Table** (160 entries): enables O(log N) lookups via `closestPrecedingFinger`.

Key-value pairs are stored on the successor of `hash(key)`, with replication to SUC_LIST_LEN consecutive successors. A `stabilize()` goroutine runs every 200ms to repair successor links after node joins or failures. A `fixFingers()` goroutine runs every 50ms to keep the finger table up to date. A `pushCopies()` goroutine runs every 500ms to ensure data replicas are consistent with the current successor list.

On `Quit()`, the node transfers its data to its live successor and updates predecessor/successor pointers to heal the ring.

### Kademlia

Kademlia organizes nodes using a **binary trie** structure via 160 k-buckets, each holding up to K=7 contacts with a shared prefix length. The core operations are:

- **Iterative Node Lookup** (`findNode`): recursively queries up to ALPHA=3 nodes per round, converging to the K closest nodes in O(log N) rounds.
- **Iterative Value Lookup** (`findValue`): same as above, but stops early when a node returns the value and caches it back to the K closest nodes.
- **Iterative Delete**: iteratively discovers and removes the key from all reaching the K closest nodes.

Bucket maintenance follows the Kademlia paper: when a bucket is full, the least-recently-seen entry is pinged; if dead, it is evicted and replaced; if alive, the new entry is discarded.

Each node periodically performs a `findNode` with a random target ID every 5 seconds to refresh its routing table and purge stale contacts.

## Innovations & Features

1. **Version-based Concurrency Control**: Both `Put` and `Delete` in Kademlia use nanosecond timestamps as version numbers. Store operations with lower versions are rejected, preventing stale data from overwriting fresh data during concurrent updates.

2. **Delete in Kademlia (Bonus)**: A full iterative deletion procedure is implemented, finding and removing keys from all K closest nodes. The deletion propagates version information to prevent re-insertion of stale data.

3. **Graceful Quit with Data Preservation**: Both protocols redistribute locally stored key-value pairs to responsible nodes before shutting down, minimizing data loss.

4. **Multi-layer Fault Tolerance in Chord**: The successor list provides fallback paths beyond the immediate successor. If both successor list and finger table contain dead nodes, `findFirstLiveSuccessor()` explores both to locate a live peer.

5. **In-process Testing with Color Output**: The test harness uses disjoint port ranges, colored pass/fail metrics, and configurable failure rate thresholds, enabling rapid iteration.

6. **Data Replication in Chord**: Keys are replicated to SUC_LIST_LEN (4) consecutive successors, tolerating up to 3 simultaneous node failures without data loss.

## Test Results

| Test | Protocol | Description |
|------|----------|-------------|
| TestBasic | Chord / Kademlia | 5 rounds of join, put, get, delete, quit with 100 nodes |
| TestForceQuit | Chord / Kademlia | 50 nodes, 9 rounds of force-quit with full data integrity checks |
| TestQuitAndStabilize | Chord / Kademlia | Gradual quit of all 50 nodes with continuous get verification |
| TestDelete | Chord / Kademlia | Complete delete lifecycle: put, delete, verify removal, re-delete |

## References

- Stoica, I., et al. "Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications." *SIGCOMM 2001*.
- Maymounkov, P., & Mazières, D. "Kademlia: A Peer-to-peer Information System Based on the XOR Metric." *IPTPS 2002*.
- Go `net/rpc` documentation: https://pkg.go.dev/net/rpc
