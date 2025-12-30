# Live Mind Map Broadcasting System Design

## Problem Statement

Design a real-time mind map collaboration platform where:
- **Creators** design workflows (nodes + connectors) in a live streaming manner
- **Followers** (up to 100k per creator) see changes in real-time
- Changes must propagate with < 500ms latency
- Support for poor network conditions with proper action sequencing
- Undo/Redo capability for creators

---

## Requirements Breakdown

### Functional Requirements
| Requirement | Details |
|-------------|---------|
| Workflow Creation | Nodes and Connectors (graph-like, no validation rules) |
| Live Sharing | Followers see real-time updates |
| Undo/Redo | Creator can undo/redo actions |
| Persistence | Workflows should be saved and recoverable |

### Non-Functional Requirements
| Metric | Target |
|--------|--------|
| Latency | < 500ms for action visibility |
| Followers per Creator | ≤ 100k |
| Concurrent Live Creators | ~1k |
| Network Resilience | Handle disconnects, proper sequencing |

### Scale Calculations
```
Peak concurrent viewers = 1k creators × 100k followers = 100M connections (worst case)
More realistic: 1k creators × avg 10k followers = 10M concurrent connections
Actions per second per creator: ~2-5 (human editing speed)
Total actions/sec = 1k × 5 = 5k actions/sec to broadcast
Each action fans out to avg 10k followers = 50M message deliveries/sec
```

---

## Data Model

### Core Entities

```
┌─────────────────────────────────────────────────────────────────┐
│                         WORKFLOW                                 │
├─────────────────────────────────────────────────────────────────┤
│  workflow_id: UUID (PK)                                          │
│  creator_id: UUID (FK)                                           │
│  title: String                                                   │
│  is_live: Boolean                                                │
│  current_version: Integer                                        │
│  created_at: Timestamp                                           │
│  updated_at: Timestamp                                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ 1:N
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                           NODE                                   │
├─────────────────────────────────────────────────────────────────┤
│  node_id: UUID (PK)                                              │
│  workflow_id: UUID (FK)                                          │
│  label: String                                                   │
│  x_position: Float                                               │
│  y_position: Float                                               │
│  width: Float                                                    │
│  height: Float                                                   │
│  style: JSON (color, shape, etc.)                                │
│  created_at: Timestamp                                           │
│  is_deleted: Boolean (soft delete for undo)                      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        CONNECTOR                                 │
├─────────────────────────────────────────────────────────────────┤
│  connector_id: UUID (PK)                                         │
│  workflow_id: UUID (FK)                                          │
│  source_node_id: UUID (FK)                                       │
│  target_node_id: UUID (FK)                                       │
│  label: String (optional)                                        │
│  style: JSON (line style, arrow type, etc.)                      │
│  is_deleted: Boolean (soft delete for undo)                      │
└─────────────────────────────────────────────────────────────────┘
```

### Action/Operation Model (Event Sourcing)

```json
{
  "action_id": "uuid",
  "workflow_id": "uuid",
  "creator_id": "uuid",
  "sequence_number": 42,
  "timestamp": "2025-01-15T10:30:00Z",
  "action_type": "NODE_CREATE | NODE_UPDATE | NODE_DELETE | CONNECTOR_CREATE | CONNECTOR_UPDATE | CONNECTOR_DELETE",
  "payload": {
    "node_id": "uuid",
    "changes": { "x_position": 100, "y_position": 200 }
  },
  "inverse_action": { /* for undo support */ }
}
```

---

## High-Level Architecture

**Protocol Choice: WebSocket**
- Long polling discarded: Too high latency (~100-500ms per poll cycle), doesn't meet <500ms requirement
- SSE discarded: Viewers need to send SUBSCRIBE and CATCH_UP requests, not purely read-only
- WebSocket: Bidirectional, low latency (~10-50ms), persistent connection, fits both creator and viewer needs

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                   CLIENTS                                        │
├──────────────────────────────┬──────────────────────────────────────────────────┤
│       Creator Client         │              Viewer Clients (100k)                │
│   (WebSocket bidirectional)  │           (WebSocket bidirectional)               │
└──────────────┬───────────────┴─────────────────────────┬────────────────────────┘
               │                                          │
               │ WebSocket                                │ WebSocket
               ▼                                          ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            LOAD BALANCER (L7)                                     │
│           (No sticky sessions needed - Redis Pub/Sub handles routing)            │
└──────────────────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      CONNECTION GATEWAY TIER                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │  Gateway 1  │  │  Gateway 2  │  │  Gateway 3  │  │  Gateway N  │   ...        │
│  │   (50k      │  │   (50k      │  │   (50k      │  │   (50k      │              │
│  │   conns)    │  │   conns)    │  │   conns)    │  │   conns)    │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
└─────────┼────────────────┼────────────────┼────────────────┼─────────────────────┘
          │                │                │                │
          └────────────────┴────────────────┴────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           MESSAGE BROKER                                          │
│                    (Redis Pub/Sub or Kafka)                                       │
│                                                                                   │
│   Topic: workflow:{workflow_id}                                                   │
│   Partitioned by workflow_id for ordering guarantees                              │
└──────────────────────────────────────────────────────────────────────────────────┘
          │                                    │
          ▼                                    ▼
┌────────────────────────┐      ┌────────────────────────────────────────────────┐
│    ACTION PROCESSOR    │      │              STATE MANAGER                      │
│                        │      │                                                 │
│  - Validates actions   │      │  - Maintains workflow state in Redis            │
│  - Assigns seq numbers │      │  - Handles undo/redo stack                      │
│  - Persists to DB      │      │  - Serves initial state to new viewers          │
│  - Publishes to broker │      │                                                 │
└────────────────────────┘      └────────────────────────────────────────────────┘
          │                                    │
          ▼                                    ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           PERSISTENCE LAYER                                       │
├─────────────────────────┬────────────────────────────────────────────────────────┤
│     PostgreSQL          │                  Redis                                  │
│                         │                                                         │
│  - Workflows            │  - Live workflow state (hot)                            │
│  - Nodes & Connectors   │  - Undo/Redo stacks                                     │
│  - Action Event Log     │  - Session management                                   │
│  - User data            │  - Pub/Sub channels                                     │
│                         │  - Connection registry                                  │
└─────────────────────────┴────────────────────────────────────────────────────────┘
```

---

## Deep Dive: Data Storage Architecture

### Where is Data Stored?

We use a **polyglot persistence** approach — different stores for different purposes:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                    DATA STORAGE TOPOLOGY (Kafka-First)                               │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐ │
│  │                      KAFKA (Commit Log - Source of Truth)                        │ │
│  │                                                                                  │ │
│  │  PURPOSE: Durable commit log, ACK to creator, enables replay                    │ │
│  │  WRITE: Synchronous (acks=all), blocks until confirmed                          │ │
│  │                                                                                  │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐          │ │
│  │  │  topic: mindmap.actions                                           │          │ │
│  │  │  partitioned by: workflow_id (ordering guarantee)                 │          │ │
│  │  │  retention: 7 days                                                │          │ │
│  │  └──────────────────────────────────────────────────────────────────┘          │ │
│  └─────────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                              │
│                    ┌───────────────────┴───────────────────┐                         │
│                    │                                       │                         │
│                    ▼                                       ▼                         │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐         │
│  │  Consumer Group: redis-applier  │    │  Consumer Group: pg-applier     │         │
│  └─────────────────┬───────────────┘    └─────────────────┬───────────────┘         │
│                    │                                       │                         │
│                    ▼                                       ▼                         │
│  ┌─────────────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │         REDIS (Hot Layer)           │  │      POSTGRESQL (Cold Layer)        │   │
│  │                                     │  │                                     │   │
│  │  PURPOSE: Real-time, low latency    │  │  PURPOSE: Durability, queries       │   │
│  │  LATENCY: < 1ms                     │  │  LATENCY: 5-50ms (batched)          │   │
│  │                                     │  │                                     │   │
│  │  ┌─────────────┐ ┌─────────────┐   │  │  ┌─────────────┐ ┌─────────────┐    │   │
│  │  │  Pub/Sub    │ │ State Cache │   │  │  │  workflows  │ │   nodes     │    │   │
│  │  │  (broadcast)│ │             │   │  │  │             │ │ connectors  │    │   │
│  │  └─────────────┘ └─────────────┘   │  │  └─────────────┘ └─────────────┘    │   │
│  │                                     │  │                                     │   │
│  │  ┌─────────────┐ ┌─────────────┐   │  │  ┌─────────────┐ ┌─────────────┐    │   │
│  │  │Action Buffer│ │ Undo/Redo   │   │  │  │ action_log  │ │   users     │    │   │
│  │  │(catch-up)   │ │ Stacks      │   │  │  │             │ │             │    │   │
│  │  └─────────────┘ └─────────────┘   │  │  └─────────────┘ └─────────────┘    │   │
│  └─────────────────────────────────────┘  └─────────────────────────────────────┘   │
│                                                                                       │
│  KEY: Kafka is written FIRST (sync). Consumers populate Redis & Postgres in parallel.│
│       If Postgres is down, Redis consumer still works → viewers served.              │
│       Failures are isolated by data store.                                           │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### Core Invariants (Event Sourcing Guarantees)

These invariants ensure correctness under partial failures, consumer lag, and restarts:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         EVENT SOURCING INVARIANTS                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  1. SINGLE AUTHORITATIVE LOG                                                        │
│     ────────────────────────                                                         │
│     Kafka topic (partitioned by workflow_id) is the ONLY source of truth.           │
│     Every action has a unique (workflow_id, seq_num) pair.                          │
│     Ordering is guaranteed within a partition.                                      │
│                                                                                      │
│  2. MATERIALIZED VIEWS WITH WATERMARKS                                              │
│     ────────────────────────────────────                                             │
│     Redis state and Postgres tables are DERIVED views, not independent stores.      │
│     Each view tracks its applied_seq watermark:                                     │
│                                                                                      │
│     Redis:    workflow:state:{id}.last_seq_num = 8123                               │
│     Postgres: SELECT MAX(sequence_number) FROM action_log WHERE workflow_id = X     │
│                                                                                      │
│     This watermark answers: "This state reflects all actions up to seq 8123"        │
│                                                                                      │
│  3. SNAPSHOTS TIED TO SEQUENCE NUMBERS                                              │
│     ──────────────────────────────────                                               │
│     Periodic snapshots store: { state: {...}, as_of_seq: 8123 }                     │
│     To rebuild: Load snapshot → replay log from as_of_seq+1 to current              │
│                                                                                      │
│  4. IDEMPOTENT CONSUMERS                                                            │
│     ─────────────────────                                                            │
│     Consumers check: if action.seq <= applied_seq, skip (already applied)           │
│     Safe to replay, restart, or re-deliver messages.                                │
│                                                                                      │
│  5. STATE IS ALWAYS REBUILDABLE                                                     │
│     ─────────────────────────────                                                    │
│     If Redis state is corrupted or lost:                                            │
│       1. Find latest snapshot (or start from empty state)                           │
│       2. Replay all actions from Kafka starting at snapshot.as_of_seq + 1           │
│       3. State is now correct and up-to-date                                        │
│                                                                                      │
│     This is the key guarantee: Log + Snapshot = Recoverable State                   │
│                                                                                      │
│  6. WRITE PATH CORRECTNESS                                                          │
│     ───────────────────────                                                          │
│     Creator action → Kafka (sync, acks=all) → ACK to creator                        │
│     ACK means: "Action is in the log. Materialized views WILL reflect it."          │
│     Consumers may lag, but they will eventually apply it.                           │
│                                                                                      │
│  7. READ PATH CORRECTNESS                                                           │
│     ──────────────────────                                                           │
│     New viewer joins:                                                               │
│       1. Read snapshot from Redis (state + last_seq_num)                            │
│       2. Subscribe to Pub/Sub for future actions                                    │
│       3. If received action.seq > last_seq_num + 1: request catch-up from buffer   │
│       4. Viewer state = snapshot + replayed catch-up + live stream                  │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Why This Matters**: Without these invariants, partial failures (e.g., Redis updated but Pub/Sub missed, or consumer lag) can cause state/log divergence. With these invariants, any divergence is detectable (via sequence numbers) and recoverable (via replay).

### Redis Data Structures in Detail

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                           REDIS KEY PATTERNS                                          │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  1. WORKFLOW STATE (Hash) - Materialized View with Watermark                        │
│     Key: workflow:state:{workflow_id}                                                │
│     ┌────────────────────────────────────────────────────────────────┐              │
│     │  Field          │  Value                                       │              │
│     ├─────────────────┼──────────────────────────────────────────────┤              │
│     │  nodes          │  JSON: [{"id":"n1","x":100,"y":50,...},...]  │              │
│     │  connectors     │  JSON: [{"id":"c1","src":"n1","tgt":"n2"}]   │              │
│     │  applied_seq    │  8123  ← WATERMARK: state reflects log up to │              │
│     │                 │         this seq (invariant for correctness) │              │
│     │  is_live        │  true                                        │              │
│     │  updated_at     │  1704067200                                  │              │
│     └─────────────────┴──────────────────────────────────────────────┘              │
│     TTL: None (persisted while workflow exists)                                      │
│                                                                                       │
│  1b. PERIODIC SNAPSHOTS (Hash) - For faster recovery                                │
│     Key: workflow:snapshot:{workflow_id}                                             │
│     ┌────────────────────────────────────────────────────────────────┐              │
│     │  Field          │  Value                                       │              │
│     ├─────────────────┼──────────────────────────────────────────────┤              │
│     │  nodes          │  JSON: [full node list at snapshot time]     │              │
│     │  connectors     │  JSON: [full connector list at snapshot time]│              │
│     │  as_of_seq      │  8000  ← Snapshot valid as of this seq       │              │
│     │  created_at     │  1704060000                                  │              │
│     └─────────────────┴──────────────────────────────────────────────┘              │
│     To rebuild: Load snapshot → replay log from as_of_seq+1 to current              │
│                                                                                       │
│  2. ACTION BUFFER (Sorted Set) - For catch-up requests                               │
│     Key: workflow:actions:{workflow_id}                                              │
│     ┌────────────────────────────────────────────────────────────────┐              │
│     │  Score (seq_num)  │  Member (action JSON)                      │              │
│     ├───────────────────┼────────────────────────────────────────────┤              │
│     │  40               │  {"type":"NODE_CREATE","payload":{...}}    │              │
│     │  41               │  {"type":"NODE_UPDATE","payload":{...}}    │              │
│     │  42               │  {"type":"CONNECTOR_CREATE","payload":{...}}│              │
│     └───────────────────┴────────────────────────────────────────────┘              │
│     TTL: 1 hour (only needed for recent catch-up)                                    │
│                                                                                       │
│  3. UNDO STACK (List)                                                                │
│     Key: workflow:undo:{workflow_id}                                                 │
│     [Action5, Action4, Action3, Action2, Action1] ← LIFO                            │
│     Max length: 100 (LTRIM)                                                          │
│                                                                                       │
│  4. PUB/SUB CHANNEL (Ephemeral)                                                      │
│     Channel: workflow:{workflow_id}                                                  │
│     Messages are fire-and-forget, not stored                                         │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Call Flow: Node Addition (Complete Data Flow)

This is the **complete journey** of a "Create Node" action from creator click to viewer render:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                    NODE ADDITION - COMPLETE CALL FLOW                                 │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  STEP 1: Creator Initiates Action                                                    │
│  ════════════════════════════════                                                    │
│                                                                                       │
│  Creator UI                                                                          │
│     │                                                                                │
│     │  User clicks "Add Node" at position (200, 150)                                │
│     │                                                                                │
│     ▼                                                                                │
│  ┌─────────────────────────────────────────┐                                        │
│  │  Creator Client (Browser)               │                                        │
│  │                                          │                                        │
│  │  1. Generate local action:               │                                        │
│  │     {                                    │                                        │
│  │       localId: "local-uuid-123",         │                                        │
│  │       type: "NODE_CREATE",               │                                        │
│  │       payload: {                         │                                        │
│  │         node_id: "node-uuid-456",        │                                        │
│  │         label: "New Idea",               │                                        │
│  │         x: 200, y: 150,                  │                                        │
│  │         style: { color: "#3498db" }      │                                        │
│  │       }                                  │                                        │
│  │     }                                    │                                        │
│  │                                          │                                        │
│  │  2. Apply OPTIMISTICALLY to local state  │                                        │
│  │  3. Render node immediately (no wait)    │                                        │
│  │  4. Add to pendingActions map            │                                        │
│  │  5. Send via WebSocket                   │                                        │
│  └──────────────────┬──────────────────────┘                                        │
│                     │                                                                │
│                     │ WebSocket (binary/msgpack)                                     │
│                     │ ~10-30ms network latency                                       │
│                     ▼                                                                │
│                                                                                       │
│  STEP 2: Gateway Receives & Routes                                                   │
│  ═══════════════════════════════════                                                │
│                                                                                       │
│  ┌─────────────────────────────────────────┐                                        │
│  │  Gateway Server (Creator's Gateway)      │                                        │
│  │                                          │                                        │
│  │  1. Authenticate creator (JWT validation)│                                        │
│  │  2. Validate creator owns this workflow  │                                        │
│  │  3. Forward to Action Processor          │                                        │
│  └──────────────────┬──────────────────────┘                                        │
│                     │                                                                │
│                     │ Internal RPC (gRPC) ~1-2ms                                     │
│                     ▼                                                                │
│                                                                                       │
│  STEP 3: Action Processor (Core Logic)                                               │
│  ═══════════════════════════════════════                                            │
│                                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐│
│  │  Action Processor Service                                                        ││
│  │                                                                                  ││
│  │  ┌────────────────────────────────────────────────────────────────────────────┐ ││
│  │  │  3a. ASSIGN SEQUENCE NUMBER (Atomic)                                       │ ││
│  │  │                                                                            │ ││
│  │  │  // Redis atomic increment                                                 │ ││
│  │  │  seq_num = INCR workflow:seq:{workflow_id}  // Returns: 43                 │ ││
│  │  │                                                                            │ ││
│  │  │  enrichedAction = {                                                        │ ││
│  │  │    ...originalAction,                                                      │ ││
│  │  │    sequence_number: 43,                                                    │ ││
│  │  │    server_timestamp: "2025-01-15T10:30:00.123Z",                           │ ││
│  │  │    creator_id: "creator-uuid-789"                                          │ ││
│  │  │  }                                                                         │ ││
│  │  └────────────────────────────────────────────────────────────────────────────┘ ││
│  │                                         │                                        ││
│  │         ┌───────────────────────────────┼───────────────────────────────┐       ││
│  │         │                               │                               │       ││
│  │         ▼                               ▼                               ▼       ││
│  │  ┌─────────────┐               ┌─────────────┐               ┌─────────────┐   ││
│  │  │ 3b. REDIS   │               │ 3c. REDIS   │               │ 3d. REDIS   │   ││
│  │  │ State Cache │               │ Action Buf  │               │ Pub/Sub     │   ││
│  │  │             │               │             │               │             │   ││
│  │  │ HSET        │               │ ZADD        │               │ PUBLISH     │   ││
│  │  │ workflow:   │               │ workflow:   │               │ workflow:   │   ││
│  │  │ state:{id}  │               │ actions:{id}│               │ {id}        │   ││
│  │  │             │               │ 43 <action> │               │ <action>    │   ││
│  │  │ Update      │               │             │               │             │   ││
│  │  │ nodes array │               │ TTL: 1 hour │               │ Fire&Forget │   ││
│  │  └─────────────┘               └─────────────┘               └──────┬──────┘   ││
│  │         │                               │                           │          ││
│  │         │ ~1ms                          │ ~1ms                      │ ~1ms     ││
│  │         │                               │                           │          ││
│  └─────────┼───────────────────────────────┼───────────────────────────┼──────────┘│
│            │                               │                           │           │
│            │                               │                           │           │
│  ┌─────────┼───────────────────────────────┼───────────────────────────┼──────────┐│
│  │         ▼                               ▼                           │          ││
│  │  ┌─────────────────────────────────────────────────────────────┐   │          ││
│  │  │ 3e. POSTGRESQL (Async Write - Non-blocking)                 │   │          ││
│  │  │                                                             │   │          ││
│  │  │  // Batched async write (doesn't block the real-time flow)  │   │          ││
│  │  │                                                             │   │          ││
│  │  │  INSERT INTO action_log (workflow_id, sequence_number,      │   │          ││
│  │  │    action_type, payload, creator_id, created_at)            │   │          ││
│  │  │  VALUES ('wf-123', 43, 'NODE_CREATE', '{...}', 'cr-789',    │   │          ││
│  │  │    '2025-01-15 10:30:00');                                  │   │          ││
│  │  │                                                             │   │          ││
│  │  │  INSERT INTO nodes (id, workflow_id, label, x, y, ...)      │   │          ││
│  │  │  VALUES ('node-456', 'wf-123', 'New Idea', 200, 150, ...);  │   │          ││
│  │  │                                                             │   │          ││
│  │  │  // Also push to undo stack in same transaction             │   │          ││
│  │  │  LPUSH workflow:undo:{id} <action_with_inverse>             │   │          ││
│  │  └─────────────────────────────────────────────────────────────┘   │          ││
│  │                                                                     │          ││
│  │  ┌─────────────────────────────────────────────────────────────┐   │          ││
│  │  │ 3f. KAFKA (Async - For Analytics/Replay) [OPTIONAL]         │   │          ││
│  │  │                                                             │   │          ││
│  │  │  PRODUCE topic=mindmap.actions partition=hash(wf-123)       │   │          ││
│  │  │  value=<enrichedAction>                                     │   │          ││
│  │  └─────────────────────────────────────────────────────────────┘   │          ││
│  └─────────────────────────────────────────────────────────────────────┼──────────┘│
│                                                                        │           │
│                                                                        │           │
│  STEP 4: Fan-Out to All Gateways via Pub/Sub                          │           │
│  ════════════════════════════════════════════                          │           │
│                                                                        │           │
│                                                                        ▼           │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐│
│  │                          Redis Pub/Sub                                          ││
│  │                                                                                 ││
│  │   Channel: workflow:wf-123                                                      ││
│  │   Message: { seq: 43, type: "NODE_CREATE", payload: {...} }                     ││
│  │                                                                                 ││
│  │                    ┌─────────────┬─────────────┬─────────────┐                 ││
│  │                    ▼             ▼             ▼             ▼                 ││
│  │              ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐              ││
│  │              │Gateway 1 │ │Gateway 2 │ │Gateway 3 │ │Gateway N │              ││
│  │              │(10k      │ │(15k      │ │(20k      │ │(5k       │              ││
│  │              │viewers)  │ │viewers)  │ │viewers)  │ │viewers)  │              ││
│  │              └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘              ││
│  │                   │            │            │            │                     ││
│  └───────────────────┼────────────┼────────────┼────────────┼─────────────────────┘│
│                      │            │            │            │                      │
│                      ▼            ▼            ▼            ▼                      │
│                                                                                    │
│  STEP 5: Gateway Delivers to Connected Viewers                                    │
│  ══════════════════════════════════════════════                                   │
│                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │  Each Gateway (example: Gateway 2 with 15k viewers for this workflow)       │  │
│  │                                                                              │  │
│  │  // On receiving pub/sub message                                            │  │
│  │  func onPubSubMessage(workflowId string, action Action) {                   │  │
│  │      // Get all local connections watching this workflow                    │  │
│  │      connections := g.workflowSubscribers[workflowId]  // ~15k connections  │  │
│  │                                                                              │  │
│  │      // Fan-out to each connection (parallel goroutines)                    │  │
│  │      for conn := range connections {                                        │  │
│  │          go conn.Send(action)  // Non-blocking WebSocket write              │  │
│  │      }                                                                       │  │
│  │  }                                                                           │  │
│  │                                                                              │  │
│  │  // Each Send() is ~0.1-1ms per connection                                  │  │
│  │  // With goroutines, 15k sends complete in ~10-50ms                         │  │
│  └──────────────────────────────────┬───────────────────────────────────────────┘  │
│                                     │                                              │
│                                     │ WebSocket frames                             │
│                                     │ ~10-200ms (network dependent)                │
│                                     ▼                                              │
│                                                                                    │
│  STEP 6: Viewer Clients Render                                                    │
│  ══════════════════════════════                                                   │
│                                                                                    │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       ┌────────────┐             │
│  │ Viewer 1   │  │ Viewer 2   │  │ Viewer 3   │  ...  │ Viewer 100k│             │
│  │            │  │            │  │            │       │            │             │
│  │ 1. Receive │  │ 1. Receive │  │ 1. Receive │       │ 1. Receive │             │
│  │    action  │  │    action  │  │    action  │       │    action  │             │
│  │            │  │            │  │            │       │            │             │
│  │ 2. Validate│  │ 2. Validate│  │ 2. Validate│       │ 2. Validate│             │
│  │    seq_num │  │    seq_num │  │    seq_num │       │    seq_num │             │
│  │            │  │            │  │            │       │            │             │
│  │ 3. Update  │  │ 3. Update  │  │ 3. Update  │       │ 3. Update  │             │
│  │    local   │  │    local   │  │    local   │       │    local   │             │
│  │    state   │  │    state   │  │    state   │       │    state   │             │
│  │            │  │            │  │            │       │            │             │
│  │ 4. Render  │  │ 4. Render  │  │ 4. Render  │       │ 4. Render  │             │
│  │    new node│  │    new node│  │    new node│       │    new node│             │
│  │    on      │  │    on      │  │    on      │       │    on      │             │
│  │    canvas  │  │    canvas  │  │    canvas  │       │    canvas  │             │
│  └────────────┘  └────────────┘  └────────────┘       └────────────┘             │
│                                                                                    │
└──────────────────────────────────────────────────────────────────────────────────────┘


STEP 7: ACK Back to Creator (Parallel with broadcast)
══════════════════════════════════════════════════════

  Action Processor ─────────────────▶ Creator Gateway ─────────────────▶ Creator Client
                                     
  { type: "ACK", localId: "local-uuid-123", serverSeqNum: 43 }
  
  Creator client:
    - Remove from pendingActions
    - Update confirmedSeqNum = 43
```

### Timing Breakdown

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         LATENCY BREAKDOWN: NODE CREATE                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Component                          │ Time      │ Blocking? │ Notes                 │
│  ───────────────────────────────────┼───────────┼───────────┼────────────────────── │
│  Creator → Gateway (network)        │ 10-30ms   │ Yes       │ WebSocket, regional   │
│  Gateway auth/validate              │ 1-2ms     │ Yes       │ JWT validation        │
│  Gateway → Action Processor (gRPC)  │ 1-2ms     │ Yes       │ Internal network      │
│  Assign seq number (Redis INCR)     │ 0.5-1ms   │ Yes       │ Must be atomic        │
│  Redis state update (HSET)          │ 0.5-1ms   │ Yes       │ State must be fresh   │
│  Redis action buffer (ZADD)         │ 0.5-1ms   │ Yes       │ For catch-up          │
│  Redis Pub/Sub (PUBLISH)            │ 0.5-1ms   │ Yes       │ Triggers fan-out      │
│  PostgreSQL write                   │ 5-20ms    │ NO ❌     │ Async, non-blocking   │
│  Kafka produce                      │ 2-10ms    │ NO ❌     │ Async, non-blocking   │
│  Pub/Sub → Gateway delivery         │ 1-5ms     │ Yes       │ Redis internal        │
│  Gateway → Viewer (network)         │ 10-200ms  │ Yes       │ Depends on location   │
│  ───────────────────────────────────┼───────────┼───────────┼────────────────────── │
│  TOTAL BLOCKING PATH                │ 25-245ms  │           │ ✅ Under 500ms        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### How Redis Gets the Data

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                     HOW DATA FLOWS TO EACH STORE                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   Creator → Gateway → Action Processor → KAFKA (sync) → ACK                         │
│                              │                 │                                     │
│                              │                 └──────────────────────┐              │
│                         seq_num                                       │              │
│                         (Redis INCR)                                  │              │
│                                                ┌──────────────────────┼──────┐       │
│                                                │                      │      │       │
│                                                ▼                      ▼      │       │
│                                        ┌─────────────┐       ┌─────────────┐│       │
│                                        │   Redis     │       │  Postgres   ││       │
│                                        │  Consumer   │       │  Consumer   ││       │
│                                        │  Group      │       │  Group      ││       │
│                                        └──────┬──────┘       └──────┬──────┘│       │
│                                               │                     │       │       │
│                                     ┌─────────┴─────────┐           │       │       │
│                                     ▼         ▼         ▼           ▼       │       │
│                                 ┌───────┐ ┌───────┐ ┌───────┐ ┌──────────┐ │       │
│                                 │ State │ │Pub/Sub│ │ Undo  │ │action_log│ │       │
│                                 │ Cache │ │       │ │ Stack │ │  nodes   │ │       │
│                                 └───────┘ └───────┘ └───────┘ └──────────┘ │       │
│                                                                            │       │
│  Summary:                                                                  │       │
│  • Kafka is the commit log (sync write, ACK to creator)                   │       │
│  • Redis Consumer: applies state, broadcasts, manages undo                │       │
│  • Postgres Consumer: persists for durability and queries                 │       │
│  • Both consumers read ALL messages (separate offsets, isolated failures) │       │
│                                                                            │       │
└────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Reliability & Failure Handling: Kafka-First Architecture

### The Problem with the Simple Approach

The earlier flow has a critical flaw — **partial failures**:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    FAILURE SCENARIO: WHAT CAN GO WRONG?                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  Action Processor tries to do multiple things:                                      │
│                                                                                      │
│    1. Redis INCR (seq)      ✅ Success                                              │
│    2. Redis HSET (state)    ✅ Success                                              │
│    3. Redis ZADD (buffer)   ❌ FAILS (Redis timeout, network blip)                  │
│    4. Redis PUBLISH         ⚠️  Never executed                                       │
│    5. PostgreSQL INSERT     ⚠️  Never executed                                       │
│                                                                                      │
│  RESULT:                                                                            │
│  • State cache has the node (partial update)                                        │
│  • Action buffer missing (catch-up broken)                                          │
│  • Viewers never got the broadcast                                                  │
│  • PostgreSQL doesn't have the record                                               │
│  • Creator might have gotten an error OR timeout                                    │
│                                                                                      │
│  💀 DATA INCONSISTENCY - Very hard to debug and recover                            │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Solution: Kafka as Write-Ahead Log (WAL)

**Principle**: ACK the creator ONLY after Kafka confirms the write. Everything else is derived from Kafka.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA-FIRST ARCHITECTURE                                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│                          ┌─────────────────────────────┐                            │
│                          │     KAFKA IS THE SOURCE     │                            │
│                          │       OF TRUTH FOR          │                            │
│                          │     IN-FLIGHT ACTIONS       │                            │
│                          └─────────────────────────────┘                            │
│                                                                                      │
│  Creator ──▶ Gateway ──▶ Action Processor ──▶ KAFKA ──▶ ACK to Creator             │
│                                                 │                                    │
│                                                 │  (Kafka confirmed = action is     │
│                                                 │   guaranteed to be processed)     │
│                                                 │                                    │
│                                                 ▼                                    │
│                                    ┌───────────────────────┐                        │
│                                    │   Kafka Consumer(s)   │                        │
│                                    │   (Action Applier)    │                        │
│                                    └───────────┬───────────┘                        │
│                                                │                                    │
│                          ┌─────────────────────┼─────────────────────┐              │
│                          │                     │                     │              │
│                          ▼                     ▼                     ▼              │
│                    ┌──────────┐          ┌──────────┐          ┌──────────┐        │
│                    │  Redis   │          │  Redis   │          │PostgreSQL│        │
│                    │  State   │          │ Pub/Sub  │          │          │        │
│                    │  Update  │          │ Broadcast│          │  Persist │        │
│                    └──────────┘          └──────────┘          └──────────┘        │
│                                                                                      │
│  KEY INSIGHT: If consumer fails, Kafka retains the message.                         │
│               Consumer restarts, re-reads from last offset, re-applies.             │
│               NO DATA LOSS. EVENTUAL CONSISTENCY GUARANTEED.                        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Revised Call Flow: Kafka-First

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│              NODE ADDITION - KAFKA-FIRST RELIABLE FLOW                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  PHASE 1: WRITE PATH (Synchronous, Latency-Critical)                                │
│  ════════════════════════════════════════════════════                                │
│                                                                                      │
│  Creator Client                                                                      │
│       │                                                                              │
│       │ 1. User clicks "Add Node"                                                   │
│       │    - Apply optimistically to local UI                                       │
│       │    - Send action via WebSocket                                              │
│       ▼                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  Gateway                                                                     │    │
│  │                                                                              │    │
│  │  2. Authenticate, validate, forward to Action Processor                     │    │
│  └───────────────────────────────────┬─────────────────────────────────────────┘    │
│                                      │                                              │
│                                      ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  Action Processor                                                            │    │
│  │                                                                              │    │
│  │  3a. Assign sequence number (Redis INCR - atomic counter)                   │    │
│  │      seq_num = INCR workflow:seq:{workflow_id}                               │    │
│  │                                                                              │    │
│  │  3b. WRITE TO KAFKA (Synchronous, wait for ACK)                             │    │
│  │      ┌─────────────────────────────────────────────────────────────────┐    │    │
│  │      │  kafka.Produce(                                                  │    │    │
│  │      │    topic: "mindmap.actions",                                     │    │    │
│  │      │    partition: hash(workflow_id) % num_partitions,  // Ordering!  │    │    │
│  │      │    key: workflow_id,                                             │    │    │
│  │      │    value: {                                                      │    │    │
│  │      │      sequence_number: 43,                                        │    │    │
│  │      │      workflow_id: "wf-123",                                      │    │    │
│  │      │      action_type: "NODE_CREATE",                                 │    │    │
│  │      │      payload: { node_id: "n-456", x: 200, y: 150, ... },         │    │    │
│  │      │      creator_id: "cr-789",                                       │    │    │
│  │      │      timestamp: "2025-01-15T10:30:00.123Z"                       │    │    │
│  │      │    },                                                            │    │    │
│  │      │    acks: "all"  // Wait for all replicas to confirm              │    │    │
│  │      │  )                                                                │    │    │
│  │      └─────────────────────────────────────────────────────────────────┘    │    │
│  │                                                                              │    │
│  │  3c. WAIT for Kafka ACK (~5-15ms)                                           │    │
│  │      - If Kafka ACK received → Action is GUARANTEED durable                  │    │
│  │      - If Kafka fails → Return error to creator, they can retry              │    │
│  │                                                                              │    │
│  │  3d. OPTIONAL: Fast-path broadcast via Redis Pub/Sub                        │    │
│  │      (Best-effort, don't wait for confirmation)                              │    │
│  │      redis.Publish("workflow:wf-123", action)                                │    │
│  │                                                                              │    │
│  └───────────────────────────────────┬─────────────────────────────────────────┘    │
│                                      │                                              │
│                                      ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │  4. ACK to Creator                                                           │    │
│  │                                                                              │    │
│  │  { type: "ACK", localId: "local-123", serverSeqNum: 43, status: "COMMITTED" }│    │
│  │                                                                              │    │
│  │  Creator removes from pendingActions, action is CONFIRMED                   │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                      │
│                                                                                      │
│  PHASE 2: APPLY PATH (Two Consumer Groups)                                          │
│  ══════════════════════════════════════════                                          │
│                                                                                      │
│  WHY TWO CONSUMER GROUPS?                                                           │
│  • Group operations by data store (Redis vs Postgres)                               │
│  • Within a store, operations succeed/fail together (atomic)                        │
│  • Cross-store failures are isolated (Redis up, Postgres down = viewers still work) │
│  • Different latency SLAs (Redis: <10ms, Postgres: <5s acceptable)                  │
│                                                                                      │
│                           ┌─────────────────┐                                       │
│                           │      KAFKA      │                                       │
│                           │  mindmap.actions│                                       │
│                           └────────┬────────┘                                       │
│                                    │                                                │
│                    ┌───────────────┴───────────────┐                                │
│                    │                               │                                │
│                    ▼                               ▼                                │
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐          │
│  │ Consumer Group: "redis-applier" │  │ Consumer Group: "pg-applier"    │          │
│  │                                 │  │                                 │          │
│  │ func Process(action Action) {   │  │ func Process(actions []Action) {│          │
│  │                                 │  │   tx := db.Begin()              │          │
│  │   // IDEMPOTENCY: Check watermark│  │                                 │          │
│  │   applied := redis.HGet(        │  │   for _, a := range actions {   │          │
│  │     stateKey, "applied_seq")    │  │     // IDEMPOTENCY: ON CONFLICT │          │
│  │   if action.Seq <= applied {    │  │     // DO NOTHING on action_log │          │
│  │     return // Already applied   │  │     tx.Exec(INSERT action_log   │          │
│  │   }                             │  │       ON CONFLICT DO NOTHING)   │          │
│  │                                 │  │     tx.Exec(INSERT/UPDATE node) │          │
│  │   pipe := redis.Pipeline()      │  │   }                             │          │
│  │   pipe.HSet(stateKey, ...)      │  │                                 │          │
│  │   pipe.HSet(stateKey,           │  │   tx.Commit() // All or nothing │          │
│  │     "applied_seq", action.Seq)  │  │ }                               │          │
│  │   pipe.ZAdd(bufferKey, ...)     │  │                                 │          │
│  │   pipe.Publish(channel, ...)    │  │ Latency SLA: <5s (can batch)    │          │
│  │   pipe.Exec() // All or nothing │  │                                 │          │
│  │ }                               │  │                                 │          │
│  │ Latency SLA: <10ms              │  │                                 │          │
│  └────────────────┬────────────────┘  └────────────────┬────────────────┘          │
│                   │                                    │                            │
│         ┌─────────┴─────────┐                          │                            │
│         ▼         ▼         ▼                          ▼                            │
│     ┌───────┐ ┌───────┐ ┌───────┐              ┌────────────┐                      │
│     │ State │ │Pub/Sub│ │ Undo  │              │ PostgreSQL │                      │
│     │ Cache │ │       │ │ Stack │              │            │                      │
│     └───────┘ └───┬───┘ └───────┘              └────────────┘                      │
│                   │                                                                 │
│                   ▼                                                                 │
│               Gateways → Viewers                                                    │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Failure Scenarios

```
┌────────────────────┬─────────────────────┬─────────────────────────────────────────┐
│ Redis Status       │ Postgres Status     │ Outcome                                 │
├────────────────────┼─────────────────────┼─────────────────────────────────────────┤
│ ✅ Up              │ ✅ Up               │ Both consumers work normally            │
├────────────────────┼─────────────────────┼─────────────────────────────────────────┤
│ ✅ Up              │ ❌ Down             │ Redis consumer: viewers served ✅       │
│                    │                     │ Postgres consumer: retries, catches up  │
├────────────────────┼─────────────────────┼─────────────────────────────────────────┤
│ ❌ Down            │ ✅ Up               │ Redis consumer: retries until Redis up  │
│                    │                     │ Postgres consumer: still recording      │
├────────────────────┼─────────────────────┼─────────────────────────────────────────┤
│ Kafka Write Fails  │ -                   │ Error to creator, clean retry possible  │
├────────────────────┼─────────────────────┼─────────────────────────────────────────┤
│ Consumer Crash     │ -                   │ Kafka retains, consumer restarts,       │
│                    │                     │ idempotent handlers skip duplicates     │
└────────────────────┴─────────────────────┴─────────────────────────────────────────┘
```

### Idempotency: Key to Reliability

All handlers must be **idempotent** (safe to replay):

```go
func updateRedisState(action Action) error {
    stateKey := "workflow:state:" + action.WorkflowId
    
    // Check if already applied (idempotency check)
    currentSeq, _ := redis.HGet(ctx, stateKey, "last_seq_num").Int64()
    
    if action.SequenceNumber <= currentSeq {
        // Already applied, skip (duplicate from retry)
        return nil
    }
    
    // Apply the action
    switch action.Type {
    case "NODE_CREATE":
        // Lua script to atomically add node and update seq
        script := `
            local nodes = cjson.decode(redis.call('HGET', KEYS[1], 'nodes') or '[]')
            table.insert(nodes, ARGV[1])
            redis.call('HSET', KEYS[1], 'nodes', cjson.encode(nodes))
            redis.call('HSET', KEYS[1], 'last_seq_num', ARGV[2])
        `
        redis.Eval(ctx, script, []string{stateKey}, action.Payload, action.SequenceNumber)
    
    // ... other action types
    }
    
    return nil
}
```

### Latency Impact

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    LATENCY COMPARISON                                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ORIGINAL (Redis-only, unreliable):                                                 │
│  ─────────────────────────────────────                                               │
│  Creator → Gateway → Redis writes → ACK                                             │
│  Total: ~15-35ms (fast, but can lose data)                                          │
│                                                                                      │
│  KAFKA-FIRST (Reliable):                                                            │
│  ─────────────────────────────────────                                               │
│  Creator → Gateway → Kafka write (acks=all) → ACK                                   │
│  Total: ~25-50ms (+10-15ms for Kafka durability)                                    │
│                                                                                      │
│  STILL UNDER 500ms TARGET? ✅ YES                                                   │
│                                                                                      │
│  Full path to viewers:                                                              │
│  ─────────────────────────────────────                                               │
│  With fast-path (Redis Pub/Sub in parallel):                                        │
│    Creator → Kafka ACK:           ~25-50ms                                          │
│    Parallel: Redis Pub/Sub:       ~5-10ms                                           │
│    Gateway → Viewer:              ~50-200ms                                         │
│    ────────────────────────────────────────                                          │
│    Total:                         ~80-260ms ✅                                       │
│                                                                                      │
│  Without fast-path (consumer only):                                                 │
│    Creator → Kafka ACK:           ~25-50ms                                          │
│    Kafka → Consumer:              ~5-20ms                                           │
│    Consumer → Redis Pub/Sub:      ~5-10ms                                           │
│    Gateway → Viewer:              ~50-200ms                                         │
│    ────────────────────────────────────────                                          │
│    Total:                         ~85-280ms ✅                                       │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Architecture Diagram: Kafka-First

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   Creator ──────────▶ Gateway ──────────▶ Action Processor                          │
│      │                                          │                                    │
│      │                                          │ 1. Get seq_num (Redis INCR)        │
│      │                                          │ 2. Write to Kafka (sync, acks=all) │
│      │                                          │ 3. Optional: Fast-path Pub/Sub     │
│      │                                          │                                    │
│      │◀─────────────── ACK ─────────────────────┤                                    │
│      │                                          │                                    │
│      │                                          ▼                                    │
│      │                                    ┌──────────┐                               │
│      │                                    │  KAFKA   │                               │
│      │                                    │ (durable │                               │
│      │                                    │  commit  │                               │
│      │                                    │   log)   │                               │
│      │                                    └────┬─────┘                               │
│      │                                         │                                     │
│      │                                         │ Consumer reads                      │
│      │                                         ▼                                     │
│      │                              ┌────────────────────┐                           │
│      │                              │  Action Applier    │                           │
│      │                              │  (Kafka Consumer)  │                           │
│      │                              └──────────┬─────────┘                           │
│      │                                         │                                     │
│      │                    ┌────────────────────┼────────────────────┐                │
│      │                    │                    │                    │                │
│      │                    ▼                    ▼                    ▼                │
│      │              ┌──────────┐        ┌──────────┐        ┌──────────┐            │
│      │              │  Redis   │        │  Redis   │        │PostgreSQL│            │
│      │              │  State   │        │ Pub/Sub  │        │          │            │
│      │              │  Cache   │        │ Broadcast│        │  Durable │            │
│      │              └──────────┘        └────┬─────┘        │  Storage │            │
│      │                                       │              └──────────┘            │
│      │                                       │                                       │
│      │                                       ▼                                       │
│      │                              ┌─────────────────┐                              │
│      │                              │    Gateways     │                              │
│      │                              │ (fan-out to     │                              │
│      │                              │  100k viewers)  │                              │
│      │                              └────────┬────────┘                              │
│      │                                       │                                       │
│      │                                       ▼                                       │
│      │                              ┌─────────────────┐                              │
│      │                              │    Viewers      │                              │
│      │                              │  (receive via   │                              │
│      │                              │   WebSocket)    │                              │
│      │                              └─────────────────┘                              │
│      │                                                                               │
└──────┴───────────────────────────────────────────────────────────────────────────────┘
```

### Trade-off Summary

| Aspect | Redis-First (Original) | Kafka-First (Recommended) |
|--------|------------------------|---------------------------|
| **Latency to ACK** | ~15-35ms | ~25-50ms |
| **Latency to viewers** | ~70-250ms | ~80-280ms |
| **Durability** | ❌ Can lose data | ✅ Guaranteed |
| **Failure recovery** | ❌ Manual intervention | ✅ Automatic retry |
| **Exactly-once** | ❌ Best-effort | ✅ With idempotency |
| **Complexity** | Lower | Higher (but worth it) |
| **Debugging** | Hard (where did it fail?) | Easy (replay from Kafka) |

### Interview Sound Bite

> "For reliability, I'd use Kafka as the commit log. ACK goes to creator only after Kafka confirms with acks=all. Two consumer groups — one for Redis (real-time), one for PostgreSQL (durability) — process independently. This isolates failures by data store: if Postgres is down, viewers still work via Redis. Idempotent handlers ensure exactly-once semantics. The latency cost is ~15ms for Kafka, but we're well under 500ms."

---

## Deep Dive: Real-Time Broadcasting Architecture

### The Fan-Out Problem

With 100k viewers per creator, we need efficient fan-out. Direct broadcast from a single server is impossible.

### Solution: Hierarchical Fan-Out with Gateway Mesh

```
                          Creator
                             │
                             ▼
                    ┌────────────────┐
                    │ Origin Gateway │  (receives creator actions)
                    └───────┬────────┘
                            │
                    ┌───────┴───────┐
                    ▼               ▼
              ┌──────────┐    ┌──────────┐
              │  Redis   │ OR │  Kafka   │
              │  Pub/Sub │    │          │
              └────┬─────┘    └────┬─────┘
                   │               │
       ┌───────────┼───────────────┼───────────┐
       ▼           ▼               ▼           ▼
   ┌────────┐ ┌────────┐     ┌────────┐  ┌────────┐
   │Gateway │ │Gateway │     │Gateway │  │Gateway │
   │   1    │ │   2    │ ... │   N    │  │   N+1  │
   └───┬────┘ └───┬────┘     └───┬────┘  └───┬────┘
       │          │              │           │
    ┌──┴──┐    ┌──┴──┐       ┌──┴──┐     ┌──┴──┐
    │50k  │    │50k  │       │50k  │     │50k  │
    │users│    │users│       │users│     │users│
    └─────┘    └─────┘       └─────┘     └─────┘
```

### Gateway Server Design

Each gateway server:
- Holds up to **50k WebSocket connections**
- Subscribes to Redis Pub/Sub channels for workflows it serves
- Maintains a local map: `workflow_id → Set<connection_id>`

```go
type GatewayServer struct {
    connections    map[string]*WebSocketConn  // conn_id -> connection
    workflowSubs   map[string]Set[string]     // workflow_id -> set of conn_ids
    redisClient    *redis.Client
}

func (g *GatewayServer) OnMessage(workflowId string, action Action) {
    // Fan-out to all local connections watching this workflow
    if connIds, ok := g.workflowSubs[workflowId]; ok {
        for connId := range connIds {
            g.connections[connId].Send(action)
        }
    }
}
```

### Why Redis Pub/Sub Over Kafka for This Use Case?

| Aspect | Redis Pub/Sub | Kafka |
|--------|---------------|-------|
| Latency | ~1-5ms | ~10-50ms |
| Durability | Fire-and-forget | Persistent |
| Ordering | Per channel | Per partition |
| Backpressure | None (drop if slow) | Consumer-controlled |
| Best for | Live streaming, ephemeral | Audit logs, replay |

**Decision**: Use **Redis Pub/Sub** for live broadcast (latency critical), **Kafka** for action log persistence (durability).

---

## Handling State Synchronization

### Initial State Load (New Viewer Joins)

When a viewer joins a live session:

```
┌─────────┐      ┌─────────────┐      ┌─────────────┐      ┌───────────┐
│ Viewer  │──1──▶│   Gateway   │──2──▶│    State    │──3──▶│   Redis   │
│         │      │             │      │   Manager   │      │  (Cache)  │
└─────────┘      └─────────────┘      └─────────────┘      └───────────┘
     │                 │                     │                    │
     │                 │                     │◀────4──────────────┤
     │                 │◀────────5───────────┤   Current State    │
     │◀────────6───────┤                     │   + last_seq_num   │
     │  Full State     │                     │                    │
     │  Snapshot       │                     │                    │
     │                 │                     │                    │
     │────7: Subscribe to workflow channel───▶                    │
     │                 │                     │                    │
     │◀───8: Incremental updates from seq_num+1 onwards───────────│
```

### State Storage in Redis

```
# Full workflow state (rebuilt from actions)
workflow:state:{workflow_id} → Hash {
    nodes: JSON array of all nodes
    connectors: JSON array of all connectors
    last_seq_num: 42
    last_updated: timestamp
}

# Active viewers tracking
workflow:viewers:{workflow_id} → Set of user_ids

# Gateway subscription registry
gateway:subscriptions:{gateway_id} → Set of workflow_ids
```

---

## Undo/Redo Implementation

### Key Principle: Append-Only Action Log

Undo doesn't rewrite history — it appends NEW inverse actions to the log. Viewers just see a continuous stream of actions.

```
User actions:     1 add, 2 add, 3 add, 4 add, 5 add
User undoes 3x:   6 rem(5), 7 rem(4), 8 rem(3)
User redoes 1x:   9 add(3)
User NEW action:  10 add(new node)

Action log: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  ← Always append-only

Viewers apply all actions in sequence. They don't know which are undos.
```

### Command Pattern with Inverse Actions

Every action stores its inverse at creation time:

```json
{
  "seq": 5,
  "action_type": "NODE_UPDATE",
  "payload": { "node_id": "abc", "x": 200, "y": 300 },
  "inverse_payload": { "node_id": "abc", "x": 100, "y": 150 }
}
```

### Architecture: Stacks Track, Log Broadcasts

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    UNDO/REDO ARCHITECTURE                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  UNDO/REDO STACKS (Redis)              ACTION LOG (Kafka)                           │
│  ─────────────────────────             ─────────────────────                         │
│  • Track what CAN be undone/redone     • What actually happened                     │
│  • Used to CREATE new log entries      • What viewers receive                       │
│  • Per-workflow, creator only          • Append-only, immutable                     │
│  • Limit: 100 actions                  • Full history preserved                     │
│                                                                                      │
│                                                                                      │
│  Creator clicks UNDO                                                                │
│         │                                                                           │
│         ▼                                                                           │
│  ┌─────────────────────┐                                                            │
│  │ Pop from UNDO stack │ → Get action with inverse_payload                          │
│  └──────────┬──────────┘                                                            │
│             │                                                                       │
│             ▼                                                                       │
│  ┌─────────────────────┐                                                            │
│  │ Push to REDO stack  │ → For potential redo later                                 │
│  └──────────┬──────────┘                                                            │
│             │                                                                       │
│             ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ Create NEW action (seq = N+1) with inverse_payload as payload              │   │
│  │ Push to Kafka → Redis Pub/Sub → Viewers                                     │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Stack Storage in Redis

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  REDIS KEYS (Per Workflow)                                                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  workflow:undo:{id}  →  List (LIFO stack)                                           │
│  workflow:redo:{id}  →  List (LIFO stack)                                           │
│                                                                                      │
│  Each entry contains:                                                               │
│  {                                                                                   │
│    "original_seq": 5,                                                               │
│    "action_type": "NODE_UPDATE",                                                    │
│    "payload": { "node_id": "abc", "x": 200, "y": 300 },                             │
│    "inverse_payload": { "node_id": "abc", "x": 100, "y": 150 }                      │
│  }                                                                                   │
│                                                                                      │
│  Max size: 100 actions (LTRIM after each push)                                      │
│  TTL: Session lifetime (cleared when workflow goes offline)                         │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Undo/Redo Flows

```go
// ON NEW ACTION (not undo/redo)
func (s *Server) OnNewAction(action Action) {
    // 1. Push to undo stack (with inverse for later)
    s.redis.LPush("workflow:undo:"+workflowId, action)
    s.redis.LTrim("workflow:undo:"+workflowId, 0, 99)  // Keep last 100
    
    // 2. Clear redo stack (new action = new timeline)
    s.redis.Del("workflow:redo:"+workflowId)
    
    // 3. Publish to Kafka → viewers
    s.kafka.Produce(action)
}

// ON UNDO
func (s *Server) OnUndo(workflowId string) error {
    // 1. Pop from undo stack
    action := s.redis.LPop("workflow:undo:"+workflowId)
    if action == nil {
        return errors.New("nothing to undo")
    }
    
    // 2. Push to redo stack (for potential redo)
    s.redis.LPush("workflow:redo:"+workflowId, action)
    
    // 3. Create NEW action with inverse payload
    inverseAction := Action{
        Type:    action.Type,
        Payload: action.InversePayload,  // Use the inverse
        // inverse_payload = action.Payload (swap for redo)
    }
    
    // 4. Publish to Kafka → viewers see it as normal action
    s.kafka.Produce(inverseAction)
    return nil
}

// ON REDO
func (s *Server) OnRedo(workflowId string) error {
    // 1. Pop from redo stack
    action := s.redis.LPop("workflow:redo:"+workflowId)
    if action == nil {
        return errors.New("nothing to redo")
    }
    
    // 2. Push back to undo stack
    s.redis.LPush("workflow:undo:"+workflowId, action)
    
    // 3. Create NEW action with original payload
    redoAction := Action{
        Type:    action.Type,
        Payload: action.Payload,  // Use the original
    }
    
    // 4. Publish to Kafka → viewers
    s.kafka.Produce(redoAction)
    return nil
}
```

### Example Walkthrough

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  EXAMPLE: 5 actions, then 3 undos, then 1 redo, then new action                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  After 5 actions (add nodes A, B, C, D, E):                                         │
│  ─────────────────────────────────────────                                           │
│  Action Log: [1:A, 2:B, 3:C, 4:D, 5:E]                                              │
│  Undo Stack: [E, D, C, B, A]  (top = E)                                             │
│  Redo Stack: []                                                                     │
│                                                                                      │
│  After UNDO (removes E):                                                            │
│  ─────────────────────────────────────────                                           │
│  Action Log: [1:A, 2:B, 3:C, 4:D, 5:E, 6:rem(E)]                                    │
│  Undo Stack: [D, C, B, A]                                                           │
│  Redo Stack: [E]                                                                    │
│                                                                                      │
│  After UNDO (removes D):                                                            │
│  ─────────────────────────────────────────                                           │
│  Action Log: [1:A, 2:B, 3:C, 4:D, 5:E, 6:rem(E), 7:rem(D)]                          │
│  Undo Stack: [C, B, A]                                                              │
│  Redo Stack: [E, D]                                                                 │
│                                                                                      │
│  After UNDO (removes C):                                                            │
│  ─────────────────────────────────────────                                           │
│  Action Log: [..., 8:rem(C)]                                                        │
│  Undo Stack: [B, A]                                                                 │
│  Redo Stack: [E, D, C]                                                              │
│                                                                                      │
│  After REDO (re-adds C):                                                            │
│  ─────────────────────────────────────────                                           │
│  Action Log: [..., 9:add(C)]                                                        │
│  Undo Stack: [C, B, A]                                                              │
│  Redo Stack: [E, D]                                                                 │
│                                                                                      │
│  After NEW action (add F):                                                          │
│  ─────────────────────────────────────────                                           │
│  Action Log: [..., 10:add(F)]                                                       │
│  Undo Stack: [F, C, B, A]                                                           │
│  Redo Stack: []  ← CLEARED! Can't redo D or E anymore.                              │
│                                                                                      │
│  Why cleared? New action creates a new timeline.                                    │
│  D and E were on the "old future" which we abandoned.                               │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Why Redo Gets Cleared on New Action

Two reasons:

1. **Definition**: Standard undo/redo semantics — new action = new timeline, old "future" discarded

2. **Dependencies**: Undone actions may reference state that no longer exists after the new action
   ```
   Original: "Connect node C to node D"
   We undid C, D, E. Then added F.
   If we redo "Connect C to D"... C and D don't exist yet!
   ```

### Viewers Don't Need Stacks

Viewers just apply actions in order. They don't care if action 6 was an "undo" or a regular action:

```javascript
// Viewer client - simple, no undo/redo logic
viewer.onAction(action) {
    switch (action.type) {
        case "NODE_CREATE": addNode(action.payload); break;
        case "NODE_DELETE": removeNode(action.payload.node_id); break;
        case "NODE_UPDATE": updateNode(action.payload); break;
    }
    // That's it! No undo/redo awareness needed.
}
```

---

## Handling Network Issues & Action Sequencing

### Client-Side Optimistic Updates

```javascript
class WorkflowClient {
  constructor() {
    this.pendingActions = new Map(); // local_id -> action
    this.confirmedSeqNum = 0;
    this.localSeqNum = 0;
  }

  applyAction(action) {
    // 1. Assign local sequence number
    action.localSeqNum = ++this.localSeqNum;
    action.localId = uuid();
    
    // 2. Apply optimistically to local state
    this.applyToLocalState(action);
    
    // 3. Store in pending queue
    this.pendingActions.set(action.localId, action);
    
    // 4. Send to server
    this.websocket.send(action);
  }

  onServerAck(ack) {
    // Server confirmed action with server sequence number
    this.pendingActions.delete(ack.localId);
    this.confirmedSeqNum = ack.serverSeqNum;
  }

  onServerAction(serverAction) {
    if (serverAction.serverSeqNum <= this.confirmedSeqNum) {
      // Already applied, skip
      return;
    }
    
    // Check if this conflicts with pending actions
    // For viewer clients: just apply
    // For creator client: reconcile if needed
    this.applyToLocalState(serverAction);
  }

  onReconnect() {
    // Request state from confirmedSeqNum
    this.websocket.send({
      type: 'SYNC',
      fromSeqNum: this.confirmedSeqNum
    });
  }
}
```

### Server-Side Sequencing

```go
type ActionProcessor struct {
    seqLock     sync.Mutex
    seqNumbers  map[string]int64  // workflow_id -> last_seq_num
}

func (p *ActionProcessor) ProcessAction(action Action) (int64, error) {
    p.seqLock.Lock()
    defer p.seqLock.Unlock()
    
    // Assign monotonically increasing sequence number
    seq := p.seqNumbers[action.WorkflowId] + 1
    p.seqNumbers[action.WorkflowId] = seq
    
    action.SequenceNumber = seq
    action.ServerTimestamp = time.Now()
    
    // Persist to action log (Kafka/DB)
    if err := p.persistAction(action); err != nil {
        return 0, err
    }
    
    // Broadcast via Redis Pub/Sub
    p.broadcast(action)
    
    return seq, nil
}
```

### Reconnection & Catch-Up Protocol

```
┌─────────────────────────────────────────────────────────────────────┐
│                    RECONNECTION FLOW                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Client                        Server                                │
│    │                             │                                   │
│    │  1. RECONNECT               │                                   │
│    │  {workflow_id, last_seq: 42}│                                   │
│    │────────────────────────────▶│                                   │
│    │                             │                                   │
│    │                             │  2. Check gap (current: 47)       │
│    │                             │                                   │
│    │  3. CATCH_UP                │                                   │
│    │  [actions 43, 44, 45, 46, 47]                                   │
│    │◀────────────────────────────│                                   │
│    │                             │                                   │
│    │  4. Apply missed actions    │                                   │
│    │                             │                                   │
│    │  5. Resume live stream      │                                   │
│    │◀════════════════════════════│                                   │
│                                                                      │
│  If gap > 100 actions: Send full state snapshot instead              │
└─────────────────────────────────────────────────────────────────────┘
```

### Keeping Clients in Sync (Heartbeat Mechanism)

**Problem**: Redis Pub/Sub is fire-and-forget. If a client silently misses a message, they won't know they're stale.

**Solution**: Periodic heartbeat with latest sequence number.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    CLIENT SYNC MECHANISMS                                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  1. SEQUENCE GAP DETECTION (Immediate)                                              │
│     Client receives seq=45, but last seen was seq=42                                │
│     → Gap detected → Request catch-up for 43, 44                                    │
│                                                                                      │
│  2. SERVER HEARTBEAT (Every 5 seconds)                                              │
│     Server broadcasts: { type: "HEARTBEAT", latest_seq: 50 }                        │
│     Client checks: my_seq (45) < latest_seq (50) → Stale! Request catch-up          │
│     If my_seq == latest_seq → All good                                              │
│                                                                                      │
│  3. CLIENT TIMEOUT POLL (Fallback, if no message for 30s)                           │
│     Client sends: { type: "SYNC_CHECK", my_seq: 45 }                                │
│     Server responds with latest_seq                                                 │
│                                                                                      │
│  4. NATURAL DISCONNECT                                                              │
│     No activity for long time → Viewers close tab → Load subsides                   │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

```go
// Server: Heartbeat broadcaster (per live workflow)
func (s *Server) StartHeartbeat(workflowId string) {
    ticker := time.NewTicker(5 * time.Second)
    for range ticker.C {
        if !s.isWorkflowLive(workflowId) {
            return
        }
        latestSeq := s.redis.HGet("workflow:state:"+workflowId, "applied_seq")
        s.redis.Publish("workflow:"+workflowId, Heartbeat{
            Type:      "HEARTBEAT",
            LatestSeq: latestSeq,
        })
    }
}
```

```javascript
// Client: Handle heartbeat and detect staleness
class ViewerClient {
  onMessage(msg) {
    this.lastMessageTime = Date.now();
    
    if (msg.type === "ACTION") {
      if (msg.seq > this.lastSeq + 1) {
        this.requestCatchUp(this.lastSeq + 1, msg.seq - 1);
      }
      this.applyAction(msg);
      this.lastSeq = msg.seq;
      
    } else if (msg.type === "HEARTBEAT") {
      if (msg.latest_seq > this.lastSeq) {
        // Stale! Missed some updates
        this.requestCatchUp(this.lastSeq + 1, msg.latest_seq);
      }
    }
  }

  // Fallback: if no messages for 30s, actively check
  startTimeoutCheck() {
    setInterval(() => {
      if (Date.now() - this.lastMessageTime > 30000) {
        this.websocket.send({ type: "SYNC_CHECK", my_seq: this.lastSeq });
      }
    }, 10000);
  }
}
```

**Why not Redis Streams?** Redis Streams provide persistence and consumer group offsets, but we already have:
- Kafka for durability
- Action buffer (ZSET) for catch-up
- Heartbeat for detecting staleness

Redis Streams would add complexity without significant benefit.

---

## Latency Optimization Strategies

### Target: < 500ms End-to-End

```
┌──────────────────────────────────────────────────────────────────────┐
│              LATENCY BUDGET BREAKDOWN                                 │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Creator Action → Gateway          ~10-30ms  (WebSocket + network)    │
│  Gateway → Action Processor        ~5-10ms   (internal)               │
│  Action Processor → Redis Pub/Sub  ~1-5ms    (Redis)                  │
│  Redis → Viewer Gateways           ~5-10ms   (pub/sub fan-out)        │
│  Gateway → Viewer Clients          ~50-200ms (network, geo-dependent) │
│  ─────────────────────────────────────────────                        │
│  TOTAL                             ~70-255ms ✅ Under 500ms           │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Optimizations Applied

| Optimization | Impact |
|--------------|--------|
| **Edge Gateways** | Deploy gateways in multiple regions (US, EU, Asia) to reduce last-mile latency |
| **Binary Protocol** | Use MessagePack/Protobuf instead of JSON (~40% smaller) |
| **Action Batching** | Batch rapid actions (e.g., mouse drag) into single message every 50ms |
| **WebSocket Compression** | Enable per-message deflate |
| **Connection Pooling** | Pre-established connections between services |

### Action Batching for Drag Operations

```javascript
class ActionBatcher {
  constructor(flushIntervalMs = 50) {
    this.batch = [];
    this.timer = null;
    this.flushInterval = flushIntervalMs;
  }

  add(action) {
    // For position updates, replace previous update for same node
    if (action.type === 'NODE_MOVE') {
      const idx = this.batch.findIndex(a => 
        a.type === 'NODE_MOVE' && a.nodeId === action.nodeId
      );
      if (idx >= 0) {
        this.batch[idx] = action; // Replace with latest position
      } else {
        this.batch.push(action);
      }
    } else {
      this.batch.push(action);
    }

    if (!this.timer) {
      this.timer = setTimeout(() => this.flush(), this.flushInterval);
    }
  }

  flush() {
    if (this.batch.length > 0) {
      this.websocket.send({ type: 'BATCH', actions: this.batch });
      this.batch = [];
    }
    this.timer = null;
  }
}
```

---

## Scaling Strategy

### Connection Tier Scaling

```
┌──────────────────────────────────────────────────────────────────────┐
│              GATEWAY FLEET SIZING                                     │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Target: 10M concurrent connections                                   │
│  Connections per gateway: 50k                                         │
│  Gateways needed: 10M / 50k = 200 gateway instances                   │
│                                                                       │
│  Distribution across regions:                                         │
│    - US-East:  50 gateways                                            │
│    - US-West:  40 gateways                                            │
│    - EU:       50 gateways                                            │
│    - Asia:     60 gateways                                            │
│                                                                       │
│  Each gateway: 4 vCPU, 8GB RAM (WebSocket optimized)                  │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Redis Cluster for Pub/Sub

```
┌──────────────────────────────────────────────────────────────────────┐
│              REDIS CLUSTER TOPOLOGY                                   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Sharded by workflow_id hash:                                         │
│                                                                       │
│   ┌────────────┐   ┌────────────┐   ┌────────────┐                   │
│   │  Redis 1   │   │  Redis 2   │   │  Redis 3   │                   │
│   │  Primary   │   │  Primary   │   │  Primary   │                   │
│   │  + Replica │   │  + Replica │   │  + Replica │                   │
│   └────────────┘   └────────────┘   └────────────┘                   │
│   workflows:       workflows:       workflows:                        │
│   hash % 3 = 0     hash % 3 = 1     hash % 3 = 2                      │
│                                                                       │
│  Each shard handles ~333 live workflows (1k total)                    │
│  Pub/Sub channels are local to the shard owning the workflow          │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Database Design

### PostgreSQL Schema

```sql
-- Workflows table
CREATE TABLE workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES users(id),
    title VARCHAR(255) NOT NULL,
    is_live BOOLEAN DEFAULT FALSE,
    is_public BOOLEAN DEFAULT TRUE,
    current_version BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_workflows_creator ON workflows(creator_id);
CREATE INDEX idx_workflows_live ON workflows(is_live) WHERE is_live = TRUE;

-- Nodes table
CREATE TABLE nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    label TEXT,
    x_position FLOAT NOT NULL,
    y_position FLOAT NOT NULL,
    width FLOAT DEFAULT 150,
    height FLOAT DEFAULT 80,
    style JSONB DEFAULT '{}',
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_nodes_workflow ON nodes(workflow_id) WHERE is_deleted = FALSE;

-- Connectors table
CREATE TABLE connectors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    source_node_id UUID NOT NULL REFERENCES nodes(id),
    target_node_id UUID NOT NULL REFERENCES nodes(id),
    label TEXT,
    style JSONB DEFAULT '{}',
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_connectors_workflow ON connectors(workflow_id) WHERE is_deleted = FALSE;

-- Action event log (for replay and audit)
CREATE TABLE action_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    sequence_number BIGINT NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    inverse_payload JSONB,
    creator_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(workflow_id, sequence_number)
);

-- Partition by workflow for efficient queries
CREATE INDEX idx_action_log_workflow_seq ON action_log(workflow_id, sequence_number);
```

---

## API Design

### WebSocket Protocol

```yaml
# Creator → Server
CreatorMessage:
  - type: ACTION
    action:
      localId: string
      actionType: NODE_CREATE | NODE_UPDATE | NODE_DELETE | CONNECTOR_CREATE | CONNECTOR_UPDATE | CONNECTOR_DELETE
      payload: object
  
  - type: UNDO
  
  - type: REDO
  
  - type: START_LIVE
  
  - type: STOP_LIVE

# Server → Creator
ServerToCreatorMessage:
  - type: ACK
    localId: string
    serverSeqNum: number
  
  - type: ERROR
    localId: string
    error: string
  
  - type: STATE_SYNC
    state: WorkflowState

# Server → Viewer
ServerToViewerMessage:
  - type: ACTION
    serverSeqNum: number
    actionType: string
    payload: object
  
  - type: INITIAL_STATE
    state: WorkflowState
    lastSeqNum: number
  
  - type: CATCH_UP
    actions: Action[]

# Viewer → Server
ViewerMessage:
  - type: SUBSCRIBE
    workflowId: string
    fromSeqNum: number (optional, for reconnect)
  
  - type: UNSUBSCRIBE
    workflowId: string
```

### REST API (for non-real-time operations)

```yaml
# Workflow CRUD
GET    /api/v1/creators/{creator_id}/workflows    # List creator's workflows (public if not self)
POST   /api/v1/creators/{creator_id}/workflows    # Create new workflow (auth: only self)
GET    /api/v1/workflows/{id}                     # Get workflow details
DELETE /api/v1/workflows/{id}                     # Delete workflow (auth: only creator)

# Shorthand for authenticated user
GET    /api/v1/me/workflows                       # List my workflows

# Live session management
POST   /api/v1/workflows/{id}/live/start          # Start live session
POST   /api/v1/workflows/{id}/live/stop           # Stop live session
GET    /api/v1/workflows/{id}/viewers             # Get viewer count

# Discovery
GET    /api/v1/live                               # List all live workflows
GET    /api/v1/creators/{id}/live                 # Get creator's live workflow
```

---

## Error Handling & Resilience

### Circuit Breaker Pattern

```go
type Broadcaster struct {
    circuitBreaker *gobreaker.CircuitBreaker
}

func (b *Broadcaster) Broadcast(action Action) error {
    _, err := b.circuitBreaker.Execute(func() (interface{}, error) {
        return nil, b.redis.Publish(
            context.Background(),
            fmt.Sprintf("workflow:%s", action.WorkflowId),
            action,
        ).Err()
    })
    
    if err != nil {
        // Fallback: queue for retry
        b.retryQueue.Add(action)
    }
    return err
}
```

### Graceful Degradation

| Failure Mode | Degradation Strategy |
|--------------|---------------------|
| Redis Pub/Sub down | Queue actions, serve from local gateway cache |
| Gateway overloaded | Reject new connections, existing viewers continue |
| Database down | Serve from Redis state cache, queue writes |
| Network partition | Each region operates independently on its viewers |

---

## Monitoring & Observability

### Key Metrics to Track

```yaml
Business Metrics:
  - live_sessions_active: Gauge
  - viewers_per_session: Histogram
  - actions_per_second: Counter

Latency Metrics:
  - action_broadcast_latency_ms: Histogram (P50, P95, P99)
  - websocket_round_trip_ms: Histogram
  - state_sync_duration_ms: Histogram

Infrastructure Metrics:
  - gateway_connections_active: Gauge per instance
  - redis_pubsub_messages_sec: Counter
  - websocket_errors: Counter by type

Alerts:
  - action_broadcast_latency_p99 > 400ms → Warning
  - action_broadcast_latency_p99 > 500ms → Critical
  - gateway_connections > 45000 → Scale up warning
```

---

## Security Considerations

```
┌──────────────────────────────────────────────────────────────────────┐
│                     SECURITY LAYERS                                   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  1. Authentication                                                    │
│     - JWT tokens for WebSocket connection (short-lived)               │
│     - Token refresh mechanism for long sessions                       │
│                                                                       │
│  2. Authorization                                                     │
│     - Only creator can send actions to their workflow                 │
│     - Viewers are read-only (server enforces)                         │
│     - Rate limiting: 100 actions/second per creator                   │
│                                                                       │
│  3. Input Validation                                                  │
│     - Sanitize node labels (XSS prevention)                           │
│     - Validate position bounds                                        │
│     - Max nodes per workflow: 1000                                    │
│     - Max connectors per workflow: 2000                               │
│                                                                       │
│  4. DoS Protection                                                    │
│     - Connection rate limiting per IP                                 │
│     - Message size limits (16KB per action)                           │
│     - Viewer connection limits per workflow (100k cap enforced)       │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Interview Sound Bites

> **On Real-Time Architecture:**
> "We use Kafka as the commit log — ACK to creator only after Kafka confirms. Two consumer groups process independently: Redis consumer for real-time (state, pub/sub, undo), Postgres consumer for durability. Gateways subscribe to Redis Pub/Sub channels and fan out to viewers. No sticky sessions needed."

> **On Reliability & Correctness:**
> "We use pure event sourcing: Kafka log is the single source of truth, Redis and Postgres are materialized views with `applied_seq` watermarks. Any state can be rebuilt by replaying the log from a snapshot. Consumers are idempotent — if action.seq ≤ applied_seq, skip. This guarantees correctness under partial failures, lag, or restarts."

> **On State Management:**
> "Event sourcing with sequence numbers. Clients track their last confirmed seq_num. On reconnect, they request missed actions. If gap is too large, we send a full snapshot instead."

> **On Undo/Redo:**
> "Server-side stacks in Redis track what can be undone/redone (limit: 100). Undo pops from stack, creates NEW action with inverse payload, publishes to Kafka. Log is append-only — viewers just apply actions in sequence. New actions clear redo stack (new timeline, old future discarded)."

> **On Scaling:**
> "For 100M connections, we need ~2000 gateway instances. Each gateway subscribes to Redis Pub/Sub for its connected viewers' workflows. No sticky sessions — any gateway can serve any viewer."

---

## Technology Choices Summary

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Commit log | Kafka | Durable WAL, ACK before response, enables replay |
| Broadcast | Redis Pub/Sub | Ultra-low latency fan-out to gateways |
| State cache | Redis | Fast reads, undo stacks, action buffer for catch-up |
| Primary DB | PostgreSQL | ACID, queryable history, long-term storage |
| Real-time protocol | WebSocket | Bidirectional (viewers send catch-up requests), <50ms latency |
| Serialization | MessagePack | Compact binary, faster than JSON |
| Gateway language | Go | Excellent for high-concurrency WebSocket handling |

---

## Alternative Approaches Considered

### CRDTs (Conflict-free Replicated Data Types)

**Pros:**
- True real-time collaboration (multiple editors)
- Automatic conflict resolution

**Cons:**
- Overkill for single-creator broadcast
- Higher complexity
- Larger state payloads

**Decision:** Not needed since only one creator edits. Simple sequence-numbered event log suffices.

### WebRTC for Viewer Connections

**Pros:**
- Potentially lower latency (P2P)
- Reduce server load

**Cons:**
- Complex NAT traversal
- Unreliable for 100k viewers
- Need signaling infrastructure anyway

**Decision:** WebSocket through gateway servers gives us more control and simpler debugging.

### DynamoDB vs PostgreSQL for Cold Storage

Both are valid choices. Selection depends on access patterns and team familiarity.

**PostgreSQL Approach:**
```
workflows, nodes, connectors, action_log, users tables
Complex queries, joins, full-text search possible
Requires sharding at scale (e.g., Citus, manual sharding)
```

**DynamoDB Approach:**
```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  TABLE: nodes                          TABLE: connectors                            │
│  PK: workflow_id                       PK: workflow_id                              │
│  SK: node_id                           SK: connector_id                             │
│                                        GSI: workflow_id + source_node_id            │
│                                                                                      │
│  TABLE: workflows                      TABLE: action_log                            │
│  PK: creator_id                        PK: workflow_id                              │
│  SK: workflow_id                       SK: seq_num                                  │
│  GSI: workflow_id (direct lookup)      TTL: 90 days                                 │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

| Factor | PostgreSQL | DynamoDB |
|--------|------------|----------|
| **Access patterns** | Complex queries, joins | Key-based lookups |
| **Our patterns** | Load by workflow, get/put by ID | ✅ Key-based fits perfectly |
| **Scaling** | Requires sharding setup | Auto-scales horizontally |
| **Ops overhead** | Higher (sharding, replicas) | Lower (managed) |
| **Cost model** | Provisioned capacity | Pay per request option |
| **Transactions** | Full ACID | TransactWriteItems (limited) |
| **Search** | Built-in full-text | Needs OpenSearch integration |

**When to choose DynamoDB:**
- Access patterns are key-based (load by workflow, get by ID)
- Need auto-scaling without sharding complexity
- Team has DynamoDB experience
- No complex analytics or search requirements

**When to choose PostgreSQL:**
- Need complex queries: "Workflows with >50 nodes created last week"
- Need full-text search on workflow titles
- Team more familiar with SQL
- Already have PostgreSQL infrastructure

**For this system:** Either works. Our access patterns (load all nodes/connectors for workflow, get/update by ID) are key-based, making DynamoDB a clean fit without sharding overhead.

---

## Conclusion

This design achieves:
- ✅ **< 500ms latency** through hierarchical fan-out and edge gateways
- ✅ **100k viewers per creator** via sharded gateway fleet
- ✅ **1k concurrent creators** via workflow-sharded Redis
- ✅ **Network resilience** through sequence-numbered actions and catch-up protocol
- ✅ **Undo/Redo** via command pattern with inverse actions

The key insight is treating this as a **broadcast problem** (one-to-many) rather than **collaboration** (many-to-many), which simplifies state management significantly while still requiring sophisticated infrastructure for fan-out at scale.

