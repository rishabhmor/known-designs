# Real-Time Updates Pattern: The Complete Guide

A comprehensive guide to designing real-time systems with consistent hashing, Redis Pub/Sub, and hot partition handling.

## Table of Contents
- [Core Principles](#core-principles)
- [Message Amplification](#message-amplification)
- [Consistent Hashing Strategy](#consistent-hashing-strategy)
- [Redis Pub/Sub Scaling](#redis-pubsub-scaling)
- [Hot Partition Handling](#hot-partition-handling)
- [Publishing Strategy: CDC vs Direct Publish](#publishing-strategy-cdc-vs-direct-publish)
- [Implementation Patterns](#implementation-patterns)
- [Production Considerations](#production-considerations)

---

## Core Principles

### First Principle: Understand Your Fan-out Ratio

The fan-out ratio determines your entire architecture:

| System Type | Fan-out | Example | Amplification |
|-------------|---------|---------|---------------|
| 1:1 Messaging | 1:1 | WhatsApp DM | Low |
| Group Chat | 1:10-100 | Slack channel | Medium |
| Live Auction | 1:10K | eBay bidding | High |
| Live Video | 1:100K-1M | YouTube live comments | Very High |

**Key Decision Tree:**

```
Is fan-out > 100?
├─ No  → Simple architecture (random LB + Pub/Sub)
└─ Yes → Use consistent hashing by content_id
    ├─ < 1000 viewers per content → Single server per content
    └─ > 1000 viewers per content → Add hot partition handling
```

---

## Message Amplification

### Two Levels of Amplification

Message amplification happens at TWO distinct levels:

1. **Inter-server (Redis → Servers)** - Can be optimized with consistent hashing
2. **Intra-server (Server → Clients)** - Cannot be avoided

### Example: Live Auction with 10,000 Viewers

**WITHOUT Consistent Hashing (Random Distribution):**
```
10,000 viewers randomly distributed across 100 servers (~100 per server)

Bid Update Flow:
1. Bid Service → Publish to Redis channel "auction:123"
2. Redis → Broadcasts to ALL 100 servers (each has some viewers)
3. Each server → Sends to ~100 local WebSocket clients

Message Count:
- Inter-server: 1 publish + 100 broadcasts = 101 messages
- Intra-server: 100 servers × 100 clients = 10,000 messages
- Total: 10,101 messages
```

**WITH Consistent Hashing (Colocated by auction_id):**
```
10,000 viewers ALL on Server 42 (via consistent hashing)

Bid Update Flow:
1. Bid Service → Publish to Redis channel "auction:123"
2. Redis → Sends to ONLY Server 42 (only subscriber)
3. Server 42 → Sends to 10,000 local WebSocket clients

Message Count:
- Inter-server: 1 publish + 1 broadcast = 2 messages
- Intra-server: 1 server × 10,000 clients = 10,000 messages
- Total: 10,002 messages

Savings: 100x reduction in inter-server traffic
```

### Critical Insight

**Consistent hashing reduces inter-server amplification from 100x to 1x.**

The intra-server amplification (10,000 messages to clients) is unavoidable regardless of architecture.

---

## Consistent Hashing Strategy

### When to Use Consistent Hashing

**DO use consistent hashing by content_id when:**
- Fan-out > 100 (live auctions, live videos, live broadcasts)
- Multiple users watching/participating in same content
- Goal: Colocate all viewers of content X on same server(s)

**DON'T use consistent hashing by content_id when:**
- Fan-out < 100 (1:1 messaging, small groups)
- Random distribution is sufficient
- Keep architecture simple

**Example Hash Keys:**
```
Live Auction:    hash(auction_id)  → server
Live Video:      hash(video_id)    → server
Live Broadcast:  hash(stream_id)   → server
Chat Room:       hash(room_id)     → server

NOT:
WhatsApp 1:1:    hash(user_id)     → Different purpose (user state)
```

### How Consistent Hashing Works

**Basic Flow:**

```
1. Client Request: Connect to auction:123

2. Load Balancer:
   - Extract auction_id = 123
   - Compute: server_index = hash(123) % num_servers
   - Route to: server_42

3. Server 42:
   - Accept WebSocket connection
   - Subscribe to Redis channel "auction:123"
   - Keep connection open

4. When bid arrives:
   - Bid Service → Redis.Publish("auction:123", bid_data)
   - Redis → Only Server 42 receives (only subscriber)
   - Server 42 → Broadcast to all 10,000 local WebSockets
```

**Virtual Nodes (Production Pattern):**

Instead of direct mapping:
```
auction:123 → hash → server_42
```

Use virtual nodes:
```
auction:123 → hash → vnode_500 → server_42

1024 vnodes mapped to 100 servers:
- vnodes 0-9    → server_1
- vnodes 10-19  → server_2
- vnodes 500-509 → server_42
```

Benefits:
- Better load distribution
- Easier to add/remove servers (only affects adjacent vnodes)
- Supports hot partition sharding (reassign vnodes)

---

## Redis Pub/Sub Scaling

### First Principle: Channels vs Subscriptions

**Channels are cheap. Subscriptions are expensive.**

- **Channel**: Named topic in Redis (e.g., "auction:123")
  - Memory: ~100 bytes per channel
  - 1M channels = ~100MB
  - Redis can handle millions of channels

- **Subscription**: Server connection subscribed to channel
  - CPU to route each message to subscribers
  - Memory for connection state
  - Practical limit: ~50K-100K subscriptions per Redis instance

### When to Shard Redis

**Decision Formula:**
```
Shard when: (num_servers × channels_per_server) > 50K subscriptions
```

**Examples:**

```
Scenario 1: 100 servers, 10K videos each
- Subscriptions = 100 × 10K = 1M
- Need: 1M / 50K = 20 Redis instances minimum

Scenario 2: 10 servers, 1K videos each
- Subscriptions = 10 × 1K = 10K
- Single Redis instance is fine
```

### Channel Strategy: 1:1 Mapping

**Always use 1:1 channel per content:**

```
1M videos = 1M channels

video:0      → channel "video:0"
video:1      → channel "video:1"
video:999999 → channel "video:999999"
```

**DO NOT shard individual channels:**
```
# Wrong:
video:123 → video:123:shard-0
          → video:123:shard-1
          → video:123:shard-2

# Right:
video:123 → channel "video:123" on Redis-7
```

### Redis Sharding Patterns

**Pattern 1: Simple Redis Sharding (No Alignment)**

```
100 servers, 100 Redis instances

Server 42:
- Handles 10K videos (via consistent hashing)
- Videos distributed across all 100 Redis instances
- Connects to ALL 100 Redis instances
- Each connection subscribes to ~100 channels
- Total: 100 connections, 10K subscriptions

Each Redis instance:
- 100 servers connected
- Each subscribes to ~100 channels
- Total: 10K subscriptions per Redis ✓

Pros: Solves subscription bottleneck
Cons: Each server maintains 100 connections
```

**When to use:** < 100 servers, connection count not a bottleneck

**Pattern 2: Aligned Hashing (Optimized)**

```
100 servers, 100 Redis instances
Both use same hash function: hash(video_id) % 100

Server 42:
- Handles videos that hash to index 42
- These videos also on Redis-42
- Only connects to Redis instances with its videos (~10)
- Total: 10 connections, 10K subscriptions

Each Redis instance:
- Only ~10 servers connected (not 100)
- Total: 10K subscriptions per Redis ✓

Pros: 10x fewer connections per server
Cons: More complex, requires coordination
```

**When to use:** > 1000 servers, connection limits become issue

**Connection Limit Example:**
```
1000 servers, 100 Redis instances

Without alignment:
- Each server connects to all 100 Redis = 100 connections/server
- Each Redis receives 1000 connections = 1000 connections/Redis ✓

With alignment:
- Each server connects to ~10 Redis = 10 connections/server
- Each Redis receives ~100 connections = 100 connections/Redis ✓
- Stays well under Redis 10K max client limit
```

### Redis Instance Selection

```go
// Server determines which Redis to subscribe
func subscribeToVideo(videoID int) {
    // Shard videos across Redis instances
    redisIndex := hash(videoID) % numRedisInstances

    conn := redisPool[redisIndex]
    conn.Subscribe(fmt.Sprintf("video:%d", videoID))
}

// Publisher also uses same sharding
func publishComment(videoID int, comment string) {
    redisIndex := hash(videoID) % numRedisInstances

    redis[redisIndex].Publish(
        fmt.Sprintf("video:%d", videoID),
        comment,
    )
}
```

---

## Hot Partition Handling

### The Hot Partition Problem

**Scenario:**
```
Auction 123: 50,000 viewers
Consistent hashing: ALL on Server 42

Server 42:
- 50,000 WebSocket connections
- High CPU for message broadcasting
- High memory for connection state
- Server overloaded

Other servers: Idle
```

**This is the primary trade-off of consistent hashing.**

### Detection Strategies

**Strategy 1: Real-Time Metrics Monitoring**

```
Each server reports every 10 seconds:
{
  "server_id": "42",
  "auction_id": 123,
  "connections": 6000,
  "cpu_usage": 75%,
  "timestamp": "2026-02-08T10:00:00Z"
}

Hot Partition Detector:
- Aggregates metrics across servers
- Threshold: connections > 5000 per auction
- Action: Trigger shard split
```

**Strategy 2: Pre-Configured Hot Events**

```
For predictable hot events (product launches, major sports):

ZooKeeper config:
{
  "auction:999": {
    "hot": true,
    "shards": 50,
    "reason": "iPhone 15 launch, expect 50K viewers"
  }
}

Applied before event starts
```

### Sharding Hot Content

**Key Principle: Don't move existing connections, route new ones to new servers**

**Flow:**

```
Initial State:
- Auction 123: 1000 viewers on Server 42
- Growing rapidly

Detection at 6000 viewers:
- Hot partition detector triggers
- Decision: Split into 6 servers

ZooKeeper Update:
{
  "auction:123": {
    "hot": true,
    "shards": 6,
    "servers": [42, 43, 44, 45, 46, 47],
    "strategy": "least-connections"
  }
}

Load Balancer Reload:
- Watches ZooKeeper path
- Updates routing table

New Connections:
- Load balancer checks: auction:123 is hot
- Routes new connections across 6 servers (least-connections)
- Server 42 keeps existing 6000 connections
- Servers 43-47 accept new connections

Eventual State:
- 10,000 viewers across 6 servers
- Server 42: 6000 (legacy)
- Servers 43-47: ~800 each (new)
```

### Implementation: Load Balancer Logic

```go
func routeConnection(auctionID int) string {
    // Check ZooKeeper for hot auction config
    hotConfig := zkClient.Get("/hot-auctions/" + auctionID)

    if hotConfig != nil && hotConfig.Hot {
        // Hot auction: use shard-aware routing
        return leastLoadedServer(hotConfig.Servers)
    }

    // Cold auction: normal consistent hashing
    vnode := hash(auctionID) % 1024
    return vnodeToServer[vnode]
}

func leastLoadedServer(servers []string) string {
    // Query each server for current connection count
    minLoad := math.MaxInt
    selectedServer := ""

    for _, server := range servers {
        load := getServerLoad(server)  // Cached, updated every 5s
        if load < minLoad {
            minLoad = load
            selectedServer = server
        }
    }

    return selectedServer
}
```

### Redis Pub/Sub for Hot Content

**Important: All sharded servers subscribe to SAME channel**

```
Auction 123 split across 6 servers:

Each server subscribes:
- Server 42: Redis.Subscribe("auction:123")
- Server 43: Redis.Subscribe("auction:123")
- Server 44: Redis.Subscribe("auction:123")
- Server 45: Redis.Subscribe("auction:123")
- Server 46: Redis.Subscribe("auction:123")
- Server 47: Redis.Subscribe("auction:123")

When bid published:
- Bid Service → Redis.Publish("auction:123", bid)
- Redis → Sends to ALL 6 servers (6 messages)
- Each server broadcasts to local clients

Trade-off:
- Without sharding: 1 Redis message, 1 overloaded server
- With sharding: 6 Redis messages, balanced load across 6 servers
```

**This is acceptable:** 6x Redis traffic vs server overload is worth it.

### Connection Rebalancing (Advanced)

**Optional: Gradually rebalance existing connections**

```go
// Server 42 detects it's overloaded
if myConnections > targetPerServer*1.5 {
    // Close oldest 10% of connections with "rebalance" close code
    for i := 0; i < len(connections)/10; i++ {
        conn := connections[i]
        conn.Close(websocket.StatusServiceRestart, "rebalancing")
    }
}

// Client receives close with StatusServiceRestart
// Immediately reconnects
// Load balancer routes to less loaded server
```

**Trade-off:** Brief reconnection disruption vs better load distribution

---

## Publishing Strategy: CDC vs Direct Publish

A critical architectural decision: How do you publish updates to Redis after writing to your database?

### The Core Problem: Dual Write

When you need both persistence (DB) and real-time updates (Redis Pub/Sub), you face the **dual write problem**:

```
Write to DB ✓
Publish to Redis ✗ (Network failure, Redis down, etc.)

Result: Data persisted but no real-time update sent
```

or vice versa:

```
Publish to Redis ✓
Write to DB ✗ (DB error, transaction rollback, etc.)

Result: Users see update that doesn't exist in DB
```

**You cannot guarantee atomicity across two separate systems.**

### Strategy 1: Direct Publish from Runtime + Client Catchup

**Architecture:**
```
Bid Service:
1. Write to DB (source of truth)
2. If success → Publish to Redis (async, fire-and-forget)
3. Return success to client

Client Catchup:
- Bids have sequence numbers
- Client detects gaps: last_seen=100, new bid arrives with seq=105
- Request: GET /auction/123/bids?after_seq=100
- Server returns missing bids from DB
```

**Implementation:**
```go
func placeBid(auctionID, bidID, amount int) error {
    // 1. Write to DB (source of truth)
    err := db.Transaction(func(tx) {
        bid := Bid{
            ID:       bidID,
            AuctionID: auctionID,
            Amount:   amount,
            Sequence: getNextSequence(auctionID),
        }
        return tx.Insert("bids", bid)
    })

    if err != nil {
        return err  // Bid rejected
    }

    // 2. Publish to Redis (best effort, async)
    go func() {
        redisErr := redis.Publish(
            fmt.Sprintf("auction:%d", auctionID),
            bid,
        )

        if redisErr != nil {
            log.Error("Failed to publish to Redis", redisErr)
            metrics.IncrementMissedPublish()
            // Don't fail the bid - client will catchup
        }
    }()

    return nil  // Bid accepted
}
```

**Client catchup:**
```go
// Client detects sequence gap
func handleBidUpdate(bid Bid) {
    if bid.Sequence > lastSeenSeq + 1 {
        // Gap detected! Missed bids between lastSeenSeq and bid.Sequence
        missedBids := fetchMissedBids(auctionID, lastSeenSeq)

        for _, missed := range missedBids {
            displayBid(missed)
        }
    }

    displayBid(bid)
    lastSeenSeq = bid.Sequence
}

func fetchMissedBids(auctionID, afterSeq int) []Bid {
    resp := http.Get(fmt.Sprintf(
        "/auction/%d/bids?after_seq=%d",
        auctionID,
        afterSeq,
    ))

    return parseResponse(resp)
}
```

**When to use:**
- **Latency critical** (< 100ms updates required)
- **High availability requirements** for core writes
- **Occasional miss acceptable** with quick recovery
- **Simple architecture** preferred

**Pros:**
- Lowest latency (10-50ms)
- DB write succeeds even if Redis is down
- Simple, no CDC infrastructure
- Works with any database

**Cons:**
- Users miss updates when Redis/network fails
- Requires client-side catchup logic
- Small window of inconsistency

**Examples:** WhatsApp messages, live comments, auction bids, notifications

---

### Strategy 2: Change Data Capture (CDC) with Debezium

**Architecture:**
```
Bid Service → Write to DB only

CDC (Debezium):
- Monitors DB transaction log (PostgreSQL WAL, MySQL binlog)
- Captures INSERT/UPDATE/DELETE
- Publishes to Kafka automatically

Kafka Consumer:
- Reads from Kafka
- Publishes to Redis Pub/Sub
```

**Implementation:**
```go
// Bid Service - ONLY writes to DB
func placeBid(auctionID, bidID, amount int) error {
    return db.Transaction(func(tx) {
        bid := Bid{
            ID:       bidID,
            AuctionID: auctionID,
            Amount:   amount,
            Sequence: getNextSequence(auctionID),
        }
        return tx.Insert("bids", bid)
    })

    // That's it! CDC handles the rest
}
```

**Debezium config:**
```json
{
  "name": "bids-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.dbname": "auction_db",
    "table.include.list": "public.bids",
    "topic.prefix": "cdc",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "cdc.public.bids",
    "transforms.route.replacement": "bid-events"
  }
}
```

**Kafka Consumer:**
```go
func consumeCDCEvents() {
    for msg := range kafka.Consume("bid-events") {
        cdcEvent := msg.Value

        if cdcEvent.Op == "c" {  // CREATE operation
            bid := cdcEvent.After

            // Publish to Redis with retries
            err := redis.Publish(
                fmt.Sprintf("auction:%d", bid.AuctionID),
                bid,
            )

            if err != nil {
                return err  // Don't ack, Kafka will retry
            }
        }

        msg.Ack()
    }
}
```

**When to use:**
- **Reliability paramount** (can't miss any updates)
- **Financial transactions** (payments, orders, inventory)
- **Compliance requirements** (audit logs)
- **Latency tolerance** (100-500ms acceptable)

**Pros:**
- No dual write problem (single write to DB)
- Guaranteed consistency (DB is single source of truth)
- Automatic - no manual publish logic
- Can replay from transaction log

**Cons:**
- CDC lag: 100-500ms latency
- More infrastructure (Debezium, Kafka Connect)
- Harder to debug (implicit propagation)
- Schema evolution complexity

**Examples:** Payment confirmations, order status, inventory updates, audit logs

---

### Strategy 3: Kafka-First (Event Sourcing)

**Architecture:**
```
Bid Service → Write to Kafka only

Kafka Consumer:
├─> Write to DB
└─> Publish to Redis
```

**Implementation:**
```go
// Bid Service - ONLY writes to Kafka
func placeBid(bid BidRequest) error {
    err := kafka.Publish("bid-events", bid)
    if err != nil {
        return err  // Bid rejected
    }

    return nil  // Return 202 Accepted
}

// Consumer does BOTH atomically
func consumeBids() {
    for msg := range kafka.Consume("bid-events") {
        bid := msg.Value

        // Check idempotency
        result := db.Execute(`
            INSERT INTO bids (id, auction_id, amount, published)
            VALUES (?, ?, ?, false)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
        `, bid.ID, bid.AuctionID, bid.Amount)

        if result.RowsAffected == 0 {
            msg.Ack()  // Already processed
            continue
        }

        // Publish to Redis
        redis.Publish(fmt.Sprintf("auction:%d", bid.AuctionID), bid)

        // Mark as published
        db.Update("bids", bid.ID, map[string]any{"published": true})

        msg.Ack()
    }
}
```

**Client handling:**
```go
// Return 202 Accepted immediately
func placeBidHandler(w http.ResponseWriter, r *http.Request) {
    bid := parseBid(r)

    err := kafka.Publish("bid-events", bid)
    if err != nil {
        w.WriteHeader(500)
        return
    }

    w.WriteHeader(202)  // Accepted, processing async
    json.NewEncoder(w).Encode(map[string]string{
        "status": "accepted",
        "bid_id": bid.ID,
    })
}

// Client receives confirmation via WebSocket
// when consumer finishes processing
```

**Idempotency considerations:**

```go
// Handle duplicate Kafka messages (retries)
func consumeBids() {
    for msg := range kafka.Consume("bid-events") {
        bid := msg.Value

        // 1. Check if already processed (DB as barrier)
        result := db.Execute(`
            INSERT INTO bids (id, ..., published)
            VALUES (?, ..., false)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
        `)

        if result.RowsAffected == 0 {
            // Duplicate - check if needs Redis publish
            existing := db.Query("SELECT published FROM bids WHERE id = ?", bid.ID)

            if !existing.Published {
                // DB insert succeeded before, but Redis publish failed
                redis.Publish(...)
                db.Update("bids", bid.ID, {"published": true})
            }

            msg.Ack()
            continue
        }

        // 2. New bid - publish to Redis
        redis.Publish(...)

        // 3. Mark as published
        db.Update("bids", bid.ID, {"published": true})

        msg.Ack()
    }
}
```

**When to use:**
- **Event-sourced architecture** already in place
- **Async processing acceptable** (202 Accepted pattern)
- **Kafka reliability sufficient** (99.95%+)
- **Latency < 200ms acceptable**

**Pros:**
- No dual write problem (Kafka is source of truth)
- Kafka retries handle failures automatically
- Scales horizontally with Kafka consumers
- Event log for replay/debugging

**Cons:**
- Kafka in critical path (downtime = can't accept writes)
- Requires idempotency handling
- Poison pill messages can block consumer
- More complex than direct publish

**Critical: Needs dead letter queue:**
```go
func consumeBids() {
    for msg := range kafka.Consume("bid-events") {
        bid := msg.Value

        err := processBid(bid)

        if err != nil {
            retryCount := msg.Headers["retry-count"]

            if retryCount > 3 {
                // Send to DLQ, don't block consumer
                kafka.Publish("bid-events-dlq", msg)
                msg.Ack()
                alertOps("Bid sent to DLQ", bid.ID)
                continue
            }

            // Retry with backoff
            return err  // Don't ack, Kafka retries
        }

        msg.Ack()
    }
}
```

**Examples:** Event-sourced systems, microservices with Kafka backbone

---

### Decision Matrix

| Pattern | Latency | Reliability | When to Use |
|---------|---------|-------------|-------------|
| **Direct Publish + Catchup** | 10-50ms | 99.9% | Messaging, live comments, auctions |
| **CDC (Debezium)** | 100-500ms | 99.99%+ | Payments, inventory, audit logs |
| **Kafka-First** | 50-200ms | 99.95% | Event-sourced architectures |

### The First Principle

**For latency-critical real-time updates (WhatsApp, live auctions, comments):**
- Use **Direct Publish + Catchup**
- Latency matters more than perfect delivery
- Users expect instant feedback
- Catchup mechanism handles rare failures

**For reliability-critical updates (payments, inventory, compliance):**
- Use **CDC**
- Guaranteed delivery matters more than latency
- 100-500ms lag is acceptable
- Can't miss any updates

**For event-sourced systems:**
- Use **Kafka-First**
- Accept async nature (202 Accepted)
- Need proper idempotency and DLQ handling
- Good middle ground if Kafka already in stack

### Interview Answer

**Question:** "Should you use CDC or direct publish for auction live updates?"

**Your Answer:**

"For auction systems, I'd use **direct publish with client catchup**. Here's why:

**Architecture:**
- Write bid to DB (source of truth)
- Publish to Redis asynchronously (fire-and-forget)
- Each bid has a sequence number
- Client detects gaps and requests missed bids from DB

**Trade-off:**
Direct publish gives us 10-50ms latency, which is critical for auction UX - users expect instant bid updates. The risk is that if Redis/network fails, some users temporarily miss updates.

CDC would solve the reliability problem but adds 100-500ms lag. For auctions, that latency is unacceptable - users would see stale bids and potentially overbid.

The catchup mechanism handles the 0.1% failure case. Redis has 99.9%+ availability, so most updates are instant. When failures occur, clients recover within seconds via sequence-based catchup.

**When I'd use CDC instead:**
If this were financial transactions (payment confirmations, inventory deductions) where we absolutely cannot miss updates and 200ms lag is acceptable, I'd use CDC. But for live updates where latency matters more, direct publish wins."

---

## Implementation Patterns

### Pattern 1: Smart Load Balancer (Most Common)

**Architecture:**
```
Client → Load Balancer → Server Pool
         ↓
         ZooKeeper (service discovery)
```

**Components:**

**Load Balancer:**
- Extracts content_id from URL/header
- Performs consistent hashing
- Routes to specific server
- Watches ZooKeeper for server health

**ZooKeeper:**
- Service discovery only
- Servers register as ephemeral nodes
- Load balancer subscribes to changes

**Example: HAProxy**

```haproxy
backend websocket_servers
    mode http
    balance hashing
    hash-type consistent

    # Extract auction_id from URL /auction/{id}/ws
    http-request set-var(txn.auction_id) path,field(3,/)

    # Use auction_id for consistent hashing
    hash-key %[var(txn.auction_id)]

    # Server pool (populated from ZooKeeper)
    server ws-1 10.0.1.1:8080 check
    server ws-2 10.0.1.2:8080 check
    server ws-42 10.0.1.42:8080 check
```

**Example: Envoy**

```yaml
clusters:
- name: websocket_cluster
  type: EDS  # Endpoint Discovery from ZooKeeper/Consul
  lb_policy: RING_HASH
  ring_hash_lb_config:
    minimum_ring_size: 1024
    maximum_ring_size: 8192
    hash_function: XX_HASH
```

**Example: NGINX**

```nginx
upstream websocket_backend {
    hash $arg_auction_id consistent;

    server 10.0.1.1:8080;
    server 10.0.1.2:8080;
    server 10.0.1.42:8080;
}

server {
    location /auction/ {
        proxy_pass http://websocket_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

**Pros:**
- Simple architecture
- Centralized routing logic
- Easy to update server pool

**Cons:**
- Load balancer is single point of failure
- All traffic goes through LB (latency hop)

**When to use:** Most scenarios, standard architecture

---

### Pattern 2: Service Mesh / Sidecar (Modern)

**Architecture:**
```
Client → Gateway → Control Plane → ZooKeeper
                    ↓
              Envoy sidecars
```

**Components:**

**Gateway:** Envoy proxy doing consistent hashing

**Control Plane:** Pilot/Consul
- Reads ZooKeeper for server list
- Generates Envoy config
- Pushes to Envoy via xDS protocol

**Flow:**
```
1. ZooKeeper has server list
2. Control plane reads ZooKeeper
3. Control plane generates Envoy config with hash ring
4. Pushes config to Envoy via xDS API
5. Envoy does consistent hashing locally
```

**Envoy doesn't talk to ZooKeeper directly** - control plane is middleman.

**Example Systems:**
- Istio (Pilot is control plane)
- Consul Connect
- AWS App Mesh

**Pros:**
- Decentralized routing
- Rich observability
- Advanced traffic management

**Cons:**
- Complex infrastructure
- Higher resource overhead
- Steep learning curve

**When to use:** Microservices with service mesh already in place

---

### Pattern 3: Client-Side Discovery (Billion-Scale)

**Architecture:**
```
Client → Discovery Service → Returns server address
  ↓
  └─> Connect directly to Server 42
```

**Flow:**

```
1. Client: HTTP GET /discover/auction/123

2. Discovery Service:
   servers = zookeeper.getChildren("/servers")

   if isHot(123):
       # Hot auction: return multiple servers
       return {
           "servers": ["ws-42.internal", "ws-43.internal"],
           "strategy": "random"
       }
   else:
       # Cold auction: single server
       index = hash(123) % len(servers)
       return {
           "server": servers[index]
       }

3. Client: Direct WebSocket to wss://ws-42.internal:8080
   - No load balancer in data path
   - Minimal latency
```

**Discovery Service:**
```go
func discoverServer(auctionID int) DiscoveryResponse {
    servers := zk.GetChildren("/websocket-servers")

    hotConfig := zk.Get("/hot-auctions/" + auctionID)
    if hotConfig != nil {
        // Hot auction
        return DiscoveryResponse{
            Servers: hotConfig.Servers,
            Strategy: "random",
        }
    }

    // Cold auction
    index := hash(auctionID) % len(servers)
    return DiscoveryResponse{
        Server: servers[index],
    }
}
```

**Pros:**
- No load balancer bottleneck
- Direct connection, lowest latency
- Scales to billions of connections

**Cons:**
- Client complexity
- Server addresses exposed to clients
- Network topology constraints

**When to use:** Billion+ scale systems (WhatsApp, Discord voice)

---

### Pattern 4: L4 LB for Low Fan-out (Simple)

**When consistent hashing doesn't matter:**

```
WhatsApp 1:1 messaging:
- User A on Server 1
- User B on Server 2
- Message must reach both servers anyway
- Consistent hashing provides NO benefit

Architecture:
Client → L4 LB (random/round-robin) → Servers
```

**L4 Load Balancer:**
- TCP termination only
- No HTTP header inspection
- Simple round-robin or least-connections
- High throughput, low latency

**When to use:**
- 1:1 messaging
- Small group chats (< 100 people)
- Low fan-out scenarios
- Simplicity over optimization

---

## Production Considerations

### ZooKeeper Data Structure

```
/websocket-cluster/
├── servers/                           # Ephemeral nodes
│   ├── server-1                       # {"ip": "10.0.1.1", "port": 8080, "capacity": 10000}
│   ├── server-2                       # {"ip": "10.0.1.2", "port": 8080, "capacity": 10000}
│   └── server-42                      # {"ip": "10.0.1.42", "port": 8080, "capacity": 10000}
│
├── hot-auctions/                      # Persistent nodes
│   ├── auction-123                    # {"shards": 6, "servers": [42,43,44,45,46,47], "since": "2026-02-08T10:00:00Z"}
│   └── auction-456                    # {"shards": 10, "servers": [...], "since": "2026-02-08T11:30:00Z"}
│
└── config/
    ├── shard-threshold                # {"connections": 5000}
    └── redis-instances                # {"endpoints": ["redis-1:6379", ...]}
```

### Server Registration

```go
func registerServer() {
    // Create ephemeral node
    path := fmt.Sprintf("/servers/server-%d", serverID)

    data := ServerInfo{
        IP: getLocalIP(),
        Port: 8080,
        Capacity: 10000,
        Zone: "us-west-2a",
    }

    zk.Create(path, data, zookeeper.FlagEphemeral)

    // If server dies, ephemeral node auto-deleted
    // Load balancer sees change, updates hash ring
}
```

### Connection Count Tracking

```go
// Server exposes metrics endpoint
func metricsHandler(w http.ResponseWriter, r *http.Request) {
    metrics := make(map[int]int)

    for _, conn := range activeConnections {
        auctionID := conn.AuctionID
        metrics[auctionID]++
    }

    json.NewEncoder(w).Encode(MetricsResponse{
        ServerID: serverID,
        Auctions: metrics,
        TotalConnections: len(activeConnections),
        CPUUsage: getCPUUsage(),
        Timestamp: time.Now(),
    })
}

// Hot partition detector polls servers
func detectHotPartitions() {
    ticker := time.NewTicker(10 * time.Second)

    for range ticker.C {
        allMetrics := collectMetricsFromAllServers()

        auctionStats := aggregateByAuction(allMetrics)

        for auctionID, stats := range auctionStats {
            if stats.TotalConnections > 5000 {
                currentShards := len(stats.Servers)
                neededShards := (stats.TotalConnections / 1000) + 1

                if neededShards > currentShards {
                    triggerShardSplit(auctionID, neededShards)
                }
            }
        }
    }
}
```

### Handling Server Failures

**Consistent hash ring rebalancing:**

```
Before failure:
- 100 servers, 1024 vnodes
- Server 42 handles vnodes 420-429 (10 vnodes)

Server 42 dies:
- ZooKeeper ephemeral node deleted
- Load balancer gets notification
- Rebuilds hash ring with 99 servers
- Vnodes 420-429 remapped to other servers

Impact:
- Existing connections on Server 42: LOST (clients reconnect)
- Other auctions: ~1% get remapped to different servers
- Clients reconnect automatically, new hash ring applies
```

**Minimizing disruption:**

```go
// Use large vnode count
vnodeCount := numServers * 10  // 1000 vnodes for 100 servers

// When server fails, only ~1% of auctions remap
percentRemapped := 1.0 / numServers
```

**Replicated subscriptions (advanced):**

```go
// Subscribe to each auction on 2 servers
func subscribeWithReplica(auctionID int) {
    primary := hash(auctionID) % numServers
    replica := hash(auctionID + salt) % numServers

    if serverID == primary || serverID == replica {
        redis.Subscribe("auction:" + auctionID)
    }
}

// Client connects to primary, falls back to replica
```

### Redis Connection Pooling

```go
// Server with 10K subscriptions across 10 Redis instances
type RedisSubscriber struct {
    pools map[int]*redis.Client  // redis index -> client
}

func (rs *RedisSubscriber) Subscribe(videoID int) {
    redisIndex := hash(videoID) % numRedisInstances

    client, exists := rs.pools[redisIndex]
    if !exists {
        client = redis.NewClient(&redis.Options{
            Addr: redisEndpoints[redisIndex],
        })
        rs.pools[redisIndex] = client

        // Start message receiver for this connection
        go rs.receiveMessages(client)
    }

    client.Subscribe(fmt.Sprintf("video:%d", videoID))
}

func (rs *RedisSubscriber) receiveMessages(client *redis.Client) {
    pubsub := client.Subscribe()

    for msg := range pubsub.Channel() {
        // msg.Channel = "video:123"
        // msg.Payload = comment data

        // Broadcast to local WebSocket clients
        broadcastToLocalClients(msg.Channel, msg.Payload)
    }
}
```

### Performance Tuning

**Server capacity limits:**
```
Single server limits:
- WebSocket connections: 10K-50K (depends on message rate)
- Memory: ~10KB per connection = 100MB for 10K connections
- CPU: Broadcasting is expensive, use efficient serialization

Per-auction limits:
- < 1000 viewers: Single server
- 1K-10K viewers: Monitor closely, prepare to shard
- > 10K viewers: Auto-shard mandatory
```

**Optimization strategies:**
```go
// 1. Message batching
// Instead of: bid@$100 → bid@$101 → bid@$102
// Batch: [bid@$100, bid@$101, bid@$102] every 100ms

type BatchPublisher struct {
    buffer map[string][]Message
    mu     sync.Mutex
}

func (bp *BatchPublisher) Start() {
    ticker := time.NewTicker(100 * time.Millisecond)
    for range ticker.C {
        bp.flush()
    }
}

func (bp *BatchPublisher) flush() {
    bp.mu.Lock()
    defer bp.mu.Unlock()

    for channel, messages := range bp.buffer {
        if len(messages) > 0 {
            batched := BatchMessage{Messages: messages}
            redis.Publish(channel, batched)
        }
    }

    bp.buffer = make(map[string][]Message)
}

// 2. Efficient serialization
// Use MessagePack or Protobuf instead of JSON
// 3x faster encoding, 50% smaller payload

// 3. Connection pooling
// Reuse WebSocket connections, avoid handshake overhead
```

---

## Interview Answer Template

### Question: "How would you design real-time updates for a live auction system?"

**Your Answer:**

"I'd use Redis Pub/Sub with consistent hashing. Here's the approach:

**Architecture:**
- Load balancer does consistent hashing on auction_id
- All viewers of auction 123 connect to the same server via hash routing
- Each server subscribes to Redis channels for its auctions only
- When a bid is published, Redis sends it to only the servers with active viewers

**Why this matters:**
Without consistent hashing, if 10,000 viewers are randomly distributed across 100 servers, we'd send 100 server-level messages for each bid update. With consistent hashing, we colocate all viewers on one server and send just 1 server-level message - a 100x reduction in inter-server traffic.

**Hot Partition Handling:**
For very popular auctions exceeding 5,000 connections, we'd detect the load and dynamically shard across multiple servers. The trade-off is sending N messages to N servers instead of 1, but it prevents server overload.

**Redis Scaling:**
At scale with millions of videos, we'd shard Redis instances as well. The bottleneck isn't channel count - it's subscription count. We'd distribute channels across multiple Redis instances to keep subscriptions per instance under 50K."

### Common Follow-ups

**Q: "What if a server dies?"**

"The consistent hash ring rebuilds when ZooKeeper detects the server failure. Existing connections are lost, but clients reconnect automatically and get routed to new servers. By using 10x more virtual nodes than servers, only ~1% of auctions need to remap. For critical auctions, we could use replicated subscriptions across 2 servers for failover."

**Q: "How do you handle hot auctions?"**

"We monitor connection counts per auction. When an auction exceeds our threshold (say 5,000 connections), we write a hot auction config to ZooKeeper specifying multiple servers. The load balancer watches this path and routes new connections across the shard pool using least-connections. Existing connections stay on their current server - we don't move them. This gradually balances the load."

**Q: "Why not use sticky sessions instead of consistent hashing?"**

"Sticky sessions solve different problems - connection stability and stateful operations. They don't reduce message amplification. Whether we use sticky sessions or not, if 10,000 viewers are spread across 100 servers, we must send messages to all 100 servers. Consistent hashing reduces this by colocating viewers, not by making connections sticky."

---

## System-Specific Patterns

### Live Auction (eBay, TicketMaster)
- **Fan-out:** 1:10K typical, 1:100K for hot items
- **Pattern:** Consistent hashing by auction_id mandatory
- **Hot handling:** Pre-configure for known hot events (concert tickets, limited drops)
- **Ordering:** Critical - use single Redis channel per auction

### Live Video Comments (YouTube, Twitch)
- **Fan-out:** 1:100K to 1:1M
- **Pattern:** Consistent hashing by video_id mandatory
- **Hot handling:** Auto-detection and sharding essential
- **Ordering:** Can relax - eventual consistency acceptable
- **Optimization:** CDN edge push for mega-popular streams

### WhatsApp / 1:1 Messaging
- **Fan-out:** 1:1
- **Pattern:** Don't use consistent hashing by conversation_id
- **Architecture:** Simple L4 LB with random distribution
- **Why:** Message must reach 2 servers regardless, consistency hashing adds complexity without benefit

### Group Chat (Slack, Discord)
- **Fan-out:** 1:10 to 1:100
- **Pattern:** Consistent hashing beneficial for large channels (>100 members)
- **Small channels:** Can use random distribution
- **Voice channels:** Client-side discovery for lowest latency

---

## Key Takeaways

1. **Understand your fan-out ratio** - it determines if consistent hashing is worth the complexity

2. **Two levels of amplification** - optimize inter-server (consistent hashing), accept intra-server (unavoidable)

3. **Channels are 1:1 with content** - don't shard individual channels, shard Redis instances

4. **Redis bottleneck is subscriptions** - not channel count, plan for 50K subscriptions per instance

5. **Hot partitions are inevitable** - plan for detection and sharding from the start

6. **Client-side discovery at billion-scale** - load balancers become bottleneck

7. **Alignment is optimization** - aligning server and Redis hashing reduces connections, not required for correctness

8. **Keep it simple for low fan-out** - don't over-engineer, random LB works fine for 1:1 messaging

---

## Further Reading

- Redis Pub/Sub internals: https://redis.io/docs/manual/pubsub/
- Consistent hashing deep dive: https://www.toptal.com/big-data/consistent-hashing
- Discord's real-time architecture: https://discord.com/blog/how-discord-stores-billions-of-messages
- WhatsApp at scale: https://www.youtube.com/watch?v=vvhC64hQZMk
