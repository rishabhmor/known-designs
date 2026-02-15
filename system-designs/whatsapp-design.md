# WhatsApp System Design

## Architecture Overview

WhatsApp is primarily a 1:1 messaging system with small groups, characterized by **low fan-out** (typically 1:1, rarely >100 members per group). This fundamentally shapes the architecture choices.

### Load Balancer Strategy

**WhatsApp uses simple L4 Load Balancer:**
- Round-robin or random distribution
- **No consistent hashing** - not by user_id, not by group_id
- Each user maintains ONE persistent WebSocket connection
- Server subscribes to Redis channels for ALL user's chats (multi-subscription pattern)

**Why no consistent hashing by group_id?**
- Users are in 10-50 groups simultaneously
- Need real-time updates from all groups
- Can't reconnect every time user switches chats (poor mobile UX)
- Server must listen to multiple group channels anyway

### Connection Architecture

```
┌──────────────────────────────────────────────────────┐
│ Initial Connection (Authentication & Discovery)      │
└────────────────────┬─────────────────────────────────┘
                     │
                     ▼
              ┌─────────────┐
              │  L7 LB/ALB  │ ← HTTP endpoints (login, APIs)
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │ API Servers │
              └─────────────┘

┌──────────────────────────────────────────────────────┐
│ WebSocket Connections (Real-time Messaging)          │
└────────────────────┬─────────────────────────────────┘
                     │
                     ▼
              ┌─────────────┐
              │  L4 LB      │ ← WebSocket upgrade (random/round-robin)
              └──────┬──────┘
                     │
                ┌────┴────┐
                ▼         ▼
           ┌────────┐ ┌────────┐
           │ Chat   │ │ Chat   │
           │Server1 │ │Server2 │
           └────────┘ └────────┘
                │         │
                └────┬────┘
                     ▼
              ┌─────────────┐
              │ Redis Pub/Sub│
              └─────────────┘
```

### WebSocket Connection Flow

**1. HTTP Upgrade Handshake**

WebSocket connections start as normal HTTP requests:

```
Client → L4 LB → Chat Server

Client sends:
GET /ws?user_id=123 HTTP/1.1
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==

Server responds:
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=

→ Same TCP connection now uses WebSocket protocol
```

**2. Multi-Subscription Pattern (WhatsApp Mobile Reality)**

```
Alice is in: Group A, Group B, Group C, chat with Bob

Connection:
- Alice → L4 LB → Server 1 (random assignment)
- Alice maintains ONE WebSocket to Server 1

Server 1 subscribes to Redis:
- channel "user:alice" (for personal messages)
- channel "group:A"
- channel "group:B"
- channel "group:C"

When Bob sends message in Group B:
- Bob's server publishes to Redis: channel "group:B"
- Multiple servers receive (including Server 1)
- Server 1 forwards to Alice's WebSocket

When Alice switches from Group B to Group C in UI:
- NO reconnection needed
- Server 1 already subscribed to both channels
- Just UI change on client side
```

**Key insight:** Can't use consistent hashing by group_id because user needs simultaneous updates from multiple groups.

### Key Architectural Principles

1. **Multi-subscription pattern** - One connection per user, server subscribes to all user's channels
2. **No consistent hashing** - Random distribution works fine for WhatsApp's use case
3. **L4 for WebSocket** - Simple, fast, no need for L7 inspection
4. **Pub/Sub for routing** - Redis handles message delivery to correct servers
5. **Don't try to colocate users** - Graph partitioning is computationally infeasible at scale

## Create Message Flow

### Overview
- Client generates unique `message_id` (UUID)
- Write to DynamoDB first (durability), then push to Redis Pub/Sub (real-time delivery)
- Polling for undelivered messages handles transient failures

### Flow

```
Client sends {message_id, chat_id, content, ...}
                    │
                    ▼
      ┌─────────────────────────────┐
      │   DynamoDB Transaction      │
      │   • Message table           │
      │     (condition:             │
      │     attribute_not_exists)   │
      │   • Inbox (each recipient)  │
      └─────────────┬───────────────┘
                    │
          ┌─────────┴─────────┐
          │                   │
       Success         ConditionalCheckFailed
          │                   │
          │            (already exists,
          │             skip to Pub/Sub)
          └─────────┬─────────┘
                    │
                    ▼
          ┌─────────────────┐
          │ Redis Pub/Sub   │
          │ (notify recipient)
          └────────┬────────┘
                   │
             ┌─────┴─────┐
             │           │
          Success      Fail
             │           │
             │        Retry once
             │           │
             │     ┌─────┴─────┐
             │   Success     Fail
             │     │           │
             │     │     Return retryable error
             │     │     (client retries, or
             │     │      polling catches it)
             ▼     ▼
          Return 200 OK
```

### Key Design Decisions

1. **Client-generated message ID** - Acts as idempotency key; no separate idempotency table needed
2. **Conditional write** - `attribute_not_exists(message_id)` on Message table prevents duplicates
3. **DynamoDB transaction limit** - 100 items per transaction; supports groups up to 100 members
4. **Durability first** - Message persisted before real-time notification attempted
5. **Fallback** - Periodic polling for undelivered messages handles edge cases (Redis failures, disconnections)

### Tables Involved

| Table | Purpose |
|-------|---------|
| Message | Stores message content, creator, timestamp |
| Inbox | Maps recipient_id → message_id for undelivered messages |

## Scaling Considerations

### Why WhatsApp Doesn't Use Consistent Hashing

**1. Multi-group membership constraint:**

```
Alice is in:
- Group A (would hash to server_1)
- Group B (would hash to server_2)
- Group C (would hash to server_3)
- Chat with Bob (would hash to server_4)

Problem: Which server does Alice connect to?
- Can't connect to 4 different servers
- Can't reconnect every time she switches chats (mobile UX)
- Must maintain ONE persistent connection

Solution: Multi-subscription pattern
- Alice connects to one server (random)
- Server subscribes to ALL her group channels
- Receives updates from all groups
```

**2. When consistent hashing by content_id DOES work:**

Only for **"active view pattern"** where user views ONE content at a time:

| System | Pattern | Connection Strategy | Works? |
|--------|---------|---------------------|--------|
| **YouTube live** | Active view | hash(video_id) | ✅ YES - viewing one video |
| **Live auction** | Active view | hash(auction_id) | ✅ YES - watching one auction |
| **Slack web** | Active view | hash(channel_id) | ✅ YES - viewing one channel |
| **WhatsApp mobile** | Multi-subscription | random | ❌ NO - in multiple groups |
| **Discord mobile** | Multi-subscription | random | ❌ NO - in multiple channels |

**3. Redis amplification is acceptable for WhatsApp:**

```
Group with 500 members:
- 500 members across 100 servers (~5 per server)
- Message published → 100 servers receive
- Inter-server amplification: 100x

Is this a problem?
- NO for WhatsApp's scale
- Redis can handle 100K+ publishes/sec
- Most groups are <10 members
- Large groups (>100) are rare
- Benefit of simplicity > optimization gains
```

### Connection Pattern Trade-offs

| System Type | Connection Pattern | Routing | Why |
|-------------|-------------------|---------|-----|
| **WhatsApp mobile** | Multi-subscription | Random | Users in multiple groups, need all updates |
| **Slack mobile** | Multi-subscription | Random | Users in multiple channels |
| **Slack web** | Active view | hash(channel_id) | View one channel at a time, can reconnect |
| **YouTube live** | Active view | hash(video_id) | View one video at a time |
| **Live auctions** | Active view | hash(auction_id) | View one auction at a time |

### Server-to-Server vs Pub/Sub

**Always use Pub/Sub**, even with consistent hashing:

**Why not server-to-server calls?**
```
Issues with direct server calls:
1. Dynamic topology - need real-time list of servers handling group
2. Two code paths - hot vs cold groups
3. Ordering complexity - need Lamport clocks or sequencer
4. Failure handling - retry logic per server
5. You're rebuilding Pub/Sub but worse
```

**Pub/Sub benefits:**
```
1. Single code path - always publish to Redis
2. Ordering guaranteed - single channel = total order
3. Dynamic membership - servers subscribe/unsubscribe transparently
4. Battle-tested - Slack, Discord use this at scale
```

**When hot groups split across multiple servers:**
```
Group 123 splits across 3 servers:
- Server 1, Server 2, Server 3 all subscribe to "group:123"
- Publish message → Redis sends to all 3 servers
- Each server broadcasts to local clients
- Trade-off: 3x Redis messages vs server overload

This is acceptable - consistent hashing reduces 95% of groups to 1 server
The 5% hot groups that need splitting get reliable delivery via Pub/Sub
```

### Alternative Approaches (Not Recommended for WhatsApp)

**1. Consistent hashing by user_id + server-to-server calls**

```
Problem: Trying to colocate users who message each other
- Graph partitioning is NP-hard
- Social graphs change constantly
- Influencers create hot spots
- Complex rebalancing when friendships change

Verdict: Not practical at scale
```

**2. Consistent hashing by group_id (active view pattern)**

```
Problem: Only works if users view ONE group at a time
- WhatsApp users need updates from all groups simultaneously
- Would require reconnecting when switching chats (poor UX)
- Mobile apps maintain persistent connections

When it works: Web apps (Slack web), live streams, auctions

Verdict: Wrong pattern for WhatsApp mobile
```

### Redis Channel Strategy

**Simple and effective:**

```go
// When user connects
func onUserConnect(userID string, groups []string) {
    // Subscribe to user's personal channel
    redis.Subscribe(fmt.Sprintf("user:%s", userID))

    // Subscribe to all group channels
    for _, groupID := range groups {
        redis.Subscribe(fmt.Sprintf("group:%s", groupID))
    }
}

// When message sent to group
func sendGroupMessage(groupID string, message Message) {
    // Write to DB for persistence
    db.Insert(message)

    // Publish to Redis (all servers with group members receive)
    redis.Publish(fmt.Sprintf("group:%s", groupID), message)
}
```

**Handling large groups:**
```
Group with 1000 members across 100 servers:
- Each server has ~10 members
- Message published → 100 servers receive
- Each server sends to ~10 local WebSocket connections
- Total: 100 inter-server + 1000 client messages

This is acceptable:
- Redis handles this easily
- Most groups are much smaller (<10 members)
- Simplicity > premature optimization
```

### Interview Answer Template

**Question: "Would you use consistent hashing for WhatsApp?"**

```
"No, I wouldn't use consistent hashing for WhatsApp because of the
multi-subscription pattern requirement.

Why it doesn't work:
- Users are in 10-50 groups simultaneously
- Need real-time updates from ALL groups
- Can't reconnect every time user switches chats (mobile UX)
- Each user maintains ONE persistent WebSocket connection

Architecture instead:
- L4 load balancer with random distribution
- User connects to one server (random assignment)
- Server subscribes to Redis channels for:
  * User's personal channel (user:123)
  * All groups user is in (group:A, group:B, group:C)
- When message sent to group, published to Redis
- All servers with group members receive and forward

Redis amplification is acceptable:
- Most groups are small (<10 members)
- Large groups are rare
- Redis easily handles this scale
- Benefit of simplicity > optimization gains

Consistent hashing by group_id only works for 'active view' patterns:
- YouTube live (viewing one video at a time)
- Slack web (viewing one channel, can reconnect)
- Live auctions (watching one auction)

WhatsApp mobile doesn't fit this pattern."
```

**Question: "What about trying to colocate users who message each other?"**

```
"That would require graph partitioning which is not practical:

1. NP-hard problem - computationally expensive at billion-user scale
2. Dynamic graph - friendships change constantly, requires rebalancing
3. Hot spots - influencers with millions of connections
4. Initial placement - new users have no connections yet
5. Massive complexity for minimal benefit

Real systems (WhatsApp, Facebook Messenger) use:
- Simple random/round-robin distribution
- Redis Pub/Sub for routing
- O(1) lookups instead of graph analysis
- Predictable load distribution

The engineering trade-off favors simplicity and reliability over
theoretical optimization that's hard to achieve in practice."
```
