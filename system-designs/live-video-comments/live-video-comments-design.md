# Live Video Comments System Design

## Overview
A real-time comment delivery system for live videos (like Facebook Live, Instagram Live). Focuses on the comment delivery mechanism, not the video streaming part.

## Key Design Choice: Dispatcher Model over Pub/Sub

### Why not Pub/Sub?
- Redis pub/sub is fire-and-forget, no ACKs, no persistence
- Messages lost if subscriber disconnected at delivery time
- Subscriber tracking relies on TCP connection state (not instant detection)
- Millions of channels (one per video) = memory overhead nightmare

### Dispatcher Model Benefits
- Direct routing with explicit ACKs
- Controlled retry logic
- Better visibility into delivery status
- No message broker in the middle that could drop messages

---

## Architecture Components

### 1. Comment Management Service (CMS)
- Receives new comments via HTTP POST
- Persists to DynamoDB (source of truth)
- Calls Dispatcher for real-time delivery
- Retries on timeout to Dispatcher
- Once ACK received from Dispatcher → CMS is done

### 2. Dispatcher Service
- Maintains dynamic mapping: `videoId → [RMS nodes]`
- Consults Zookeeper/etcd for node registration
- Routes comments to correct RMS nodes
- Owns delivery responsibility after ACKing to CMS
- Retries to RMS nodes, skips if node marked dead

### 3. Realtime Messaging Service (RMS) Nodes
- Holds SSE connections to viewers
- Receives comments from Dispatcher
- Pushes to connected viewers
- Registers with Zookeeper on startup
- Multiple nodes can serve same popular video

### 4. Zookeeper/etcd
- Service discovery and coordination
- Tracks which RMS nodes are alive
- Maintains videoId → node mappings
- Ephemeral nodes for automatic cleanup on failure

---

## Client Connection Flow (Viewers)

### Protocol: SSE over WebSocket
- SSE is simpler, unidirectional (server→client)
- No protocol upgrade overhead
- Native HTTP - works with proxies, CDNs, load balancers
- Auto-reconnect built into browser API
- WebSocket overkill when viewers mostly just receive

### Connection Flow
1. Viewer opens live video
2. Client establishes SSE connection to Load Balancer
3. LB routes to RMS node (round-robin among nodes serving that videoId)
4. RMS adds connection to its viewer pool for that video
5. Comments flow: Dispatcher → RMS → SSE → Viewer

---

## Failure Handling

### RMS Node Dies

**For existing viewers:**
- SSE connection drops
- Client auto-reconnects (built into SSE)
- LB routes to healthy node
- Client sends `lastSeenCommentId` for catch-up
- Server fetches missed comments from DB/cache

**For Dispatcher:**
- Zookeeper detects via missed heartbeats (~10 seconds)
- Dispatcher can detect faster via failed push attempts
- Stops routing to dead node immediately
- Comments already in DB, not lost

### Zookeeper Configuration (Aggressive for Live Systems)
```
sessionTimeout: 6-10 seconds
```
- Fast failure detection critical for live experience
- Tradeoff: too aggressive = false positives from GC pauses
- Layer Dispatcher-level health checks for sub-second detection

### Load Balancer Behavior
- Has its own health checks (5-30 second intervals)
- Not Zookeeper-aware typically
- Gap where clients might hit dead node
- Solution: Client retry handles it (connection fails → retry → different node)

---

## Scaling for Popular Videos

### Multiple RMS Nodes per Video
```
videoId: "viral_video_123" → [RMS-1, RMS-2, RMS-3]
```

- Single node can't handle millions of SSE connections
- Dispatcher fans out to ALL nodes for that video
- Each node holds subset of viewers
- LB round-robins new connections across nodes

### Comment Delivery Fan-out
```
New Comment → CMS → Dispatcher
                      ↓
              ┌───────┼───────┐
              ↓       ↓       ↓
            RMS-1   RMS-2   RMS-3
              ↓       ↓       ↓
           Viewers Viewers Viewers
```

---

## Caching Strategy

### In-Memory Cache on RMS Nodes (Preferred)
- Keep last N comments per video in memory
- Comments already flow through node from Dispatcher
- Cache builds organically
- Fastest catch-up for new viewers

### Request Coalescing
When multiple viewers connect simultaneously:
```
Viewer 1 → cache miss → trigger DB fetch
Viewer 2 → fetch in-flight → wait on same promise
Viewer 3 → fetch in-flight → wait on same promise
DB returns → all 3 served from single fetch
```

Benefits:
- Avoids thundering herd on popular video start
- Only 1-2 nodes per video = limited duplication
- No Redis dependency for hot path

### When to Use Redis Cache
- If RMS nodes are stateless/ephemeral
- Cross-node cache sharing needed
- Simpler operational model at cost of extra hop

---

## Alternative: Redis Pub/Sub Approach

### Sharding Strategy
- Can't have millions of channels
- Hash videoId to fixed number of shards: `channel = hash(videoId) % NUM_SHARDS`
- NUM_SHARDS is static config (e.g., 1000)

### Subscription Patterns
1. **Every node subscribes to all shards** - simple but wasteful
2. **Consistent hashing** - each node owns subset of shards

### How Redis Tracks Subscribers
- No heartbeat - purely TCP connection based
- `SUBSCRIBE` → Redis adds connection to channel map
- TCP drops → OS notifies Redis → cleanup
- Downside: TCP timeout can take minutes for hung connections

---

## Data Model

### Comments Table (DynamoDB)
```
PK: commentId
SK: videoId (for GSI)
Attributes:
  - content
  - author
  - createdAt
  - videoId

GSI: videoId-createdAt-index (for fetching recent comments)
```

### Zookeeper Structure
```
/live-comments
  /rms-nodes
    /rms-1 (ephemeral)
    /rms-2 (ephemeral)
  /video-assignments
    /video_123 → [rms-1, rms-2]
    /video_456 → [rms-3]
```

---

## Summary

| Aspect | Design Choice |
|--------|---------------|
| Delivery Model | Dispatcher (not pub/sub) |
| Client Protocol | SSE (not WebSocket) |
| Failure Detection | Zookeeper + Dispatcher health checks |
| Caching | In-memory on RMS with request coalescing |
| Scaling | Multiple RMS nodes per popular video |
| Reliability | Best-effort real-time + DB catch-up |
