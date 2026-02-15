# Facebook Post Search Design (Without Elasticsearch)

## System Overview

Design a search system for Facebook posts that allows users to search by keywords and get results sorted by various factors (likes, recency, relevance) without using Elasticsearch.

### Architecture Diagram

```
┌──────────────┐
│ Post Service │ ─── create Posts ──→ ┌───────────────┐
└──────────────┘                      │ Load Balancer │
                                      └───────┬───────┘
┌──────────────┐                              │
│ Like Service │ ─── create Likes ───────────┐│
└──────────────┘                             ││
       ↑                                     ││
       │                                     ↓↓
       └─────── Query Likes ─────── ┌──────────────┐      ┌──────────────────┐
                                    │ Event Writer │ ───→ │     Kafka        │
                                    └──────────────┘      └────────┬─────────┘
                                                                   │
                    ┌─────────────────────────────────────────────┘
                    ↓
┌─────────────┐  ┌──────────────────────┐
│Like Batcher │  │ Ingestion Service    │
└─────────────┘  └──────────┬───────────┘
                            │
                            ├────→ Creation: Keyword → [PostIds]
                            ├────→ Likes: Keyword → [PostIds]
                            ↓
        ┌───────────────────────────────────┐
        │   Index Storage (RocksDB)         │
        │                                   │
        │  Inverted Index:                  │
        │  "vacation" → [post1, post5, ...]│
        │  "taylor swift" → [post2, ...]   │
        │                                   │
        │  Like Counts Cache (Redis):      │
        │  Sorted Set by likes             │
        └───────────────┬───────────────────┘
                        │
        ┌───────────────┴──────────────────┐
        │      Search Service              │
        └───────────────┬──────────────────┘
                        │
        ┌───────────────┴──────────────────┐
        │    Search Cache (Redis)          │
        │    Common queries cached         │
        └──────────────────────────────────┘
                        │
                        ↓
        ┌──────────────────────────────────┐
        │   Cold Indexes (Blob Storage)    │
        │   Old/rarely accessed posts      │
        └──────────────────────────────────┘
                        ↑
                        │
        ┌───────────────┴──────────────────┐
        │      API Gateway / CDN           │
        └───────────────┬──────────────────┘
                        │
                        ↓
                   ┌────────┐
                   │ Client │
                   └────────┘
```

---

## Key Design Decisions

### 1. Redis vs RocksDB for Inverted Indexes

#### Original Proposal: Redis Only
```
Author's suggestion:
"Use Redis for inverted indexes - blazing fast queries in memory"
```

**Problems with Redis-only approach:**

| Issue | Impact |
|-------|--------|
| **Durability** | Data loss on crashes; expensive to rebuild billions of posts |
| **Cost** | Storing TBs of data in RAM is extremely expensive |
| **Scale** | Eventually hit physical memory limits per node |
| **Snapshot overhead** | BGSAVE causes copy-on-write memory spikes (discussed below) |

#### Recommended Approach: RocksDB + Redis Hybrid

**RocksDB for Primary Storage:**
```
Advantages:
✓ Durable by default (LSM tree + write-ahead logs)
✓ Cost-effective (disk-based with memory caching)
✓ Battle-tested at Meta (Facebook) scale
✓ Fast reads with proper tuning:
  - Block cache keeps hot data in memory
  - Bloom filters reduce disk reads
  - Microsecond latencies for cached data

Storage format:
Key: "keyword:vacation" → Value: [postId1, postId2, postId3, ...]
Key: "keyword:beach"    → Value: [postId5, postId8, postId10, ...]
```

**Redis for Hot Data Cache:**
```
Cache layer for:
- Recent posts (last 7 days)
- Trending searches
- Popular posts
- Like counts (sorted sets)

Cache miss? → Query RocksDB
```

**Why this works:**
- RocksDB provides durability and cost-effective storage for full dataset
- Redis accelerates hot queries without storing everything in memory
- Best latency for common queries, durability for all data

---

## 2. AWS MemoryDB Alternative

### What is MemoryDB?

**MemoryDB = Redis API + Guaranteed Durability**

```
Write Request
    ↓
Primary Node (in-memory)
    ↓
Multi-AZ Transaction Log (durable storage)
    ↓ (after log persists)
Acknowledgment to client
```

**Key characteristics:**
- Full Redis compatibility (sorted sets, hashes, etc.)
- Durable transaction log distributed across multiple AZs
- Microsecond reads, single-digit millisecond writes
- Automatic failover without data loss
- **NOT tunable** - no quorum vs all acks option
- **More expensive** than Redis OSS

**Comparison:**

| Feature | Redis OSS | MemoryDB | RocksDB |
|---------|-----------|----------|---------|
| Durability | Configurable (AOF/RDB) | Strong (always) | Strong (always) |
| Cost | Cheapest | Most expensive | Medium |
| Latency | Microseconds | 1-digit ms | Sub-ms (cached) |
| Tunability | High | None | Medium |
| Use case | Caching, sessions | Critical data in Redis | Primary storage |

---

## 3. Sorting Challenges

### Problem: Sorting Posts by Like Count

**Redis Sorted Sets (Built-in):**
```redis
ZADD popular_posts 5432 post123  # score=likes, member=postId
ZADD popular_posts 1234 post456

ZRANGE popular_posts 0 9 REV WITHSCORES  # Top 10 by likes
# O(log n) operations, automatically maintained
```

**RocksDB (No Native Sorted Sets):**

RocksDB is just a key-value store with sorted keys. No built-in sorted set structure.

#### Solutions for Sorting in RocksDB

**Option 1: Composite Keys** (Simple but update-heavy)
```
Key format: "likes:{padded_count}:{postId}"
Example:
  "likes:0000005432:post123"
  "likes:0000001234:post456"

Range scan: Iterate by prefix to get top posts

Problem:
- Updating likes requires DELETE old key + INSERT new key
- Expensive for frequently updated posts
```

**Option 2: Hybrid Approach** (Recommended)
```
RocksDB: Inverted indexes only (keyword → postIds)
Redis Sorted Sets: Like counts only (small, easy to rebuild)

Query flow:
1. Search "vacation" in RocksDB → [post1, post2, post3, ...]
2. Fetch like counts from Redis sorted set
3. Sort results by likes in application layer

OR:

1. Get top 1000 posts by likes from Redis
2. Filter by keyword using RocksDB inverted index
```

**Why this works:**
- Inverted indexes are expensive to rebuild → Store in durable RocksDB
- Like counts are easy to rebuild from source of truth → Store in Redis
- Get durability where it matters, performance where needed

**Option 3: Pre-computed Rankings**
```
RocksDB stores:
"keyword:vacation:top_liked" → [post1, post2, post3...] (pre-sorted)

Background job updates rankings every few minutes
Trade freshness for query speed
```

**Reality check:**
- Not all results need real-time like sorting
- Batch update like counts periodically
- Use approximate rankings (good enough for most users)
- Combine with other signals (recency, relevance, personalization)

---

## 4. Bigram Indexing with Count-Min Sketch

### The Problem

```
Query: "Taylor Swift Concert" or "Taylor Swift at the Superbowl"

Challenge: Can't store all possible bigrams/trigrams/n-grams
- Exponential combinations
- Most are rare and not worth indexing
```

### Solution: Selective Bigram Indexing

**Count-Min Sketch** = Probabilistic frequency counter
- Tracks item frequency in constant memory
- Never underestimates (might say 1000 when it's 950, won't say 50)
- Perfect for identifying popular bigrams

#### Implementation

```
Ingestion Pipeline:

1. New post: "I love Taylor Swift concerts"

2. Extract bigrams:
   ["i love", "love taylor", "taylor swift", "swift concerts"]

3. Update Count-Min Sketch for each bigram

4. If bigram frequency > THRESHOLD (e.g., 10,000):
   → Create inverted index for this bigram

5. Otherwise: Skip indexing this rare bigram
```

**Result:**
- Index only popular bigrams: "taylor swift", "new york", "happy birthday"
- Skip rare bigrams: "vacation switzerland", "purple elephant"

#### Query Handling

**Query: "taylor swift concert"**

```
Case 1: Popular bigram indexed
"taylor swift" → [post1, post5, post99, ...]  (direct lookup)
"concert"      → [post1, post2, post5, ...]   (unigram)
Intersect → [post1, post5, ...]
✓ Fast!

Case 2: Rare bigram NOT indexed
"vacation switzerland"
"vacation"    → [post10, post20, post30, ...]
"switzerland" → [post10, post25, post30, ...]
Intersect unigrams → [post10, post30, ...]
✓ Still acceptable
```

**Why this works (Pareto Principle):**
- 90% of queries use common phrases → Pre-indexed, blazing fast
- 10% use rare combinations → Computed on-the-fly, acceptable latency

#### Storage Math

```
Without filtering:
1B unique words × 1B = 10^18 possible bigrams (impossible)

With Count-Min Sketch (top 0.01%):
100M bigrams × 1000 postIds each = manageable
```

#### Alternative Approaches

**Bloom Filter + Exact Counts:**
- "Does bigram exist in frequent set?" (O(1))
- If yes → Look up count and index

**Heavy Hitters (Space-Saving/Misra-Gries):**
- Track top-K most frequent bigrams exactly
- Discard everything else

**Time-Window:**
- Index bigrams popular in last 30 days
- Drop stale indexes
- Keeps index fresh and bounded

---

## 5. How Elasticsearch Handles Sorting

### Two Separate Data Structures

**1. Inverted Index (for matching):**
```
Term "vacation" → [doc1, doc5, doc10, doc50, ...]
```
Finds WHICH documents match.

**2. Doc Values (for sorting/aggregation):**
```
DocID → Field Value (column-oriented)

doc1 → 1000 likes
doc2 → 50 likes
doc5 → 5000 likes
doc10 → 200 likes
```
Column format stored alongside inverted index.

### Query Flow

```
Query: "vacation sorted by likes DESC"

Step 1: Inverted index finds matches
"vacation" → [doc1, doc5, doc10, doc50, doc100, ...]

Step 2: Fetch sort values from Doc Values
doc1 → 1000 likes
doc5 → 5000 likes
doc10 → 200 likes
...

Step 3: Sort in-memory using priority queue
Keep top-K results (e.g., top 100)
No need to sort all millions

Step 4: Return top K
[doc5, doc50, doc1, ...]
```

### Are Doc Values Pre-Sorted? No

Doc values stored in **DocID order** (doc1, doc2, doc3...), not by field value.

Sorting happens **at query time** in memory.

### Multi-Segment Merge

Lucene/ES data split into **immutable segments** (like SSTables):

```
Query across segments:

Segment 1: Query + sort → top 100 [5000, 4500, 4000...]
Segment 2: Query + sort → top 100 [4800, 4200, 3800...]
Segment 3: Query + sort → top 100 [4600, 4100, 3900...]

Final merge-sort: [5000, 4800, 4600, 4500, 4200...]
```

K-way merge sort across segments.

### Optional: Index Sorting

```
Settings:
  index.sort.field = "likes"
  index.sort.order = "desc"

Result: Documents physically sorted by likes within each segment

Benefits:
- Early termination (stop once you have top K)
- Faster sorting queries

Tradeoffs:
- Slower indexing (maintain sort order)
- Limited to ONE sort field
- Updates/deletes more expensive
```

### Comparison Table

| Approach | Storage | Query Time | Updates |
|----------|---------|------------|---------|
| Doc Values (default) | DocID order | Sort in-memory, fast for top-K | Fast |
| Index Sorting | Pre-sorted | Even faster, early termination | Slower |
| RocksDB composite | Pre-sorted | Fast reads | Expensive |
| Redis sorted sets | In-memory sorted | O(log n) | Fast |

**Why ES scales well:**
- Only sorts documents that match, not entire dataset
- Priority queue for top-K (no need to sort millions)
- Segment-level parallelization

---

## 6. Redis Persistence: AOF + Snapshots

### The Combined Strategy

**Problem:**
- AOF only → Slow recovery (replay entire history)
- Snapshot only → Can lose up to 1 hour of data

**Solution: Both**

```
Timeline:
10:00 AM → RDB Snapshot (full dataset to disk)
10:01 AM → Write operations logged to AOF
10:15 AM → More writes to AOF
10:30 AM → More writes to AOF
10:45 AM → CRASH!

Recovery:
1. Load 10:00 AM snapshot (fast, ~10 seconds)
2. Replay AOF from 10:00-10:45 AM (~5 seconds)
3. Total downtime: ~15 seconds
4. Data loss: 0-1 second (with appendfsync everysec)

vs. AOF-only: Could take minutes/hours replaying days of logs
```

### Configuration

```redis
# RDB Snapshots
save 3600 1      # Every 1 hour if ≥1 key changed
save 300 100     # Every 5 min if ≥100 keys changed
save 60 10000    # Every 1 min if ≥10000 keys changed

# AOF
appendonly yes
appendfsync everysec   # Fsync every second (max 1s data loss)

# AOF Rewrite (keep log compact)
auto-aof-rewrite-percentage 100  # Rewrite when 2x last size
auto-aof-rewrite-min-size 64mb   # Minimum trigger size
```

### AOF Rewrite

AOF grows forever, so Redis periodically compacts it:

```
Before rewrite (inefficient):
SET user:1 "Alice"
SET user:1 "Bob"
SET user:1 "Charlie"
INCR counter
INCR counter

After rewrite (compact):
SET user:1 "Charlie"
SET counter 2
```

### Tunability Comparison

**Redis AOF fsync options:**
```redis
appendfsync always   # Every write (slow, most durable)
appendfsync everysec # Every second (balanced) ← Most common
appendfsync no       # Let OS decide (fast, least durable)
```

**MemoryDB:**
- No tunability
- Always synchronously persists to Multi-AZ log
- Cannot trade consistency for latency

**Cassandra:**
```
Write with QUORUM (majority ack)
Write with ALL (all replicas)
Write with ONE (fastest, least durable)
```

### Production Best Practice

```redis
# Snapshot hourly for fast recovery
save 3600 1

# AOF for durability
appendonly yes
appendfsync everysec  # Max 1s data loss

# Keep AOF compact
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

Recovery characteristics:
- Data loss: 0-1 second
- Recovery time: Seconds to minutes
```

---

## 7. Copy-On-Write Memory Spikes

### What is Copy-On-Write?

When Redis creates snapshot (`BGSAVE`), it uses `fork()`:

```
Before BGSAVE:
Redis process: 10 GB memory

fork() called:
Parent process: 10 GB (serves traffic)
Child process:  10 GB (shares same memory pages)
Total memory:   10 GB (not 20 GB - they share!)
```

### The Memory Spike Mechanism

```
Initially: Parent and child SHARE memory pages (read-only)

When parent writes:
OS must COPY that page before modifying it

Timeline during BGSAVE:

T+0s:   Child starts writing snapshot
        Memory: 10 GB (shared)

T+1s:   Parent: SET user:123 "Alice"
        OS copies that page
        Memory: 10 GB + 4 KB

T+10s:  1000 more writes to different keys
        More pages copied
        Memory: 10 GB + 4 MB

T+60s:  Heavy write traffic during snapshot
        Many pages copied
        Memory: Could spike to 15-18 GB!
```

### Worst vs Typical Case

**Worst case:**
```
Every memory page written during snapshot
Original: 10 GB
Peak memory: ~20 GB (nearly double!)
```

**Typical case:**
```
10-30% of pages written during snapshot
Original: 10 GB
Peak memory: ~12 GB
```

### Why Snapshot Frequency Matters

**Hourly snapshots:**
```
10:00 → BGSAVE (1 min spike)
10:01 → Spike ends
10:02-11:00 → Normal memory
11:00 → Next BGSAVE

Memory spike: 1 min per hour = ~1.6% of time
```

**Every 5 minutes:**
```
10:00 → BGSAVE
10:01 → Spike ends
10:05 → BGSAVE again (only 4 min gap)
10:06 → Spike ends
...

Memory spike: 1 min per 5 min = ~20% of time
```

**More frequent snapshots = More time in spike state = Higher OOM risk**

### Real-World Example

```
Redis: 50 GB dataset
Write rate: 10,000 writes/sec
BGSAVE duration: 2 minutes

During 2 minutes: 1.2 million writes
If 20% of data touched: 10 GB extra

Peak memory: 50 GB + 10 GB = 60 GB

Server with 64 GB:
✓ Safe with hourly snapshots
✗ Risk OOM with frequent snapshots
```

### Monitoring

```bash
redis-cli INFO memory

# Normal:
used_memory: 10737418240          # 10 GB
used_memory_rss: 12884901888      # 12 GB

# During BGSAVE:
used_memory_rss: 18884901888      # 18 GB (COW spike!)
mem_fragmentation_ratio: 1.75
```

### Mitigation Strategies

**1. Reduce snapshot frequency:**
```redis
save 3600 1  # Once per hour
```

**2. Use AOF instead:**
```redis
appendonly yes
appendfsync everysec
# AOF doesn't fork as often
```

**3. Disable snapshots:**
```redis
save ""  # Rely on AOF only
```

**4. Provision more memory:**
```
Rule of thumb: 1.5x - 2x dataset size
50 GB dataset → Need 75-100 GB RAM
```

**5. Use Redis Cluster:**
```
Split data across nodes
50 GB → 5 nodes × 10 GB each
COW overhead per node: 10 → 15 GB (safer)
```

---

## Final Architecture Recommendation

### Primary Stack

```
┌─────────────────────────────────────────────────┐
│             Application Layer                   │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│          Search Service (Query Layer)           │
│  - Handles query parsing                        │
│  - Intersects inverted indexes                  │
│  - Ranks and sorts results                      │
└─────────────────────────────────────────────────┘
                      ↓
        ┌─────────────┴──────────────┐
        ↓                            ↓
┌──────────────────┐        ┌─────────────────┐
│ Redis Cache      │        │ RocksDB Primary │
│ (Hot data)       │        │ (Durable store) │
│                  │        │                 │
│ - Recent posts   │        │ - Full inverted │
│ - Trending       │        │   indexes       │
│ - Like counts    │        │ - All keywords  │
│   (sorted sets)  │        │ - All posts     │
└──────────────────┘        └─────────────────┘
        ↓                            ↓
   Cache miss                   Archival
        ↓                            ↓
        └──────────┬─────────────────┘
                   ↓
        ┌─────────────────────┐
        │   Blob Storage      │
        │   (Cold indexes)    │
        │   Posts > 1 year    │
        └─────────────────────┘
```

### Component Choices

| Component | Technology | Reason |
|-----------|-----------|--------|
| **Inverted Index Storage** | RocksDB | Durable, cost-effective, proven at Meta scale |
| **Hot Cache** | Redis | Fast lookups for recent/trending content |
| **Like Counts** | Redis Sorted Sets | Built-in sorting, easy to rebuild |
| **Cold Storage** | S3/Blob Storage | Cost-effective for old posts |
| **Bigram Selection** | Count-Min Sketch | Constant memory, identify popular phrases |
| **Message Queue** | Kafka | Decouple ingestion from indexing |

### Data Flow

**Write Path:**
```
1. User creates post → Post Service
2. Post Service → Kafka
3. Ingestion Service:
   - Extracts keywords/bigrams
   - Updates Count-Min Sketch
   - Writes to RocksDB inverted indexes
   - Updates Redis like counts
```

**Read Path:**
```
1. User searches "vacation beach"
2. Check Redis cache → Hit? Return
3. Cache miss → Query RocksDB:
   - "vacation" → [post1, post5, post10, ...]
   - "beach" → [post5, post10, post20, ...]
   - Intersect → [post5, post10, ...]
4. Fetch like counts from Redis sorted set
5. Sort by likes in application
6. Cache results in Redis
7. Return to user
```

### Scaling Characteristics

**RocksDB cluster:**
- Shard by keyword hash
- Each node handles subset of keywords
- 10 nodes × 10 TB each = 100 TB total

**Redis cluster:**
- Hot cache only (last 7 days)
- 100 nodes × 100 GB each = 10 TB total
- Much cheaper than storing everything

**Cost comparison:**
```
All-Redis: 100 TB × $100/TB/month = $10,000/month
RocksDB + Redis cache:
  - RocksDB: 100 TB × $10/TB/month = $1,000/month
  - Redis: 10 TB × $100/TB/month = $1,000/month
  - Total: $2,000/month (5x cheaper!)
```

---

## Key Takeaways

1. **Storage**: RocksDB for durability, Redis for hot cache
2. **Sorting**: Hybrid approach - RocksDB for indexes, Redis sorted sets for likes
3. **Bigrams**: Use Count-Min Sketch to index only popular phrases
4. **Persistence**: AOF + hourly snapshots for fast recovery with minimal data loss
5. **Memory**: Budget 1.5-2x RAM for COW overhead, reduce snapshot frequency
6. **Scale**: Proven at Meta - RocksDB handles billions of posts efficiently

This design balances:
- ✓ Low latency (Redis cache for hot data)
- ✓ Durability (RocksDB with WAL)
- ✓ Cost efficiency (disk-based storage)
- ✓ Scale (proven at Facebook)
- ✓ Operational simplicity (managed services available)
