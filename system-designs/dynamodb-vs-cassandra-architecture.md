# DynamoDB vs Cassandra: Architecture Deep Dive

## Core Architectural Difference: Storage Engine

### DynamoDB: B+ Tree Based Storage
- **Storage Structure**: B+ tree (NOT LSM tree as commonly assumed)
- **Source**: AWS re:Invent deep dives and insider leaks confirm B+ tree architecture
- **Key Characteristic**: Data stored sorted by sort key within each partition
- **Trade-off**: Optimized for **read performance** with predictable single-digit millisecond latency

### Cassandra: LSM Tree Based Storage
- **Storage Structure**: Log-Structured Merge (LSM) trees with SSTables
- **Key Characteristic**: Append-only writes to commit log + memtable, periodic compaction
- **Trade-off**: Optimized for **write performance** with sequential I/O

## Why This Matters for Performance

### Write Path Comparison

**DynamoDB (B+ Tree)**:
```
Write arrives
  ↓
Leader receives write
  ↓
Read B+ tree to find insertion point (DISK READ - random I/O)
  ↓
Insert/update at correct position
  ↓
Potentially rebalance tree nodes (MORE RANDOM I/O)
  ↓
Write modified pages to disk
  ↓
Replicate via Paxos to 2 other nodes (quorum)
  ↓
ACK to client (~5-10ms)
```

**Cassandra (LSM Tree)**:
```
Write arrives
  ↓
Any node can coordinate (token-aware routing)
  ↓
Append to CommitLog (SEQUENTIAL write)
  ↓
Insert into Memtable (in-memory)
  ↓
Async replication (CL configurable)
  ↓
ACK to client (~1-2ms with CL=ONE)
```

**Impact**:
- B+ tree: Random I/O + reads before writes = **slower writes**
- LSM tree: Sequential append-only = **10-100x faster writes**

### Read Path Comparison

**DynamoDB (B+ Tree)**:
```
Consistent Read
  ↓
Route to Leader node
  ↓
B+ tree lookup (O(log n) with predictable disk seeks)
  ↓
Return data (~1-3ms, very consistent)
```

**Cassandra (LSM Tree)**:
```
Read arrives
  ↓
Check Memtable (memory)
  ↓
Check bloom filters
  ↓
Check multiple SSTables (read amplification)
  ↓
Merge results from multiple sources
  ↓
Return data (~3-10ms, variable based on compaction state)
```

**Impact**:
- B+ tree: Direct lookup, **predictable single-digit millisecond latency**
- LSM tree: Read amplification, **variable latency** (especially during compaction)

## Write Performance Factors (Why Cassandra Wins)

### 1. Storage Engine (Primary Factor) 🔥🔥🔥🔥🔥
```
B+ Tree (DynamoDB):
- Random I/O for every write
- Must read before write
- Tree rebalancing overhead
- Write throughput: ~5,000-10,000 writes/sec per node

LSM Tree (Cassandra):
- Sequential I/O (append-only)
- No reads required
- No rebalancing overhead
- Write throughput: ~50,000-100,000 writes/sec per node
```

**This is 10-100x difference and the dominant factor.**

### 2. Tunable Consistency 🔥🔥
```
DynamoDB:
- Always waits for Leader + 1 replica (quorum)
- No option for faster eventual consistency on writes
- ~5-10ms minimum

Cassandra:
- CL=ONE: Write to 1 node, immediate ACK (~1ms)
- CL=QUORUM: Write to 2/3 nodes (~3ms)
- CL=ALL: Write to all replicas (~5ms)
```

**2-5x latency difference depending on CL choice.**

### 3. Direct Node Access 🔥🔥
```
DynamoDB:
Client → API Gateway → Request Router → Leader Node
(Multiple infrastructure hops)

Cassandra:
Client (token-aware driver) → Coordinator Node directly
(Single hop)
```

**2-3x latency reduction from removing API layer.**

### 4. Cross-AZ Replication 🔥
```
DynamoDB:
- Must replicate across 3 AZs synchronously
- Inter-AZ latency: 1-3ms per hop
- Always waits for cross-AZ quorum

Cassandra:
- Can deploy in single datacenter with local replication
- Sub-millisecond local network
- Or use LOCAL_QUORUM for multi-DC
```

**2x latency reduction for single-DC deployments.**

### 5. API Overhead 🔥
```
DynamoDB (every write):
- HTTPS parsing (JSON over HTTP)
- IAM authentication/authorization (SigV4)
- WCU/RCU throttling checks
- CloudWatch metrics emission
- Request validation

Cassandra:
- Binary CQL protocol
- Persistent connection pool
- Minimal per-request overhead
```

**1.5-2x performance improvement.**

## Read Performance Factors (Why DynamoDB Wins)

### 1. B+ Tree Direct Lookup
- O(log n) with predictable disk seeks
- No read amplification
- Consistent 1-3ms latency

### 2. LSM Read Amplification
- Must check memtable + multiple SSTables
- Compaction can cause latency spikes
- Variable 3-10ms latency

### 3. Leader-Based Consistent Reads
DynamoDB consistent reads go to **Leader node directly**:
```
Leader always has latest data
  ↓
Single node lookup
  ↓
No quorum coordination needed
  ↓
Predictable latency
```

Cassandra quorum reads require coordination:
```
Coordinator sends read to multiple nodes
  ↓
Wait for R nodes to respond
  ↓
Resolve conflicts if any
  ↓
Additional network hops + coordination overhead
```

## Replication Models

### DynamoDB: Leader-Based Replication
```
Write Flow:
1. Client → Leader node
2. Leader appends to Write-Ahead Log (WAL)
3. Leader replicates log to 2 followers via Paxos
4. Wait for quorum (Leader + 1 follower)
5. Leader applies to B+ tree
6. ACK to client

Read Flow (Consistent):
- Always reads from Leader
- Leader has most recent data
- No quorum needed

Read Flow (Eventually Consistent):
- Can read from any replica
- May see stale data
- Lower latency
```

**Key Point**: Leader serializes all writes, ensuring strong consistency for consistent reads.

### Cassandra: Quorum-Based Replication
```
Write Flow:
1. Client → Coordinator node (any node)
2. Coordinator determines replicas via token hash
3. Coordinator sends write to N replicas
4. Wait for W nodes to acknowledge (configurable)
5. ACK to client

Read Flow:
1. Client → Coordinator node
2. Coordinator sends read to R nodes (configurable)
3. Wait for R responses
4. Resolve conflicts using timestamps
5. Return data to client
```

**Key Point**: No leader, any node can coordinate. Consistency achieved via quorum (R + W > N).

## Hot Partition Problem

### DynamoDB: 1000 WCU Per Partition Limit
```
Hard Limit: 1000 writes/sec per partition

Problem:
- Even with 100,000 WCU provisioned for table
- Single hot partition throttled at 1000 WCU
- Must engineer artificial sharding

Example:
partition_key = user_id + "#" + random(0, 999)

Downside:
- Query complexity increases
- Must query 1000 partitions to read data
```

### Cassandra: No Hard Per-Partition Limit
```
Typical Limit: 10,000+ writes/sec per partition (hardware dependent)

Flexibility:
- Compound partition keys naturally distribute data
- No artificial sharding required

Example:
PRIMARY KEY ((user_id, date), timestamp)

Benefit:
- Natural data distribution
- Simple queries still work
```

## Cost Implications at High Scale

### Scenario: 1 Million Writes/Second

**DynamoDB**:
```
1M writes/sec = 1,000,000 WCU
Cost: $0.00065 per WCU-hour
= $0.00065 × 1,000,000 × 24 × 30
= $468,000/month (writes only)

Plus:
+ Storage costs
+ GSI costs (if any)
+ Cross-region replication (if using Global Tables)
+ Data transfer costs

Total: ~$500,000 - $700,000/month
```

**Cassandra (Self-Managed)**:
```
1M writes/sec on Cassandra
= ~10-20 nodes (i3.4xlarge @ $2/hour each)
= 20 nodes × $2/hour × 24 × 30
= $28,800/month (compute)

Plus:
+ Operational overhead (DevOps, monitoring, maintenance)
+ Storage costs (EBS if not using instance storage)

Total: ~$50,000 - $100,000/month including ops
```

**Savings: 5-10x cost reduction at high write scale**

### When Cost Favors Cassandra

1. **Write-heavy workloads** (>100K writes/sec sustained)
2. **Large dataset** with high throughput (>10TB with >50K ops/sec)
3. **Predictable traffic patterns** (can optimize cluster sizing)
4. **Engineering team with distributed systems expertise**

### When Cost Favors DynamoDB

1. **Variable/unpredictable traffic** (auto-scaling value)
2. **Small to medium scale** (<50K writes/sec)
3. **Limited operational resources** (fully managed)
4. **Multi-region requirements** (Global Tables simplicity)

## Interview Key Points

### Opening: The Fundamental Difference

"The key difference between DynamoDB and Cassandra comes down to their storage engines. Based on AWS re:Invent deep dives and architecture leaks, **DynamoDB uses B+ trees** while **Cassandra uses LSM trees**. This single architectural choice drives most of the performance characteristics."

### Why Cassandra is More Write-Performant

"Cassandra achieves 10-100x better write performance primarily because:

1. **LSM sequential writes vs B+ tree random I/O** (the dominant factor)
   - B+ trees require reading the tree, finding the insertion point, and potentially rebalancing - all random I/O
   - LSM just appends to a commit log sequentially - no reads required

2. **Tunable consistency** - CL=ONE allows 1ms ACKs vs DynamoDB's forced 5-10ms quorum
3. **Direct node access** - token-aware clients skip the API layer
4. **No cross-AZ mandate** - can use single-DC with sub-millisecond replication
5. **Binary protocol** - CQL over native protocol vs JSON over HTTPS

At very high write scales (1M+ writes/sec), the B+ tree architecture becomes the bottleneck, and **cost savings of 5-10x make Cassandra the only viable option**."

### Why DynamoDB Excels at Reads

"DynamoDB's B+ tree architecture provides:

1. **Predictable single-digit millisecond latency** - direct tree lookup, no read amplification
2. **Leader-based consistent reads** - no quorum coordination overhead
3. **Stable performance** - no compaction-related latency spikes

This is why DynamoDB is preferred for read-heavy, latency-sensitive applications where predictable performance matters more than raw throughput."

### The Cost Argument (Closing Note)

"At extreme write scales (hundreds of thousands to millions of writes per second), **DynamoDB's cost becomes prohibitive**. The B+ tree architecture, while providing excellent read latency, simply cannot match LSM's write throughput per dollar.

For example, at 1M writes/sec:
- DynamoDB: ~$500K-700K/month
- Cassandra: ~$50K-100K/month (self-managed)

This is a **5-10x cost difference**, and at this scale, the operational complexity of managing Cassandra is justified by the massive cost savings. The leaked information about DynamoDB's B+ tree architecture explains why it cannot compete on write performance at high scale - it's a fundamental storage engine limitation, not just an API or protocol overhead issue."

### The Closing Statement

"So in interviews, when discussing why Cassandra over DynamoDB at scale, I can confidently state:

1. **Architecture leaks confirm B+ tree vs LSM** - this is the root cause
2. **Write performance is 10-100x better** on Cassandra due to sequential vs random I/O
3. **At 1M+ writes/sec, cost savings are 5-10x** - making Cassandra the only economically viable option
4. **The trade-off is operational complexity** vs DynamoDB's managed simplicity

For write-heavy, high-scale systems, Cassandra wins on both performance and cost. For read-heavy, variable traffic with limited ops resources, DynamoDB wins on simplicity and predictability."

## Cassandra Data Modeling: Range Queries and Clustering

### Composite Partition Keys

Cassandra allows **composite partition keys** where multiple columns together determine the partition:

```cql
CREATE TABLE user_events (
    user_id UUID,
    date TEXT,  -- YYYY-MM-DD format
    timestamp TIMESTAMP,
    event_type TEXT,
    payload TEXT,
    PRIMARY KEY ((user_id, date), timestamp, event_type)
);
```

**Key Breakdown**:
- `(user_id, date)` - **Composite Partition Key** (both fields together decide which partition)
- `timestamp, event_type` - **Clustering Columns** (define sort order within partition)

**Partition Distribution**:
```
Token Hash = hash(user_id + date)

Example:
- (user_123, "2024-01-15") → Node A
- (user_123, "2024-01-16") → Node B
- (user_456, "2024-01-15") → Node C

Same user_id but different dates = different partitions
```

**Benefits**:
1. **Natural time-based sharding** - distributes load across dates
2. **Prevents partition bloat** - limits partition size to single day
3. **Enables efficient date-range queries** (with multiple queries per date)
4. **Better write distribution** - no single hot partition for active users

### Clustering Columns: Range Queries and Sorting

Clustering columns provide **ordered storage within a partition** enabling efficient range queries:

```cql
-- Query examples
SELECT * FROM user_events
WHERE user_id = ? AND date = ?
AND timestamp > '2024-01-15 10:00:00'
AND timestamp < '2024-01-15 12:00:00';

SELECT * FROM user_events
WHERE user_id = ? AND date = ?
AND timestamp = '2024-01-15 10:30:00'
AND event_type IN ('click', 'view');
```

**How Clustering Works Internally**:

```
Partition: (user_123, "2024-01-15")
Stored on disk as sorted rows:

timestamp: 08:00:00, event_type: "login"    → Row 1
timestamp: 08:05:30, event_type: "click"    → Row 2
timestamp: 08:05:30, event_type: "view"     → Row 3  ← Same timestamp, sorted by event_type
timestamp: 10:30:00, event_type: "purchase" → Row 4
timestamp: 15:45:00, event_type: "logout"   → Row 5
```

**Physical Storage**: Rows are **physically sorted on disk** by clustering columns, making range scans O(1) to start + O(k) to scan k matching rows.

### Multi-Level Clustering: Hierarchical Sorting

With multiple clustering columns, sorting is **hierarchical** (like SQL `ORDER BY`):

```cql
CREATE TABLE analytics (
    org_id UUID,
    year INT,
    month INT,
    day INT,
    hour INT,
    metric_name TEXT,
    value DOUBLE,
    PRIMARY KEY (org_id, year, month, day, hour, metric_name)
);
```

**Clustering Columns**: `year, month, day, hour, metric_name`

**Sort Order (within partition)**:
```
1. First by year (ascending)
   2. Then by month (ascending) within same year
      3. Then by day (ascending) within same year+month
         4. Then by hour (ascending) within same year+month+day
            5. Then by metric_name (ascending) within same year+month+day+hour

Example stored order:
(2024, 1, 15, 8, "cpu_usage")
(2024, 1, 15, 8, "memory_usage")     ← Same prefix, sorted by metric_name
(2024, 1, 15, 9, "cpu_usage")        ← Next hour
(2024, 1, 16, 8, "cpu_usage")        ← Next day
(2024, 2, 1, 8, "cpu_usage")         ← Next month
```

### Range Query Rules for Clustering Columns

**Golden Rule**: Must specify clustering columns **in order** (left to right) for range queries:

```cql
-- ✅ VALID: Prefix of clustering columns specified
WHERE org_id = ? AND year = 2024 AND month = 1 AND day >= 15

-- ✅ VALID: Can use range on last specified column
WHERE org_id = ? AND year = 2024 AND month >= 1 AND month <= 3

-- ❌ INVALID: Skipping 'month' column
WHERE org_id = ? AND year = 2024 AND day = 15  -- ERROR

-- ✅ VALID: Equality on prefix, range on next
WHERE org_id = ? AND year = 2024 AND month = 1 AND day >= 10 AND day <= 20

-- ❌ INVALID: Range on non-consecutive column
WHERE org_id = ? AND year >= 2024 AND day = 15  -- ERROR
```

**Why This Restriction?**

Cassandra stores rows sorted by clustering columns in a **composite key format**:

```
Composite Key = year:month:day:hour:metric_name

On disk:
2024:01:15:08:cpu_usage
2024:01:15:08:memory_usage
2024:01:15:09:cpu_usage
...
```

To find `day >= 15`, Cassandra needs to know the prefix (`year`, `month`) to determine where to start scanning. Without the prefix, it cannot perform a binary search.

### DynamoDB vs Cassandra: Range Query Comparison

**DynamoDB**:
```
Table Schema:
- Partition Key: user_id
- Sort Key: timestamp

Query:
- ✅ Range on sort key only
- ❌ No composite partition keys
- ❌ Single sort key only (no multi-level sorting)

SELECT * FROM events
WHERE user_id = ?
AND timestamp BETWEEN ? AND ?
```

**Cassandra**:
```
Table Schema:
- Composite Partition Key: (user_id, date)
- Multiple Clustering Columns: (timestamp, event_type)

Query:
- ✅ Range on any clustering column (with prefix)
- ✅ Composite partition keys for better distribution
- ✅ Multi-level sorting within partition

SELECT * FROM events
WHERE user_id = ? AND date = ?
AND timestamp >= ? AND timestamp <= ?
AND event_type IN (?, ?)
```

**Cassandra Advantages**:
1. **Composite partition keys** prevent hot partitions
2. **Multiple clustering columns** enable complex range queries
3. **Hierarchical sorting** supports nested time-series queries
4. **Better partition distribution** via multi-column partition keys

**DynamoDB Advantages**:
1. **Simpler mental model** - one partition key, one sort key
2. **No query ordering restrictions** - just partition + optional sort key range
3. **Global Secondary Indexes** - flexible alternative access patterns

### Real-World Example: Time-Series Data

**Problem**: Store metrics for millions of devices, query recent data efficiently

**Cassandra Solution**:
```cql
CREATE TABLE device_metrics (
    device_id UUID,
    date TEXT,           -- Partition per day
    hour INT,
    minute INT,
    sensor_id TEXT,
    value DOUBLE,
    PRIMARY KEY ((device_id, date), hour, minute, sensor_id)
) WITH CLUSTERING ORDER BY (hour ASC, minute ASC, sensor_id ASC);
```

**Query Patterns**:
```cql
-- Get last hour of data for device
SELECT * FROM device_metrics
WHERE device_id = ? AND date = '2024-01-15'
AND hour = 14 AND minute >= 0;

-- Get specific sensor data for time range
SELECT * FROM device_metrics
WHERE device_id = ? AND date = '2024-01-15'
AND hour >= 10 AND hour <= 12
AND sensor_id = 'temp_sensor_1';

-- Get all sensors at specific time
SELECT * FROM device_metrics
WHERE device_id = ? AND date = '2024-01-15'
AND hour = 14 AND minute = 30;
```

**Benefits**:
- Partitions naturally limited to 1 day (prevents unbounded growth)
- Efficient range scans on hour/minute (sorted storage)
- Can filter by sensor_id after time range (also sorted)
- Write distribution across dates prevents hot spots

### Interview Talking Points

"Cassandra's clustering columns provide a powerful advantage over DynamoDB's single sort key:

1. **Composite partition keys** like `(user_id, date)` distribute data naturally, preventing hot partitions that DynamoDB requires artificial sharding to avoid.

2. **Multi-level clustering** enables hierarchical sorting - I can have `year, month, day, hour` as clustering columns and efficiently query ranges at any level, as long as I specify the prefix in order.

3. **Physical sorting on disk** means range queries on clustering columns are O(1) to start plus O(k) for k results - Cassandra just seeks to the start of the range and scans sequentially.

4. The **clustering column ordering rule** (must specify in sequence) is a trade-off - it requires careful schema design, but in return you get predictable, efficient queries with no query planning overhead.

For time-series or hierarchical data, Cassandra's clustering model is significantly more powerful than DynamoDB's single sort key limitation."

## Storage Capacity Per Node

### Physical Capacity
```
Hardware Limits: 2-8TB+ per node

Example instances:
- i3.2xlarge:  1.9TB NVMe SSD
- i3.4xlarge:  3.8TB NVMe SSD
- i3.8xlarge:  7.6TB NVMe SSD
- i3.16xlarge: 15.2TB NVMe SSD
```

### Recommended Operational Limits

**Best Practice: 500GB-2TB per node**

```
✅ Sweet Spot: 500GB-1TB per node
⚠️ Acceptable: 1TB-2TB per node
❌ Avoid: >3TB per node
```

**Why these limits?**

#### 1. Repair Time Constraints
```
Repair = Anti-entropy process to sync replicas across nodes

500GB node: 2-4 hours repair time ✅
1TB node:   4-8 hours repair time ✅
2TB node:   8-16 hours repair time ⚠️
4TB node:   16-32 hours repair time ❌

Critical Rule:
Must complete repair within gc_grace_seconds (default 10 days)
Otherwise: Deleted data can resurrect
```

**Repair is needed even with CL=QUORUM writes:**
```
Scenario: RF=3, Write with CL=QUORUM (W=2)

Write succeeds on Node A and Node B
Node C misses the write (temporarily down)
✅ Client gets ACK (2/3 quorum met)

Problem: Node C still has stale/missing data
Solution: Repair syncs Node C within 10 days

Even CL=ALL needs repair:
- Node crashes after ACK
- Disk corruption
- Network partitions
```

#### 2. Bootstrap/Decommission Time
```
Adding new node (streaming data):

500GB node at 50MB/sec: ~3 hours
2TB node at 50MB/sec:   ~11 hours
4TB node at 50MB/sec:   ~22 hours

Longer recovery = higher risk during node replacement
```

#### 3. Compaction Overhead
```
Compaction = Merging SSTables on single node (NOT repair)

Size-Tiered Compaction (default):
1TB data → needs 500GB-1TB temp space
2TB data → needs 1-2TB temp space

Compaction time:
1TB node: 4-8 hours
2TB node: 8-16 hours
```

### Repair vs Compaction

| Aspect | Repair | Compaction |
|--------|--------|------------|
| **Purpose** | Sync replicas across nodes | Merge SSTables on single node |
| **Scope** | Cross-node operation | Single-node operation |
| **Network** | Heavy network I/O (streaming) | No network (local disk) |
| **What it fixes** | Replica inconsistency | Storage fragmentation, read perf |
| **When to run** | Periodically (within gc_grace) | Automatic |
| **Time constraint** | Must complete within 10 days | No constraint |
| **Disk space** | No extra space needed | Needs 50-100% temp space (STCS) |

### Storage Calculation with Replication

```
Example: 50TB logical data, RF=3

Physical storage needed:
= 50TB × 3 (replication)
= 150TB total across cluster

With 1TB nodes:
= 150TB / 1TB per node
= 150 nodes

With 2TB nodes:
= 150TB / 2TB per node
= 75 nodes
```

## Change Data Capture (CDC)

### DynamoDB Streams
```
✅ Auto-managed stream of changes
✅ 24-hour retention (auto-deleted)
✅ Built-in checkpointing (Kinesis Client Library)
✅ Exactly-once semantics via sequence numbers
✅ Sharded by partition key
✅ No disk space management

Consumer reads from stream API directly
```

### Cassandra CDC
```
CDC enabled per table:
CREATE TABLE users (...) WITH cdc = true;

Write Path:
1. Write arrives → CommitLog
2. If CDC enabled → Mark CommitLog segment as CDC
3. When segment full → Moved to cdc_raw directory
4. Consumer reads from cdc_raw directory
5. ⚠️ Consumer must DELETE files after processing

Critical Configuration:
cdc_total_space_in_mb: 4096  # Default 4GB

If CDC logs exceed limit:
→ Cassandra STOPS accepting writes for CDC-enabled tables
→ Must manually delete old CDC logs to resume
```

**Cassandra CDC Consumer Pattern**:
```java
public class CDCConsumer {
    private final Path cdcRawDir = Paths.get("/var/lib/cassandra/cdc_raw");

    public void consumeChanges() {
        List<Path> cdcLogs = Files.list(cdcRawDir)
            .filter(p -> p.toString().endsWith(".log"))
            .sorted()  // Process in order
            .collect(Collectors.toList());

        for (Path log : cdcLogs) {
            try {
                List<Mutation> changes = parseCommitLog(log);
                for (Mutation m : changes) {
                    processChange(m);
                }

                // ⚠️ CRITICAL: Delete after successful processing
                Files.delete(log);

            } catch (Exception e) {
                // Don't delete if processing failed
                log.error("Failed to process: " + log, e);
            }
        }
    }
}
```

**Key Differences**:

| Feature | DynamoDB Streams | Cassandra CDC |
|---------|------------------|---------------|
| **Auto-deletion** | ✅ After 24 hours | ❌ Manual deletion required |
| **Consumption** | Stream API | File-based (CommitLog parsing) |
| **Disk management** | Not needed | Critical (stops writes if full) |
| **Checkpointing** | Built-in (KCL) | Manual (application) |
| **Exactly-once** | Via sequence numbers | Manual deduplication |
| **Operational overhead** | None | High (monitor disk, delete files) |

## Secondary Indexes / GSI Equivalent

### DynamoDB: Global Secondary Indexes (GSI)
```
Main Table:
PK: user_id, SK: order_id

GSI (auto-synced):
PK: email, SK: order_date

✅ Automatic synchronization
✅ Eventually consistent
✅ Separate provisioned throughput
✅ Separate storage billing
❌ Additional cost per GSI
```

### Cassandra: Manual Secondary Tables
```cql
-- Main table
CREATE TABLE orders_by_user (
    user_id UUID,
    order_id UUID,
    email TEXT,
    order_date TIMESTAMP,
    PRIMARY KEY (user_id, order_id)
);

-- "GSI" equivalent - separate table
CREATE TABLE orders_by_email (
    email TEXT,
    order_date TIMESTAMP,
    order_id UUID,
    user_id UUID,
    PRIMARY KEY (email, order_date, order_id)
);

-- Application writes to BOTH tables
BEGIN BATCH
    INSERT INTO orders_by_user (...);
    INSERT INTO orders_by_email (...);
APPLY BATCH;
```

**Cassandra Secondary Index Options**:

#### 1. Manual Secondary Tables (Recommended)
```
✅ Full control over schema
✅ Optimized for query pattern
✅ Storage included in cluster cost
❌ Manual write to both tables (application logic)
❌ No automatic consistency guarantees
❌ Must handle write failures to both tables
```

#### 2. Built-in Secondary Indexes (Avoid)
```cql
CREATE INDEX ON orders_by_user (email);

❌ Scatter-gather query across ALL nodes
❌ Slow performance (no partition pruning)
❌ Not recommended for production
```

#### 3. Materialized Views (Deprecated)
```cql
CREATE MATERIALIZED VIEW orders_by_email AS
    SELECT * FROM orders_by_user
    WHERE email IS NOT NULL
    PRIMARY KEY (email, order_date, order_id);

⚠️ Auto-synced but deprecated due to bugs
❌ Consistency issues in production
❌ Not recommended (removed in Cassandra 5.0)
```

### GSI Comparison

| Feature | DynamoDB GSI | Cassandra Secondary Table |
|---------|--------------|---------------------------|
| **Auto-sync** | ✅ Automatic | ❌ Manual (app writes both) |
| **Write overhead** | Transparent | Explicit dual writes |
| **Consistency** | Eventually consistent | Application controls |
| **Write atomicity** | None (async) | BATCH provides atomicity |
| **Storage cost** | Separate billing | Included in cluster |
| **Query flexibility** | Limited to GSI schema | Full schema control |
| **Operational complexity** | None | Manage write logic |
| **Schema evolution** | Add GSI anytime | Create new table anytime |

**Best Practice in Cassandra**:
```
Denormalize with multiple tables
↓
Use BATCH for atomic writes
↓
Handle failures gracefully
↓
Trade storage for query performance
```

## Summary Table

| Factor | DynamoDB | Cassandra | Winner |
|--------|----------|-----------|--------|
| **Storage Engine** | B+ Tree | LSM Tree | - |
| **Write Performance** | 5K-10K writes/sec/node | 50K-100K writes/sec/node | Cassandra (10x) |
| **Write Latency** | 5-10ms (forced quorum) | 1-2ms (CL=ONE) | Cassandra (5x) |
| **Read Performance** | 1-3ms (predictable) | 3-10ms (variable) | DynamoDB |
| **Consistent Reads** | Leader-based (fast) | Quorum-based (slower) | DynamoDB |
| **Cost at 1M writes/sec** | $500K-700K/month | $50K-100K/month | Cassandra (5-10x) |
| **Storage per Node** | N/A (managed) | 500GB-2TB (operational limit) | DynamoDB |
| **Operational Complexity** | Fully managed | Self-managed | DynamoDB |
| **Scaling** | Auto-scaling | Manual tuning | DynamoDB |
| **Hot Partition Limit** | 1000 WCU hard limit | 10K+ writes (hardware) | Cassandra (10x) |
| **Partition Key** | Single column | Composite (multi-column) | Cassandra |
| **Sort Keys** | Single sort key | Multiple clustering columns | Cassandra |
| **Range Queries** | Simple (partition + sort) | Complex (hierarchical) | Cassandra |
| **CDC** | Streams (auto-managed, 24hr) | Manual file deletion required | DynamoDB |
| **Secondary Indexes** | GSI (auto-synced) | Manual secondary tables | DynamoDB |
| **Repair Needed** | No (managed) | Yes (within 10 days) | DynamoDB |

## References

- AWS re:Invent 2024 - Dive deep into Amazon DynamoDB (DAT406)
- AWS re:Invent 2024 - Architecture choices for Amazon DynamoDB (DAT419)
- DynamoDB architecture leaks confirming B+ tree storage engine
- Cassandra documentation on LSM tree architecture
