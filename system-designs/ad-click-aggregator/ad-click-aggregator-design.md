# Ad Click Aggregator System Design

## Overview
Design a system to aggregate ad clicks/impressions in real-time, handling high throughput (100k-200k writes/second) with low latency for analytics and billing purposes.

## Requirements

### Functional Requirements
- Track clicks and impressions for advertisements
- Aggregate metrics at multiple time granularities (1-min, 5-min, hourly, daily)
- Support real-time queries for recent metrics
- Support historical queries for long-term analytics

### Non-Functional Requirements
- **Scale**: 100k-200k writes per second (impression counting scenario)
- **Latency**: Near real-time aggregation (seconds)
- **Availability**: High availability for ingestion
- **Durability**: No data loss for billing purposes

## High-Level Architecture

```
Browser → API Gateway → Kafka → Flink → Multiple Sinks
                                           ├─ Cassandra (long-term aggregates)
                                           ├─ Redis (real-time counters)
                                           ├─ S3 (raw event backup)
                                           └─ Kafka (downstream events)
```

## Key Components

### 1. Ingestion Pipeline
- **API Gateway**: Receives click events from browsers/apps
- **Kafka**: Durable message queue partitioned by ad_id
- **Throughput**: 10k-200k events/second

### 2. Stream Processing (Flink)
- Real-time aggregation engine
- Multiple windowing strategies:
  - Tumbling windows: 1-min for real-time
  - **Flush Interval**: 5-10 seconds for progressive updates
- **Multiple Sinks Support**: Single Flink job can write to multiple destinations simultaneously

#### Flink Multi-Sink Pattern with Flush Intervals
```java
DataStream<Click> clicks = ...

// Sink 1: Raw clicks to S3 for backup
clicks.sinkTo(s3Sink);

// Sink 2: Minute-level aggregates to Cassandra
// Window: 1 minute, Flush: 10 seconds
// Same (ad_id, minute_bucket) gets UPSERTed 6 times per minute
clicks
  .keyBy(click -> click.adId)
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .aggregate(new ClickAggregator())
  .sinkTo(cassandraSink)
  .setBufferTimeout(10000);  // Flush every 10 seconds

// Sink 3: Real-time 1-minute counters to Redis (same flush config)
clicks
  .keyBy(click -> click.adId)
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .sum("count")
  .sinkTo(redisSink)
  .setBufferTimeout(10000);

// Sink 4: Republish aggregated events to Kafka
clicks
  .keyBy(click -> click.adId)
  .window(TumblingEventTimeWindows.of(Time.minutes(5)))
  .aggregate(new ClickAggregator())
  .sinkTo(kafkaSink);
```

**Flush Interval Behavior:**
```
Window: 00:00:00 - 00:01:00, Flush: 10s

00:00-00:10 → 500 clicks → UPSERT → "500 clicks at 00:00"
00:10-00:20 → 600 clicks → UPSERT → "1,100 clicks at 00:00" (progressive)
00:20-00:30 → 450 clicks → UPSERT → "1,550 clicks at 00:00" (progressive)
...
00:50-01:00 → 400 clicks → UPSERT → "3,000 clicks at 00:00" (FINAL)
```

**Benefits:**
- **Latency**: Max 10s instead of 60s for advertisers
- **Progressive updates**: Counts increment live in real-time dashboards
- **Final accuracy**: Still correct at window boundary
- **Cost**: 6x write volume (acceptable tradeoff for responsiveness)

**Requirements:**
- Sink must support UPSERT operations (Cassandra counters, Redis SET, Postgres ON CONFLICT)
- UI should indicate "partial" for incomplete current-minute windows

### 3. Storage Layer

#### Cassandra (Long-term Storage)
- Stores aggregated metrics at multiple granularities (minute, hourly, daily)
- Schema:
```sql
CREATE TABLE ad_metrics_minute (
    ad_id text,
    time_bucket timestamp,
    click_count counter,
    impression_count counter,
    PRIMARY KEY (ad_id, time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC)
AND default_time_to_live = 172800;  -- 48h TTL

CREATE TABLE ad_metrics_hourly (
    ad_id text,
    time_bucket timestamp,
    click_count counter,
    impression_count counter,
    PRIMARY KEY (ad_id, time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC)
AND default_time_to_live = 864000;  -- 10 days TTL

CREATE TABLE ad_metrics_daily (
    ad_id text,
    time_bucket timestamp,
    click_count counter,
    impression_count counter,
    PRIMARY KEY (ad_id, time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC);
-- No TTL, long-term retention
```

#### Redis (Real-time Cache)
- Recent metrics (last 1-5 minutes) for sub-second queries
- Key pattern: `ad:{ad_id}:minute:{timestamp_minute}`
- TTL: 10 minutes
- Receives progressive updates from Flink every 10 seconds

### 4. Batch Processing (Handling Late Arrivals)

**Problem**: Events can arrive late (mobile offline, retries, batch uploads)

**Solution**: Run batch jobs on raw S3 data with lookback windows

#### Batch Job Schedule
```
Hourly Rollups:
- Runs at: T+6h
- Processes: [T-6h, T] window
- Purpose: Captures 99.9% of late arrivals before compacting to hourly

Daily Rollups:
- Runs at: T+3d
- Processes: [T-3d, T] window
- Purpose: Handles extreme late arrivals (batch uploads, corrections)
```

**Example:**
```
Current time: 14:00

Hourly job processes: [08:00, 14:00]
- Reads raw events from S3 for this 6h window
- Aggregates into hourly buckets
- UPSERTs to ad_metrics_hourly table

Daily job processes: [3 days ago, now]
- Reads raw events from S3 for last 3 days
- Aggregates into daily buckets
- UPSERTs to ad_metrics_daily table
```

**Why Lookback > Processing Granularity:**
- 6h lookback ensures hourly data is complete before we rely on it
- 3d lookback ensures daily data captures all corrections/reprocessing
- Trades compute cost for correctness

### 5. Query Service

**Query Routing Logic (Based on Scan Efficiency):**

**Decision Rule:** If scan would exceed ~1,500 records, use coarser granularity

```python
def get_ad_metrics(ad_id, start_time, end_time):
    duration_hours = (end_time - start_time).total_hours()

    if duration_hours <= 24:
        # Max 1,440 minute records - acceptable to scan
        return query_cassandra("ad_metrics_minute", ad_id, start_time, end_time)

    elif duration_hours <= 168:  # 7 days
        # Max 168 hourly records - efficient
        return query_cassandra("ad_metrics_hourly", ad_id, start_time, end_time)

    else:
        # Use daily aggregates
        return query_cassandra("ad_metrics_daily", ad_id, start_time, end_time)
```

**Query Thresholds:**
- **< 24h**: Query minute-level (1,440 records max)
- **1-7 days**: Query hourly-level (168 records max)
- **7+ days**: Query daily-level (30-365 records)

**Data Lifecycle & TTLs:**
```
Minute-level:
- Written by: Flink (real-time)
- TTL: 48h (deleted after hourly batch complete)
- Storage: ~72GB for 1M ads

Hourly-level:
- Written by: Batch job at T+6h
- TTL: 10 days (deleted after daily batch complete)
- Storage: ~5GB for 1M ads

Daily-level:
- Written by: Batch job at T+3d
- No TTL: Long-term retention
- Storage: ~500MB/year for 1M ads
```

**Why This Works:**
- Write aggregation lookback (6h, 3d) > read threshold (24h, 7d)
- Ensures coarser data is complete when we query at boundaries
- Balances storage cost vs. correctness

**Real-time Path (Last Few Minutes):**
- Check Redis first for current incomplete minute
- Redis gets progressive updates every 10s from Flink
- Fallback to Cassandra minute-level for complete minutes

**OLAP Path:**
- ClickHouse/Druid for ad-hoc analytics queries
- Periodically sync from Cassandra aggregates

## Scaling Challenges & Solutions

### Challenge 1: Hot Shards (Hot Ads)

**Problem**: Popular ad (e.g., Nike + LeBron) gets all clicks → single Kafka partition overwhelmed → lag/backlog

#### Solution: Partition Key Salting

**For Kafka Ingestion:**
```python
# API Gateway: When writing to Kafka
partition_key = f"{ad_id}_{hash(click_id) % N}"

# Examples:
# "nike_ad_0"
# "nike_ad_1"
# "nike_ad_2"
```

**In Flink Processing:**
```java
clicks
  .map(click -> {
    // Strip suffix to get original ad_id
    String originalAdId = click.adId.split("_")[0];
    click.adId = originalAdId;
    return click;
  })
  .keyBy(click -> click.adId)
  .window(...)
  .aggregate(...)
```

**Benefits:**
- Spreads hot ad events across multiple Kafka partitions
- No single partition bottleneck
- Flink still aggregates correctly by original ad_id
- No read penalty (unlike Cassandra salting)

**Recommended Approach:**
- **Always salt** with N=8 or N=16 for all ads
- Simple, works for all ads (hot or not)
- No need for dynamic detection

#### Alternative: Dynamic Hot Ad Detection (Not Recommended)

**Approach:**
1. Keep in-memory counter at API Gateway
2. Flush to Redis every 10ms (atomic increment)
3. If rate > 5k/sec for 10 seconds, mark as "hot ad"
4. Propagate flag to all API servers
5. Dynamically create partitions for hot ads

**Why Not Recommended:**
- **Latency**: Detection takes 10+ seconds, surge might be over
- **State sync**: All API servers need to know hot ads
- **Complexity**: Multiple moving parts for marginal benefit
- **Simpler**: Just salt all partition keys upfront

### Challenge 2: Cassandra Write Scaling

**Problem**: Single hot ad → all writes to same Cassandra partition → node overwhelmed (50k writes/node limit)

#### Solution: Cassandra Partition Key Salting

```sql
-- Instead of:
PRIMARY KEY (ad_id, time_bucket)

-- Use:
PRIMARY KEY ((ad_id, shard_id), time_bucket)
```

**Write Path:**
```python
# For hot ads only
shard_id = random.randint(0, N-1)
partition_key = f"{ad_id}_{shard_id}"

# Spreads writes across N Cassandra nodes
```

**Read Path (Critical Tradeoff):**
```python
# Must query ALL N partitions
results = []
for shard_id in range(N):
    results.append(
        cassandra.query(f"ad_id = nike_ad_{shard_id} AND time_bucket = ...")
    )
aggregated = sum(results)  # Application-side aggregation
```

**Tradeoffs:**
- ✅ Scales writes linearly with N
- ❌ Every read = N queries to Cassandra
- ❌ Application must aggregate results

**When to Use:**
- Only for genuinely hot ads (monitoring-based decision)
- When write hotspots are actually causing issues
- When read penalty is acceptable (rare reads)

**Note**: For Flink → Cassandra writes, salting usually not needed since Flink pre-aggregates significantly.

### Challenge 3: Consumer Scaling

**Context**: Kafka consumers (Flink) need to scale based on lag

**Approach:**
- Monitor consumer lag metrics from Kafka
- Auto-scale consumers when lag exceeds threshold
- Kafka handles consumer group rebalancing automatically

**Rebalancing Considerations:**
- Brief pause during rebalancing (stop-the-world effect)
- Can cause temporary lag spikes
- Mitigation: Incremental cooperative rebalancing (newer Kafka versions)

**Interview Guidance:**
- **Mention it exists**: "We'd monitor lag and scale consumers as needed"
- **Don't dive deep** unless interviewer asks specifically
- Focus on core design first (aggregation, storage, API)

## Data Flow

### Write Path

#### Real-time Path (Flink)
1. User clicks ad in browser
2. API Gateway receives event, writes to Kafka with salted partition key: `ad_id_{hash(click_id) % 8}`
3. Kafka durably stores event across multiple partitions (hot partition mitigation)
4. Flink consumes from Kafka
5. Flink strips partition suffix, extracts original ad_id
6. Flink aggregates by ad_id in 1-minute tumbling windows
7. **Progressive Flushing**: Every 10 seconds, Flink UPSERTs partial aggregates
   - Same `(ad_id, minute_bucket)` key updated 6 times per window
8. Flink writes to multiple sinks:
   - **Cassandra minute table**: Progressive UPSERTs
   - **Redis**: Progressive updates (10s latency max)
   - **S3**: Raw event backup (for batch processing)
   - **Kafka**: Republish for downstream consumers

#### Batch Path (Handling Late Arrivals)
```
T+6h:  Hourly batch job
       ├─ Read: S3 raw events [T-6h, T]
       ├─ Aggregate: By ad_id and hour
       └─ Write: UPSERT to ad_metrics_hourly

T+3d:  Daily batch job
       ├─ Read: S3 raw events [T-3d, T]
       ├─ Aggregate: By ad_id and day
       └─ Write: UPSERT to ad_metrics_daily
```

**Why Both Paths:**
- Flink: Low-latency real-time aggregates (seconds)
- Batch: Correctness for late-arriving events (minutes to days late)
- UPSERT semantics: Batch can backfill/correct real-time data

### Read Path (Query Service)

```python
GET /metrics?ad_id=nike_ad&start=2024-01-01&end=2024-01-07

1. Calculate duration: 7 days
2. Select granularity:
   - Duration <= 24h → minute-level
   - Duration <= 7d  → hourly-level
   - Duration > 7d   → daily-level
3. Query appropriate Cassandra table
4. If querying current hour, check Redis for incomplete minute
5. Merge and return results
```

**Decision Tree:**
```
Query span calculation:
  records_to_scan = duration / granularity

If records_to_scan > ~1500:
  → Use coarser granularity

Examples:
  - Last 24h: 1,440 min records → scan minute-level ✓
  - Last 7d:  10,080 min records → use hourly (168 records) ✓
  - Last 30d: 720 hourly records → use daily (30 records) ✓
```

## Capacity Planning

### Kafka
- **Partitions**: 256 partitions (with salting N=8, effective 32 logical ads per partition)
- **Retention**: 7 days for raw events
- **Throughput**: 200k events/sec = ~200 MB/sec (assuming 1KB per event)

### Cassandra
- **Nodes**: 10 nodes for 200k writes/sec (20k writes/node)
- **Replication Factor**: 3
- **Storage**: Compressed aggregates, minimal storage needed

### Redis
- **Memory**: Hot recent data only (last 10 min)
- **Throughput**: 200k writes/sec easily handled by Redis cluster

### Flink
- **Task Managers**: Auto-scale based on Kafka lag
- **Parallelism**: 64-128 parallel tasks
- **State Backend**: RocksDB for large state

## Monitoring & Alerting

### Key Metrics
- Kafka consumer lag (per partition)
- Flink processing latency
- Cassandra write latency (P99)
- Redis memory usage
- Hot partition detection (skew in partition traffic)

### Alerts
- Consumer lag > 10k messages
- Flink job failures/restarts
- Cassandra node write saturation
- Missing aggregates (data quality)

## Architecture Comparison

### Current Design (Hybrid Lambda)
```
Real-time: Flink → minute-level (10s flush interval)
Batch: Spark → hourly (T+6h), daily (T+3d)
Query: Route by duration to appropriate granularity
```

**Pros:**
- ✅ Low latency (10s) for recent data
- ✅ Handles late arrivals correctly (batch backfill)
- ✅ Efficient queries (scan only ~1,500 records max)
- ✅ Storage efficient (TTLs for old fine-grained data)

**Cons:**
- ❌ Two processing pipelines to maintain
- ❌ Slightly more complex query routing logic

### Alternative 1: Pure Streaming (No Batch)
```
Flink only: minute, hourly, daily aggregates with watermarks
```

**Pros:**
- ✅ Simpler: Single processing pipeline
- ✅ Lower operational complexity

**Cons:**
- ❌ Late events (>6h) dropped or cause incorrect aggregates
- ❌ No reprocessing capability
- ❌ Watermark tuning difficult (too aggressive = data loss, too conservative = high latency)

**When to Use:** Acceptable to drop late events (e.g., non-billing use cases)

### Alternative 2: Pure Batch (No Streaming)
```
Spark jobs every minute on S3
```

**Pros:**
- ✅ Perfect correctness (all events processed)
- ✅ Simple reprocessing

**Cons:**
- ❌ Higher latency (minute+ instead of 10s)
- ❌ Higher cost (starting Spark job every minute = overhead)
- ❌ Can't go sub-minute without excessive overhead

**When to Use:** Latency > 1min acceptable, perfect correctness required

## Spark + Cassandra Batch Processing Deep Dive

If you choose the pure batch approach (Alternative 2), here's how to implement it with Spark running every 5 minutes on Cassandra.

### Cassandra Data Model for Batch Processing

**Schema Design:**
```sql
-- Raw events table (written by Click Processor)
CREATE TABLE ad_clicks_raw (
    time_bucket timestamp,    -- Partition key (5-min rounded timestamp)
    shard int,                -- Partition key (for distribution)
    ad_id text,               -- Clustering key
    click_id uuid,            -- Clustering key (for uniqueness)
    user_id text,
    timestamp timestamp,
    metadata map<text, text>,
    PRIMARY KEY ((time_bucket, shard), ad_id, click_id)
) WITH CLUSTERING ORDER BY (ad_id ASC, click_id ASC);
```

**Key Design Decisions:**

1. **Composite Partition Key: `(time_bucket, shard)`**
   - `time_bucket`: Rounded to 5-minute intervals (e.g., 14:30, 14:35, 14:40)
   - `shard`: Integer 0-9 for 10x distribution across nodes
   - **Why**: Prevents hot partitions during high-traffic periods

2. **Clustering Keys: `(ad_id, click_id)`**
   - Groups clicks by ad within each partition
   - `click_id` ensures uniqueness and prevents overwrites

3. **Time Bucket Calculation:**
```python
# API Gateway / Click Processor
import datetime

def get_time_bucket(timestamp):
    """Round timestamp to nearest 5-minute bucket"""
    dt = datetime.datetime.fromtimestamp(timestamp)
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)

def get_shard():
    """Distribute writes across shards"""
    return random.randint(0, 9)  # 0-9 for 10 shards

# Example write
time_bucket = get_time_bucket(event.timestamp)
shard = get_shard()
cassandra.insert(time_bucket, shard, ad_id, click_id, ...)
```

### Spark Batch Job (Every 5 Minutes)

**Cron Schedule:**
```bash
*/5 * * * * spark-submit --class AdClickAggregator aggregator.jar
```

**Spark Job Implementation:**
```scala
import org.apache.spark.sql._
import com.datastax.spark.connector._

object AdClickAggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdClickAggregator")
      .config("spark.cassandra.connection.host", "cassandra-host")
      .getOrCreate()

    import spark.implicits._

    // Current time bucket (e.g., 14:30)
    val currentBucket = getCurrentTimeBucket()

    // Read from ALL shards for this time bucket
    val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> "ad_clicks_raw",
        "keyspace" -> "analytics"
      ))
      .load()
      .where($"time_bucket" === currentBucket)
      // Cassandra Spark Connector automatically handles:
      // - Reading from all 10 shards (0-9)
      // - Pagination across large partitions
      // - Token-aware routing to correct nodes

    // Aggregate by ad_id
    val aggregated = df
      .groupBy("ad_id")
      .agg(
        count("click_id").as("click_count"),
        countDistinct("user_id").as("unique_users"),
        collect_list("user_id").as("user_list")  // Optional
      )

    // Write to OLAP database (Redshift/Snowflake/BigQuery)
    aggregated.write
      .format("jdbc")
      .option("url", "jdbc:redshift://cluster.region.redshift.amazonaws.com:5439/analytics")
      .option("dbtable", "ad_metrics")
      .option("user", "admin")
      .option("password", "password")
      .mode("append")  // Or "overwrite" depending on requirements
      .save()

    // Optional: Write aggregated data back to Cassandra for caching
    aggregated.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> "ad_metrics_5min",
        "keyspace" -> "analytics"
      ))
      .mode("append")
      .save()

    spark.stop()
  }

  def getCurrentTimeBucket(): String = {
    val now = System.currentTimeMillis()
    val fiveMinAgo = now - (5 * 60 * 1000)  // Process previous 5-min bucket
    // Round to 5-minute bucket
    val dt = new java.util.Date(fiveMinAgo)
    // Format: "2026-02-14 14:30:00"
    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dt)
  }
}
```

### Cassandra Pagination Mechanics

**How Cassandra Handles Large Partitions:**

Cassandra supports pagination similar to DynamoDB's `LastEvaluatedKey`, but it's handled automatically by the Cassandra Spark Connector.

**Manual Pagination (Java Driver):**
```java
// Set fetch size (similar to DynamoDB page size)
Statement stmt = QueryBuilder.select()
    .from("analytics", "ad_clicks_raw")
    .where(eq("time_bucket", timestamp))
    .and(eq("shard", 0))
    .setFetchSize(5000);  // Fetch 5000 rows at a time

ResultSet rs = session.execute(stmt);

// Iterate through pages automatically
for (Row row : rs) {
    // Process each row
    // Driver automatically fetches next page when needed
}

// Or manual paging state management
PagingState pagingState = rs.getExecutionInfo().getPagingState();

// Next page query
Statement nextStmt = stmt.setPagingState(pagingState);
ResultSet nextRs = session.execute(nextStmt);
```

**Comparison: Cassandra vs DynamoDB Pagination**

| Feature | DynamoDB | Cassandra |
|---------|----------|-----------|
| **Pagination Token** | `LastEvaluatedKey` (JSON map) | `PagingState` (base64 byte array) |
| **Driver Support** | Manual handling required | Automatic in native driver |
| **Page Size Parameter** | `Limit` | `FetchSize` |
| **Resume Query** | Pass `ExclusiveStartKey` | Pass `PagingState` |
| **Automatic Iteration** | No (manual loop needed) | Yes (driver auto-fetches) |
| **Token Format** | Human-readable JSON | Opaque binary blob |

**Spark Connector Advantage:**
```scala
// NO manual pagination needed!
val df = spark.read
  .format("org.apache.spark.sql.cassandra")
  .options(Map(
    "table" -> "ad_clicks_raw",
    "keyspace" -> "analytics"
  ))
  .load()
  .where($"time_bucket" === currentBucket)

// Cassandra Spark Connector automatically:
// 1. Splits partition reads across Spark executors
// 2. Handles pagination internally (fetches in chunks)
// 3. Distributes work using token ranges
// 4. Provides token-aware routing to Cassandra nodes
```

### Spark Parallelism with Sharded Partitions

**How Spark Processes 10 Shards:**

```scala
// Spark automatically creates 10 tasks (one per shard)
// Each executor reads from different (time_bucket, shard) partitions in parallel

Task 1 → (14:30, shard=0) → Node 1
Task 2 → (14:30, shard=1) → Node 2
Task 3 → (14:30, shard=2) → Node 3
...
Task 10 → (14:30, shard=9) → Node 10

// All tasks run in parallel, then Spark aggregates results
```

**Benefits:**
- ✅ **10x parallelism** vs single partition
- ✅ **No hot nodes** even for high-traffic time buckets
- ✅ **Spark automatically distributes** across executors
- ✅ **Token-aware routing** minimizes network hops

**Without Sharding:**
```
Task 1 → (14:30) → Single Node (bottleneck)
         ↓
   100k rows from one partition
```

**With Sharding:**
```
Task 1-10 → (14:30, 0-9) → 10 Nodes in parallel
            ↓
   10k rows per partition (10x faster)
```

### Data Lifecycle & Cleanup

**TTL Strategy:**
```sql
-- Raw events: Keep for 7 days (for reprocessing)
CREATE TABLE ad_clicks_raw (
    ...
) WITH default_time_to_live = 604800;  -- 7 days

-- Aggregated metrics: No TTL (long-term retention)
CREATE TABLE ad_metrics_5min (
    ad_id text,
    time_bucket timestamp,
    click_count bigint,
    unique_users bigint,
    PRIMARY KEY (ad_id, time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC);
-- No TTL for aggregated data
```

### Batch vs Streaming Trade-offs

**Pure Batch (Spark every 5 min):**

| Aspect | Spark Batch | Flink Streaming |
|--------|-------------|-----------------|
| **Latency** | 5+ minutes | 10 seconds (with flush intervals) |
| **Correctness** | Perfect (processes all events) | Good (needs batch backfill for late arrivals) |
| **Late Arrivals** | Naturally handled (reprocess anytime) | Requires watermarks + batch correction |
| **Infrastructure** | Simpler (cron + Spark) | More complex (Kafka + Flink + state management) |
| **Cost** | Higher (Spark startup overhead every 5 min) | Lower (continuous processing) |
| **Reprocessing** | Easy (just rerun job) | Complex (reset state, replay Kafka) |
| **Sub-minute Granularity** | Not practical | Easy (1-min windows) |
| **Operational Overhead** | Lower (stateless jobs) | Higher (stateful streaming, checkpoints) |

**When to Choose Spark Batch:**
- ✅ Latency requirement > 5 minutes is acceptable
- ✅ Perfect correctness critical (billing, compliance)
- ✅ Frequent reprocessing expected
- ✅ Team has strong Spark expertise (not Flink)
- ✅ Simple operational model preferred

**When to Choose Flink Streaming:**
- ✅ Latency requirement < 1 minute
- ✅ Real-time dashboards/alerts needed
- ✅ Can tolerate rare late event drops
- ✅ Team has Flink expertise
- ✅ Higher throughput (millions of events/sec)

### Spark Configuration Best Practices

**For 200k events/sec (5-min batch = 60M events):**

```bash
spark-submit \
  --class AdClickAggregator \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 8G \
  --executor-cores 4 \
  --num-executors 20 \
  --conf spark.cassandra.connection.host=cassandra-cluster \
  --conf spark.cassandra.input.split.size_in_mb=64 \
  --conf spark.cassandra.input.fetch.size_in_rows=5000 \
  --conf spark.sql.shuffle.partitions=200 \
  aggregator.jar
```

**Key Configurations:**
- `executor-memory`: 8GB per executor for in-memory aggregation
- `num-executors`: 20 executors for parallel shard reads (2 executors per shard)
- `split.size_in_mb`: Controls how Cassandra data is split for Spark
- `fetch.size_in_rows`: Page size for Cassandra reads

### Monitoring & Alerting for Spark Batch

**Key Metrics:**
```python
# CloudWatch / Prometheus metrics
- spark_job_duration_seconds (should be < 4 minutes for 5-min cadence)
- spark_job_failure_count (alert if > 0)
- cassandra_read_latency_p99 (should be < 100ms)
- rows_processed_per_batch (monitor for anomalies)
- time_bucket_lag (current_time - last_processed_bucket, alert if > 10 min)
```

**Alerting Rules:**
```yaml
# If Spark job takes > 4 min, next job will overlap
- alert: SparkJobDurationHigh
  expr: spark_job_duration_seconds > 240
  for: 5m

# If time bucket lag > 15 min, we're falling behind
- alert: BatchProcessingLag
  expr: time_bucket_lag_minutes > 15
  for: 10m
```

### Alternative 3: Direct DB Writes (No Kafka)
```
API Gateway → Cassandra directly
```

**Pros:**
- ✅ Simplest architecture
- ✅ Lowest latency

**Cons:**
- ❌ No buffering (DB downtime = data loss)
- ❌ No replay capability
- ❌ Single consumer (can't add new sinks easily)
- ❌ API Gateway must handle DB backpressure

**When to Use:** Low scale, simple requirements, no multi-consumer needs

### Alternative 4: Different Stream Processors
- **Kafka Streams**: Simpler for Kafka-only, but less powerful windowing
- **Spark Streaming**: Good if already using Spark for batch
- **Flink**: Best for complex event-time processing, exactly-once semantics

## Key Design Decisions Summary

### 1. Flink Flush Intervals (Low Latency)
- **Decision**: 1-minute window, 10-second flush
- **Why**: Advertisers see updates within 10s (vs 60s)
- **Cost**: 6x write volume (totally acceptable)
- **How**: Same `(ad_id, minute)` UPSERTed 6 times progressively

### 2. Multi-Granularity Storage (Query Efficiency)
- **Decision**: Maintain minute, hourly, daily aggregates
- **Why**: Scanning >1,500 records is inefficient
- **Thresholds**:
  - < 24h → minute (1,440 records)
  - 1-7d → hourly (168 records)
  - 7d+ → daily (30-365 records)

### 3. Batch Backfill (Correctness)
- **Decision**: Hourly batch at T+6h, daily batch at T+3d
- **Why**: Handles late arrivals without dropping events
- **Principle**: Write lookback > read threshold (6h > 24h, 3d > 7d)

### 4. Kafka Partition Salting (Hot Partition Mitigation)
- **Decision**: Always salt with `ad_id_{hash % 8}`
- **Why**: Prevents single hot partition bottleneck
- **Trade-off**: Flink strips suffix (minimal overhead)
- **Alternative rejected**: Dynamic detection (too complex)

### 5. Progressive Data Lifecycle (Storage Cost)
- **Decision**: TTLs on minute (48h) and hourly (10d) data
- **Why**: Keep fine-grained only until coarser data complete
- **Storage**: 72GB (minute) → 5GB (hourly) → 500MB/year (daily)

### 6. Hybrid Lambda Architecture (Best of Both)
- **Decision**: Flink for speed + Batch for correctness
- **Why**: 10s latency + handles all late arrivals
- **Trade-off**: Two pipelines vs pure streaming (drops late events)

## Key Takeaways

1. **Flush intervals**: Get sub-minute latency while maintaining minute-level aggregation semantics
2. **Multi-sink capability**: Single Flink job → multiple destinations (Cassandra, Redis, S3, Kafka)
3. **Scan efficiency rule**: If >1,500 records to scan, use coarser granularity
4. **Partition salting**: Always salt for simplicity (don't dynamically detect)
5. **Write lookback > read threshold**: Ensures data completeness at query boundaries
6. **Batch for correctness**: Streaming for speed, batch for late arrivals

## Interview Tips

### What to Lead With
1. **High-level architecture**: Kafka → Flink → Multi-sink (Cassandra, Redis, S3)
2. **Data model**: Show understanding of time-series aggregation patterns
3. **Query routing**: Explain scan efficiency reasoning (1,500 record threshold)

### When Asked About Scaling
- **Hot partitions**: Mention Kafka partition salting (always salt)
- **Consumer scaling**: Brief mention, don't deep-dive unless asked
- **Storage scaling**: TTLs and progressive granularity rollup

### When Asked About Latency
- **Flink flush intervals**: This is the key differentiator (10s vs 60s)
- **Progressive updates**: Explain UPSERT pattern for same key
- **Trade-off**: 6x writes for better UX (acceptable)

### When Asked About Correctness
- **Late arrivals**: Batch jobs at T+6h (hourly), T+3d (daily)
- **UPSERT semantics**: Batch can backfill/correct real-time data
- **Data lifecycle**: Explain why write lookback > read threshold

### What Not to Over-Emphasize
- Dynamic hot partition detection (over-engineered)
- Cassandra partition salting (usually not needed with pre-aggregation)
- Consumer rebalancing details (mention awareness, don't dive deep)

### Priority Order (If Time-Limited)
1. **Architecture & Data Flow** (2-3 min)
2. **Storage Schema & Query Routing** (3-4 min)
3. **Aggregation Strategy** (Flink flush intervals, batch backfill) (2-3 min)
4. **Scaling Concerns** (hot partitions, if asked) (1-2 min)

### Staff Engineer Talking Points
- "We use 1-minute windows with 10s flush intervals for progressive updates—same key UPSERTed 6 times"
- "Query routing based on scan efficiency: if we'd scan >1,500 records, step up to coarser granularity"
- "Write aggregation lookback exceeds read thresholds—6h for hourly ensures data's complete when we query at 24h boundary"
- "Always salt Kafka partitions upfront—simpler than dynamic detection, no downside"
