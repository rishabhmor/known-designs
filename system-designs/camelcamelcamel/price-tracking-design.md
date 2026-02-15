# CamelCamelCamel - Price Tracking System Design

## Overview
Price tracking service that monitors product prices across e-commerce sites via browser extension and web crawlers, alerting users when prices drop below their target threshold.

## Core Requirements

### Functional
- Users subscribe to products with price threshold alerts
- Browser extension + web crawlers collect price data
- Send notifications when price drops below threshold
- Historical price charts (multi-granularity: hourly, daily, monthly)
- Support millions of products, millions of users

### Non-Functional
- Handle billions of price records (time-series data)
- Fast queries for price history (any time range)
- Low latency notifications (<1 minute from price change)
- Scalable writes (10K+ price updates/sec)

---

## Database Design Strategy

### Core Entities: PostgreSQL vs DynamoDB

Two valid approaches for Users, Products, and Subscriptions:

#### Option 1: PostgreSQL (Recommended for this design)

**Tables:**
```sql
-- Users
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Products
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    url VARCHAR(500) UNIQUE NOT NULL,
    title VARCHAR(500),
    current_price DECIMAL(10,2),
    last_scraped TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Subscriptions (active working set)
CREATE TABLE subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    product_id INT REFERENCES products(product_id),
    price_threshold DECIMAL(10,2),
    status VARCHAR(20) DEFAULT 'active', -- active, paused, expired
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expire_at TIMESTAMPTZ, -- optional for cleanup
    UNIQUE(user_id, product_id)
);

-- Indexes
CREATE INDEX idx_subscriptions_product ON subscriptions(product_id)
    WHERE status = 'active'; -- Partial index for active subscriptions only
CREATE INDEX idx_subscriptions_user ON subscriptions(user_id)
    WHERE status = 'active';
```

**Scale Management:**
- Keep subscriptions table small via periodic cleanup (archive/delete expired)
- Partial indexes on `status='active'` keep index size in RAM
- Use table partitioning (by user_id hash or time) if >100M active subscriptions
- Read replicas for read-heavy queries

**When PostgreSQL works well:**
- Up to 5-10TB active data with partitioning
- Write rates <10K/sec
- Complex queries, joins, transactions needed
- Schema stability

#### Option 2: DynamoDB (Alternative approach)

**Tables:**
```
Users Table
- PK: user_id
- Attributes: email, created_at

Products Table
- PK: product_id
- Attributes: url, title, current_price, last_scraped

Subscriptions Table
- PK: user_id
- SK: product_id
- GSI on product_id (to query "which users subscribe to this product?")
- Attributes: price_threshold, status, created_at
```

**When DynamoDB is better:**
- Write-heavy workloads (>10K writes/sec sustained)
- Need unlimited horizontal scale
- Simple access patterns (key-value, single-table queries)
- Variable/spiky traffic with auto-scaling
- Multi-region active-active requirements

---

## Time-Series Data: Price History

### PostgreSQL + TimescaleDB (Recommended)

TimescaleDB is a **PostgreSQL extension** (not a separate database) that adds specialized time-series features.

#### Architecture

```
┌─────────────────────────────────────┐
│   Single PostgreSQL Instance        │
│                                     │
│  ┌───────────────────────────────┐ │
│  │   TimescaleDB Extension       │ │
│  │   (loaded into Postgres)      │ │
│  └───────────────────────────────┘ │
│                                     │
│  Regular Tables:                   │
│   - users                          │
│   - products                       │
│   - subscriptions                  │
│                                     │
│  Hypertables (TimescaleDB):        │
│   - price_history                  │
│     └─> Auto-partitioned by time   │
└─────────────────────────────────────┘
```

#### Setup

```sql
-- 1. Enable TimescaleDB extension
CREATE EXTENSION timescaledb;

-- 2. Create price history table
CREATE TABLE price_history (
    time TIMESTAMPTZ NOT NULL,
    product_id INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    source VARCHAR(50), -- 'crawler', 'extension', 'api'
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 3. Convert to hypertable (TimescaleDB manages this)
SELECT create_hypertable('price_history', 'time');

-- 4. Add indexes
CREATE INDEX idx_price_product_time ON price_history (product_id, time DESC);
```

#### Automatic Chunking

TimescaleDB automatically partitions data into time-based chunks:

```
price_history (logical view - user queries this)
├─ chunk_202401: Jan 2024 data (50M rows) - COMPRESSED
├─ chunk_202402: Feb 2024 data (50M rows) - COMPRESSED
├─ chunk_202403: Mar 2024 data (50M rows) - COMPRESSED
├─ chunk_202411: Nov 2024 data (50M rows) - COMPRESSED
├─ chunk_202412: Dec 2024 data (50M rows) - COMPRESSED
└─ chunk_202501: Jan 2025 data (50M rows) - UNCOMPRESSED (recent, active writes)
```

Queries automatically scan only relevant chunks.

#### Compression Strategy

```sql
-- Enable automatic compression for chunks older than 7 days
ALTER TABLE price_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'product_id',
    timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy
SELECT add_compression_policy('price_history', INTERVAL '7 days');
```

**Storage savings:**
- Uncompressed: 10B records × 40 bytes = 400 GB
- Compressed (columnar): ~40 GB (10x reduction)
- Indexes also compressed proportionally

**Performance benefit:**
- Smaller indexes → more likely to fit in RAM
- Faster queries on historical data

---

## Multi-Granularity Aggregations

### Continuous Aggregates (Pre-computed Rollups)

TimescaleDB automatically maintains materialized views for different time granularities:

```sql
-- Hourly averages (for last 30 days queries)
CREATE MATERIALIZED VIEW price_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    product_id,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as num_samples
FROM price_history
GROUP BY hour, product_id;

-- Daily averages (for last year queries)
CREATE MATERIALIZED VIEW price_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    product_id,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as num_samples
FROM price_history
GROUP BY day, product_id;

-- Monthly averages (for multi-year queries)
CREATE MATERIALIZED VIEW price_monthly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', time) AS month,
    product_id,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as num_samples
FROM price_history
GROUP BY month, product_id;
```

#### Automatic Updates

```sql
-- Hourly aggregate: refresh every hour
SELECT add_continuous_aggregate_policy('price_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Daily aggregate: refresh once per day
SELECT add_continuous_aggregate_policy('price_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- Monthly aggregate: refresh once per day
SELECT add_continuous_aggregate_policy('price_monthly',
    start_offset => INTERVAL '60 days',
    end_offset => INTERVAL '30 days',
    schedule_interval => INTERVAL '1 day');
```

**How it works:**
- New price data inserted → background jobs incrementally update aggregates
- Only affected time buckets recomputed (not entire dataset)
- Transactional consistency maintained

#### Query Strategy by Time Range

```python
def get_price_chart(product_id, time_range):
    if time_range <= timedelta(days=7):
        # Last week: raw data (finest granularity)
        query = """
            SELECT time, price
            FROM price_history
            WHERE product_id = %s AND time > NOW() - INTERVAL '7 days'
        """
        return execute(query, product_id)

    elif time_range <= timedelta(days=90):
        # Last 3 months: hourly aggregates
        query = """
            SELECT hour as time, avg_price as price
            FROM price_hourly
            WHERE product_id = %s AND hour > NOW() - INTERVAL '90 days'
        """
        return execute(query, product_id)

    elif time_range <= timedelta(days=365):
        # Last year: daily aggregates
        query = """
            SELECT day as time, avg_price as price
            FROM price_daily
            WHERE product_id = %s AND day > NOW() - INTERVAL '1 year'
        """
        return execute(query, product_id)

    else:
        # Multi-year: monthly aggregates
        query = """
            SELECT month as time, avg_price as price
            FROM price_monthly
            WHERE product_id = %s AND month > NOW() - INTERVAL '5 years'
        """
        return execute(query, product_id)
```

**Performance comparison:**

| Time Range | Granularity | Rows Scanned | Query Time | Data Source |
|------------|-------------|--------------|------------|-------------|
| 7 days | Raw | ~10K | ~50ms | price_history |
| 30 days | Hourly | ~720 | ~10ms | price_hourly |
| 1 year | Daily | ~365 | ~5ms | price_daily |
| 5 years | Monthly | ~60 | ~2ms | price_monthly |

---

## Data Retention Strategy

Keep different granularities for different durations to optimize storage:

```sql
-- Raw data: keep 30 days only
SELECT add_retention_policy('price_history', INTERVAL '30 days');

-- Hourly aggregates: keep 6 months
SELECT add_retention_policy('price_hourly', INTERVAL '6 months');

-- Daily aggregates: keep 5 years
SELECT add_retention_policy('price_daily', INTERVAL '5 years');

-- Monthly aggregates: keep forever (or 10 years)
SELECT add_retention_policy('price_monthly', INTERVAL '10 years');
```

**Storage breakdown (per product tracked for 2 years):**

Without retention:
- 2 years × 365 days × 100 samples/day = 73K rows
- Storage: ~3 MB per product × 10M products = 30 TB

With retention + aggregates:
- Raw (30 days): 3K rows
- Hourly (6 months): ~4,300 rows
- Daily (2 years): 730 rows
- Monthly (2 years): 24 rows
- Total: ~300 KB per product × 10M products = 3 TB

**20x storage reduction!**

---

## PostgreSQL Index Architecture

### How Indexes Work

PostgreSQL uses **heap storage** for tables and **B-tree indexes** with pointers:

```
Table: price_history (heap storage - full rows)
┌─────────────────────────────────────────────────────┐
│ Row 1: time=..., product_id=12345, price=99.99, ... │  (40 bytes)
│ Row 2: time=..., product_id=12345, price=89.99, ... │  (40 bytes)
│ Row 3: time=..., product_id=67890, price=199.99,... │  (40 bytes)
└─────────────────────────────────────────────────────┘
Stored at: Page 42, Slot 1
           Page 42, Slot 2
           Page 100, Slot 7

Index: idx_price_product_time (B-tree)
┌──────────────────────────────────────────────┐
│ product_id=12345, time=... → TID (42, 1)     │  (18 bytes)
│ product_id=12345, time=... → TID (42, 2)     │  (18 bytes)
│ product_id=67890, time=... → TID (100, 7)    │  (18 bytes)
└──────────────────────────────────────────────┘
```

**TID (Tuple Identifier):**
- 6-byte pointer: (page_number, slot_number)
- Points to exact location in heap storage

**Size comparison (1 billion rows):**
- Heap storage: 40 bytes/row = 40 GB
- Index on (product_id, time): ~18 bytes/entry = 18 GB
- Index is **2-3x smaller** than heap

### Query Execution Flow

```sql
SELECT * FROM price_history WHERE product_id = 12345;
```

**With index in RAM:**
1. **Index scan** (in RAM): Find all TIDs for product_id=12345 (~microseconds)
2. **Direct jump to heap**: Use TIDs to read exact pages from disk (1-5ms per row)
3. **Return full rows**: All columns returned

**Without index (sequential scan):**
1. Read entire 40 GB table from disk
2. Check each row's product_id
3. Much slower (~minutes)

**Key insight:** Index in RAM eliminates table scanning. You jump **directly** to the data on disk.

### Index-Only Scans

If query only needs indexed columns, skip heap entirely:

```sql
-- Index on (product_id, time)
SELECT product_id, time
FROM price_history
WHERE product_id = 12345;
```

**Execution:**
1. Scan index
2. Data already in index (product_id, time)
3. **No heap access needed!**
4. 10x faster (no disk I/O for heap)

This is why **covering indexes** matter - include all commonly queried columns.

### RAM Requirements

**Rule of thumb:** Index working set should fit in RAM

```
Table: 1 TB (10B rows)
Indexes:
- (product_id, time): 150 GB
- (product_id): 80 GB
- (time): 80 GB
Total indexes: 310 GB

If available RAM = 128 GB:
- Active queries use ~2 indexes
- Those indexes must fit in 128 GB
- Use partial indexes, compression, or limit index count
```

**PostgreSQL shared_buffers configuration:**
```
shared_buffers = 32GB  # 25% of RAM typically
effective_cache_size = 96GB  # OS + Postgres cache combined
```

---

## Event-Driven Notifications

### Architecture: Change Data Capture (CDC)

When price changes, immediately notify affected users without expensive scans.

```
┌──────────────┐
│   Crawler/   │
│  Extension   │
└──────┬───────┘
       │ Insert new price
       ▼
┌─────────────────┐         CDC         ┌──────────────┐
│  Price History  │────────────────────►│ Price Change │
│  (TimescaleDB)  │   (DB trigger or    │    Worker    │
└─────────────────┘    logical rep)     └──────┬───────┘
                                               │
                                               ▼
                                        ┌──────────────┐
                                        │    Kafka     │
                                        │  price_event │
                                        └──────┬───────┘
                                               │
                                               ▼
                                      ┌─────────────────┐
                                      │  Notification   │
                                      │    Service      │
                                      └────────┬────────┘
                                               │
                    ┌──────────────────────────┼───────────────────┐
                    ▼                          ▼                   ▼
              Query subscriptions      Check threshold      Send alerts
              WHERE product_id=?       IF new_price <=      (Email/Push)
                                      threshold
```

#### Implementation Option 1: Database Triggers

```sql
-- Function to publish price changes
CREATE OR REPLACE FUNCTION notify_price_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Publish to external queue (using pg_notify or external system)
    PERFORM pg_notify('price_changes',
        json_build_object(
            'product_id', NEW.product_id,
            'old_price', OLD.price,
            'new_price', NEW.price,
            'timestamp', NEW.time
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on price updates
CREATE TRIGGER price_change_trigger
AFTER INSERT ON price_history
FOR EACH ROW
EXECUTE FUNCTION notify_price_change();
```

**Notification service consumes events:**
```python
# Pseudo-code
def handle_price_change_event(event):
    product_id = event['product_id']
    new_price = event['new_price']

    # Query subscriptions (fast with index on product_id)
    subscriptions = db.query("""
        SELECT user_id, email, price_threshold
        FROM subscriptions s
        JOIN users u ON s.user_id = u.user_id
        WHERE s.product_id = %s
          AND s.status = 'active'
          AND %s <= s.price_threshold
    """, product_id, new_price)

    # Send notifications
    for sub in subscriptions:
        send_email(sub.email, f"Price dropped to ${new_price}!")
```

#### Implementation Option 2: Debezium CDC

Use Debezium to stream PostgreSQL changes to Kafka:

```yaml
# Debezium connector config
connector.class: io.debezium.connector.postgresql.PostgresConnector
database.hostname: postgres-host
database.port: 5432
database.user: postgres
database.dbname: price_tracker
table.include.list: public.price_history
publication.name: price_changes_pub
```

**Benefits:**
- Decoupled from database
- Reliable event delivery (Kafka persistence)
- Can replay events
- Multiple consumers possible

---

## Scale Considerations

### PostgreSQL Vertical Scale Limits

**Single instance can handle:**
- **Storage:** 5-10 TB active data with partitioning
- **Writes:** 10-20K writes/sec with tuning
- **Reads:** Nearly unlimited with read replicas
- **Row count:** Billions with proper indexing

**When to shard or switch:**
- Sustained writes >20K/sec
- Active working set >10 TB
- Single-instance becomes bottleneck
- Need multi-region active-active writes

**Sharding strategies (if needed):**
- Shard by product_id hash (Citus extension)
- Shard by geography (US, EU, Asia databases)
- Hybrid: Keep metadata in Postgres, move time-series to specialized store

### TimescaleDB Specific Optimizations

```sql
-- Tune chunk interval based on data volume
SELECT set_chunk_time_interval('price_history', INTERVAL '1 day');
-- Default is 7 days; reduce for very high write volumes

-- Enable parallel query execution
SET max_parallel_workers_per_gather = 4;

-- Use distributed hypertables (multi-node TimescaleDB) for extreme scale
-- Requires TimescaleDB clustering setup
```

---

## Alternative: DynamoDB for Time-Series

If you choose DynamoDB for price history:

### Schema Design

```
Table: PriceHistory
PK: product_id#granularity#period
SK: timestamp
Attributes: price, avg_price, min_price, max_price, sample_count

Examples:
- PK: "12345#raw#2024-02-12", SK: "2024-02-12T10:30:15Z"
  → Raw price data

- PK: "12345#hourly#2024-02-12", SK: "2024-02-12T10:00:00Z"
  → Hourly aggregate

- PK: "12345#daily#2024-02", SK: "2024-02-12"
  → Daily aggregate
```

### Pros vs TimescaleDB

**DynamoDB advantages:**
- Unlimited horizontal scale
- Auto-scaling writes
- Multi-region with global tables
- No infrastructure management

**TimescaleDB advantages:**
- Automatic continuous aggregates (vs manual Lambda rollups)
- Compression built-in (vs pay per GB)
- Complex queries (JOINs, analytical functions)
- Cost-effective at medium scale
- Familiar SQL interface

---

## System Architecture Diagram

```
┌──────────────┐         ┌──────────────┐
│   Browser    │         │     Web      │
│  Extension   │         │   Crawler    │
└──────┬───────┘         └──────┬───────┘
       │                        │
       │   POST /prices         │
       └────────────┬───────────┘
                    ▼
              ┌──────────┐
              │   API    │
              │ Gateway  │
              └─────┬────┘
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
┌────────────┐ ┌─────────┐ ┌─────────────┐
│Price Update│ │ Product │ │Subscription │
│  Service   │ │ Service │ │   Service   │
└─────┬──────┘ └────┬────┘ └──────┬──────┘
      │             │              │
      └─────────────┼──────────────┘
                    ▼
         ┌──────────────────────┐
         │  PostgreSQL +        │
         │  TimescaleDB         │
         │                      │
         │ Tables:              │
         │  - users             │
         │  - products          │
         │  - subscriptions     │
         │                      │
         │ Hypertables:         │
         │  - price_history     │
         │                      │
         │ Continuous Aggs:     │
         │  - price_hourly      │
         │  - price_daily       │
         │  - price_monthly     │
         └──────────┬───────────┘
                    │ CDC/Triggers
                    ▼
              ┌──────────┐
              │  Kafka   │
              │price_event
              └─────┬────┘
                    │
                    ▼
         ┌──────────────────┐
         │  Notification    │
         │    Service       │
         └─────┬────────────┘
               │
               ▼
         ┌──────────┐
         │  Email/  │
         │   Push   │
         └──────────┘
```

---

## Key Takeaways

### Database Choice

**Use PostgreSQL + TimescaleDB when:**
- Complex queries, joins, ACID transactions needed
- Time-series data with aggregations
- Team comfortable with SQL
- Cost-conscious (self-hosted cheaper at medium scale)
- Scale <10TB, <10K writes/sec

**Use DynamoDB when:**
- Simple access patterns (key-value, single-table)
- Need unlimited horizontal scale (>20K writes/sec)
- Multi-region active-active required
- Want managed service with zero ops
- Spiky/unpredictable traffic

### PostgreSQL Index Strategy

- **Indexes are pointers:** Small (2-3x smaller than heap), point to exact data location
- **RAM is critical:** Working set indexes should fit in memory for fast queries
- **Partial indexes:** Use `WHERE status='active'` to keep indexes small
- **Index-only scans:** Include all queried columns for maximum speed

### TimescaleDB Features

- **Not a separate database:** Extension loaded into PostgreSQL
- **Automatic chunking:** Time-based partitioning with no manual setup
- **Compression:** 5-20x reduction with columnar storage
- **Continuous aggregates:** Pre-computed rollups (hourly, daily, monthly)
- **Retention policies:** Auto-delete old data at different granularities

### Multi-Granularity Pattern

- Raw data: Last 7-30 days (finest granularity)
- Hourly aggregates: 30 days to 6 months
- Daily aggregates: 6 months to 5 years
- Monthly aggregates: Long-term (5+ years)

This pattern optimizes storage, query speed, and data freshness.

---

## References

- TimescaleDB: https://docs.timescale.com/
- PostgreSQL Performance Tuning: https://wiki.postgresql.org/wiki/Performance_Optimization
- CamelCamelCamel inspiration: https://camelcamelcamel.com/
