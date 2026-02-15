# Job Scheduler System Design

## Functional Requirements
- Users can schedule jobs for future or immediate execution
- Users can query for their job status
- Support for one-time and recurring (cron) jobs

## Non-Functional Requirements
- **Availability > Consistency** (with nuance):
  - Job creation/modification: Strong consistency (or read-your-writes) - prevents job running at wrong time after update
  - Job status queries: Eventual consistency acceptable (few seconds stale OK)
  - Job execution path: Read from primary/leader to avoid duplicates
- Execute jobs within 2 seconds of scheduled time
- Scale: 10k jobs per second

## Data Model

### Entity Relationships
```
Task (what to do)
  ↓
Job (definition + schedule)
  ↓
Execution (each run instance)
```

### Tables

**Jobs Table:**
```
- id
- userId
- taskId
- params
- schedule (cron expression or one-time datetime)
```

**Executions Table:**
```
- execution_id
- jobId
- time (concrete execution timestamp)
- userId
- status (PENDING | QUEUED | PROCESSING | DONE | FAILED)
- attempt
- queued_at
- updated_at
```

### Why Separate Job and Execution?
One Job (recurring) produces many Executions:
```
Job: schedule="0 9 * * MON" (every Monday 9am)
  → Execution 1: Jan 13 9am, status=DONE
  → Execution 2: Jan 20 9am, status=FAILED, attempt=2
  → Execution 3: Jan 27 9am, status=PENDING
```

Each execution independently trackable with its own status, retry count, etc.

---

## The Precision Problem

### Why Simple Polling Doesn't Work
Querying DB every 2 seconds for 10k jobs/sec means:
- Fetching ~20k jobs per query (jobs due in next 2s)
- Large payload transfer + serialization overhead
- Query time + network latency eats into precision window
- Heavy DB load affects system stability

### Solution: Two-Layered Architecture

**Phase 1: Database Query (every 5 minutes)**
- Cron queries Executions table for jobs due in next ~5 minutes
- Batch operation, runs infrequently

**Phase 2: Message Queue (precision layer)**
- Jobs pushed to queue with delivery delay
- Workers pull and execute with sub-second precision
- Decouples DB querying from execution timing

---

## Queue Implementation Options

### Option 1: SQS (Managed - Preferred)
- Native delayed message delivery (acts as priority queue)
- Visibility timeout handles worker failures automatically
- Dead-letter queues for failed jobs
- Scales automatically, multi-AZ availability

### Option 2: Redis ZSET (Self-Managed)

When managed solutions aren't allowed.

**Basic Structure:**
```
ZADD job_queue <execution_timestamp> <job_payload_or_id>
```
Score = execution time, sorted ascending automatically.

**Worker Consumption - Lua Script (Atomic):**
```lua
-- Atomically: check if job is due AND pop it
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 1)
if #jobs > 0 then
    redis.call('ZREM', KEYS[1], jobs[1])
    return jobs[1]
end
return nil
```

Called as: `EVALSHA <sha> 1 job_queue <current_timestamp>`

**Why Lua Script?**
- Redis executes Lua atomically (single-threaded)
- No race condition between checking time and removing job
- Multiple workers calling simultaneously each get different jobs

**Redis ZSET Limitations:**
- Score is always numeric (64-bit float), no custom comparators
- For multi-field sorting, encode into single score: `score = (timestamp * 10) + priority`
- Or use separate ZSETs per priority level

---

## Handling Failures

### Failure Window 1: Worker Dies Mid-Processing

**Solution: Processing Queue with Timeout**

```
ready_queue (ZSET)              processing_queue (ZSET)
score = execution_time          score = deadline (now + timeout)
```

**Atomic Lua script - move between queues:**
```lua
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 1)
if #jobs > 0 then
    redis.call('ZREM', KEYS[1], jobs[1])
    redis.call('ZADD', KEYS[2], ARGV[2], jobs[1])  -- add to processing with deadline
    return jobs[1]
end
return nil
```

**Worker Flow:**
```python
job = pop_from_ready_queue()  # moves to processing_queue

try:
    execute(job)
    redis.zrem("processing_queue", job.id)
    dynamodb.update(job.id, status="DONE")
except Exception:
    redis.zrem("processing_queue", job.id)
    redis.zadd("ready_queue", {job.id: time.now()})  # retry
    dynamodb.update(job.id, status="PENDING", attempts=+1)
```

**Reaper Process (runs every few seconds):**
```python
# Find jobs past their deadline (worker crashed)
stuck_jobs = redis.zrangebyscore("processing_queue", 0, time.now())
for job in stuck_jobs:
    redis.zrem("processing_queue", job)
    redis.zadd("ready_queue", {job: time.now()})
```

### Failure Window 2: Job Never Made It to Redis

Cron crashed between DB query and Redis push.

**Solution: Dual-Write with Status Tracking**

Execution statuses: `PENDING → QUEUED → PROCESSING → DONE/FAILED`

**Main Flow:**
1. Cron queries: `WHERE execution_time < now+5min AND status = PENDING`
2. For each job: Push to Redis, update status = QUEUED, queued_at = now
3. Worker pops, updates: status = PROCESSING
4. Worker completes: status = DONE

**Recovery Cron (runs every 1 min - for durability, not precision):**
```sql
-- Jobs that should have been queued but weren't
WHERE execution_time < now AND status = PENDING

-- Jobs stuck in QUEUED too long (Redis lost them)
WHERE status = QUEUED AND queued_at < now - 5min

-- Jobs stuck in PROCESSING (worker died, reaper missed)
WHERE status = PROCESSING AND updated_at < now - timeout
```

This recovery cron is a safety net for edge cases. The 2-second precision comes from Redis, the 1-minute cron provides durability.

---

## Handling New Jobs Scheduled < 5 Minutes Out

Problem: Job created to run in 30 seconds, but cron runs every 5 minutes.

**Solution: Direct Queue Insertion**

On job creation:
```python
if execution_time < now + 5_minutes:
    # Bypass the cron, push directly to Redis/SQS
    push_to_queue(job, delay=execution_time - now)
    update_status(job, "QUEUED")
else:
    # Normal path - cron will pick it up
    update_status(job, "PENDING")
```

**Why Kafka Doesn't Work Here:**
- Kafka processes messages in partition order
- Job scheduled for 10 min from now at head of partition blocks all jobs behind it
- Would need per-job topics or time-bucketed topics (messy)

---

## Architecture Diagram

```
┌────────┐     ┌──────────────────┐     ┌─────────────────────────────────┐
│ Client │────▶│ Schedule Service │────▶│ Job Store (DynamoDB)            │
└────────┘     └──────────────────┘     │  - Jobs table                   │
                                        │  - Executions table             │
                                        └─────────────────────────────────┘
                                                      │
                                                      ▼
                                        ┌─────────────────────────────────┐
                                        │ Watcher (Cron every 5 min)      │
                                        │ - Polls for jobs due soon       │
                                        └─────────────────────────────────┘
                                                      │
                                                      ▼
                                        ┌─────────────────────────────────┐
                                        │ Queue (SQS or Redis ZSET)       │
                                        │ - Delivery delay until exec time│
                                        └─────────────────────────────────┘
                                                      │
                                                      ▼
                                        ┌─────────────────────────────────┐
                                        │ Workers                         │
                                        │ - Execute tasks                 │
                                        │ - Update execution status       │
                                        └─────────────────────────────────┘
```

---

## Key Tradeoffs Summary

| Aspect | SQS | Redis ZSET |
|--------|-----|------------|
| Ops complexity | Managed | Self-managed |
| Visibility timeout | Built-in | Build yourself |
| Retry/DLQ | Built-in | Build yourself |
| Replication | Multi-AZ automatic | Manual setup, potential lag |
| Latency | ~ms | Sub-ms |
| Per-job consumption | Native | Native (ZPOPMIN) |

**Fundamental truth:** Can't have exactly-once without either:
1. Atomic dual-write across systems (hard/impossible)
2. Idempotent retry with state tracking (practical approach)
