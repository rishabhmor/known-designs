# Uber System Design

## Driver Matching with Temporal Workflow

### Architecture Overview

```
┌──────────────┐     ride request      ┌──────────────────┐
│  Ride Service │ ──────────────────▶  │ Temporal Workflow │
└──────────────┘                       └────────┬─────────┘
                                                │
                    ┌───────────────────────────┼───────────────────────────┐
                    │                           │                           │
                    ▼                           ▼                           ▼
           findAndLock()                 sendRideOffer()              releaseLock()
                    │                           │                           │
                    ▼                           ▼                           ▼
            ┌───────────────┐           ┌───────────────┐           ┌───────────────┐
            │   Matching    │           │  Notification │           │   Matching    │
            │   Service     │           │   Service     │           │   Service     │
            └───────┬───────┘           └───────┬───────┘           └───────────────┘
                    │                           │
                    ▼                           ▼
              Redis Geo +                  FCM/APNS
              Redis Lock
```

---

### API Design

**1. `matchingService.findAndLock(riderId, location, excludeDriverIds[])`**

- Queries Redis Geo for nearby available drivers
- Filters out excluded drivers (already tried)
- Attempts to acquire Redis lock on best candidate
- If lock fails, tries next candidate internally
- Returns `{ driverId, lockId }` or `null` if no one available

**Why combine find + lock:**
- Avoids race condition between find returning and lock being called
- Matching Service can try next candidate internally if first lock fails
- Single network hop for the hot path

**2. `notificationService.sendRideOffer(driverId, rideDetails)`**

- Sends push notification via FCM/APNS
- Returns delivery status

**3. `matchingService.releaseLock(lockId)`**

- Releases the Redis lock on driver
- Called on reject/timeout

---

### Temporal Workflow

```python
def ride_matching_workflow(ride_request):
    exclude_driver_ids = []
    max_attempts = 10
    
    for attempt in range(max_attempts):
        # Activity 1: Find available driver and lock
        result = await workflow.execute_activity(
            find_and_lock,
            args=[ride_request.rider_id, ride_request.location, exclude_driver_ids],
            start_to_close_timeout=timedelta(seconds=5)
        )
        
        if result is None:
            # No drivers available, wait and retry or expand radius
            await workflow.sleep(timedelta(seconds=2))
            continue
        
        driver_id, lock_id = result.driver_id, result.lock_id
        
        # Activity 2: Send push notification
        await workflow.execute_activity(
            send_ride_offer,
            args=[driver_id, ride_request],
            start_to_close_timeout=timedelta(seconds=5)
        )
        
        # Wait for driver response (accept signal) or timeout
        try:
            accepted = await workflow.wait_condition(
                lambda: workflow.driver_accepted,
                timeout=timedelta(seconds=10)
            )
            if accepted:
                return {"status": "matched", "driver_id": driver_id}
        except TimeoutError:
            pass
        
        # Rejected or timeout - release lock and try next driver
        await workflow.execute_activity(
            release_lock,
            args=[lock_id],
            start_to_close_timeout=timedelta(seconds=2)
        )
        
        exclude_driver_ids.append(driver_id)
    
    return {"status": "no_driver_found"}
```

---

### Redis Lock Strategy

**Lock key:** `driver:lock:{driver_id}`
**TTL:** 10 seconds (matches offer timeout)

```python
def acquire_driver_lock(driver_id, ride_request_id):
    lock_key = f"driver:lock:{driver_id}"
    # SET NX with TTL - atomic acquire
    acquired = redis.set(lock_key, ride_request_id, nx=True, ex=10)
    return acquired

def release_driver_lock(driver_id, ride_request_id):
    lock_key = f"driver:lock:{driver_id}"
    # Only release if we own the lock (Lua script for atomicity)
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """
    redis.eval(script, keys=[lock_key], args=[ride_request_id])
```

**Why Matching Service owns the lock (not Temporal):**
- Lock management is domain logic, belongs close to matching
- Matching Service can retry internally on lock contention
- Keeps Temporal workflow focused on orchestration

---

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Lock before PN | Fail fast on contention, don't waste PN call if driver already locked |
| Find + Lock in one API | Avoid race between find returning and lock being called |
| Temporal tracks excludeDriverIds | Pass to Matching Service each iteration for filtering |
| Fresh fetch each iteration | Handles driver availability changes naturally (simpler than caching candidate list) |
| 10s offer timeout | Balances driver response time vs rider wait time |

---

### Interview Sound Bite

> "We use Temporal to orchestrate the matching loop. The Matching Service exposes `findAndLock` which queries Redis Geo, filters excluded drivers, and atomically locks the best candidate. On success, Temporal calls Notification Service to send the PN. We lock before notifying to fail fast on contention. If the driver rejects or times out, we release the lock and add them to the exclude list for the next iteration."


---

## Data Pipeline: Redis → Kafka → Data Lake

### Why Kafka for Analytics/ML

Redis handles real-time matching, but for analytics and ML training you need durable, queryable historical data. Instead of dual-writing to DynamoDB, push to Kafka:

```
Driver location update 
    → Redis (real-time matching)
    → Kafka (async, fire-and-forget)
            ↓
    ┌───────┴───────────────┐
    ↓                       ↓
Spark Streaming         Other consumers
    ↓                   (fraud detection, surge pricing, ETA models)
Data Lake (S3/HDFS)
    ↓
Analytics / ML training
```

### Benefits

- **Raw event stream** — Keep every location update, ML teams decide granularity later
- **Decoupled consumers** — Add new use cases without touching Location Service
- **Backfill friendly** — Kafka retention allows replay for new consumers
- **Schema evolution** — Easier to version events in Kafka than retrofitting a database

### Location Service Implementation

```python
def update_driver_location(driver_id, lat, lng, timestamp):
    # Hot path: Redis for real-time matching
    shard = get_redis_shard(lat, lng)
    redis_client = get_redis_client(shard)
    redis_client.geoadd("drivers:active", lng, lat, driver_id)
    
    # Async: Kafka for analytics (fire-and-forget, non-blocking)
    kafka_producer.send(
        topic="driver-locations",
        key=driver_id,
        value={
            "driver_id": driver_id,
            "lat": lat,
            "lng": lng,
            "timestamp": timestamp,
            "geohash": compute_geohash(lat, lng)
        }
    )
```

### Comparison: Kafka vs DynamoDB Dual-Write

| Aspect | Kafka + Data Lake | DDB Dual Write |
|--------|-------------------|----------------|
| Infra complexity | Higher (Kafka, Spark, S3) | Lower |
| Query flexibility | High (SQL on data lake) | Limited (DDB access patterns) |
| Real-time analytics | Spark Streaming | Need DDB Streams → Lambda |
| Cost at scale | Cheaper for high volume | DDB writes add up |
| ML training data | Full history, any granularity | Sampled, fixed schema |

---

## Redis Geo Sharding Strategy

### The Problem

Redis Cluster shards by key hash, not geo-awareness. A single `drivers:active` key would land on one node — doesn't scale for millions of drivers globally.

### Solution: Application-Level Geo Routing

```
Driver location update (lat, lng)
    ↓
Application: resolve_shard(lat, lng) → "us-west-sf"
    ↓
Redis: GEOADD drivers:us-west-sf ...
```

### Geohash-Based Shard Mapping

```python
# Geohash prefix → Redis shard mapping
SHARD_MAP = {
    "9q8y": "redis-sf-1",      # San Francisco downtown (dense)
    "9q8z": "redis-sf-2",      # SF adjacent
    "9q9":  "redis-bayarea",   # Broader Bay Area
    "dr5r": "redis-nyc-1",     # Manhattan (dense)
    "dr5x": "redis-nyc-2",     # Brooklyn
    # ... more mappings
}

def get_redis_shard(lat, lng):
    geohash = compute_geohash(lat, lng, precision=4)
    
    # Try progressively shorter prefixes until match
    for prefix_len in [4, 3, 2]:
        prefix = geohash[:prefix_len]
        if prefix in SHARD_MAP:
            return SHARD_MAP[prefix]
    
    return "redis-default"

def update_driver_location(driver_id, lat, lng):
    shard = get_redis_shard(lat, lng)
    redis_client = get_redis_client(shard)
    redis_client.geoadd("drivers:active", lng, lat, driver_id)
```

### Dense Region Handling

Shard at finer geohash precision for dense areas:

```
Manhattan: 4-char geohash shards (each ~1km²)
Rural Texas: 2-char geohash shards (each ~1000km²)
```

This gives dense regions more shards (more capacity) while keeping rural areas consolidated.

### Cross-Shard Queries (Boundary Edge Case)

When a rider is near a shard boundary, query multiple shards and merge:

```python
def find_nearby_drivers(rider_lat, rider_lng, radius_km):
    # Get all shards that overlap with search radius
    shards = get_overlapping_shards(rider_lat, rider_lng, radius_km)
    
    results = []
    for shard in shards:
        redis_client = get_redis_client(shard)
        drivers = redis_client.georadius(
            "drivers:active", 
            rider_lng, rider_lat, 
            radius_km, unit="km"
        )
        results.extend(drivers)
    
    # Merge and sort by distance
    return sorted(results, key=lambda d: d.distance)[:100]
```

### Responsibility Summary

| What | Who handles it |
|------|----------------|
| Key distribution across nodes | Redis Cluster (hash slots) |
| Geo-aware shard selection | Application layer |
| Cross-boundary queries | Application layer (fan-out + merge) |
| Shard density tuning | Configuration (geohash → shard mapping) |

---

### Interview Sound Bite

> "Redis Cluster shards by key hash, not geography. For geo-aware sharding, we route at the application layer using geohash prefixes — dense areas like Manhattan get finer-grained shards (4-char geohash), rural areas use coarser shards (2-char). For cross-boundary queries, we fan out to overlapping shards and merge results. For analytics, we dual-write to Kafka and use Spark Streaming to sink to a data lake — keeps the hot path fast while giving ML teams full historical data."
