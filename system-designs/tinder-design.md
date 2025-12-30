# Tinder System Design

## Geo-Location Store: Redis vs Elasticsearch

### Decision: **Elasticsearch** for Tinder

Tinder requires **geo + multi-field filtering** in a single query:
- Distance < X km from current location
- Age within range
- Gender matching preferences
- Interests overlap
- Exclude already-swiped profiles

This is exactly what Elasticsearch excels at.

---

### Why Not Redis Geo?

| Challenge | Why Redis Falls Short |
|-----------|----------------------|
| Multi-field filtering | Redis Geo only returns by proximity. You'd fetch candidates then filter in app — wasteful |
| 100s of millions in a region | Even with regional sharding, a city like NYC could have millions. Fetching all then filtering = high latency |
| Inverted index needs | "Find people interested in males aged 25-30" is an inverted index problem |

---

### When to Use Redis Geo Instead

- **Uber/Lyft**: Simple "find drivers within 5km" with sub-second location updates
- **Delivery tracking**: Real-time location, minimal filtering
- **"Who's online nearby"**: Pure proximity, no complex filters

**Rule of thumb:**
- Just proximity queries → Redis Geo
- Proximity + complex filters → Elasticsearch
- Real-time location updates (every few seconds) → Redis Geo

---

### Elasticsearch Architecture for Tinder

```
┌─────────────────────────────────────────┐
│           Regional ES Clusters          │
│  (US-West, US-East, Europe, Asia...)    │
└─────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │   Index per region    │
        │   Sharded by geo-hash │
        └───────────────────────┘
```

**Index mapping:**
```json
{
  "location": { "type": "geo_point" },
  "age": { "type": "integer" },
  "gender": { "type": "keyword" },
  "interested_in": { "type": "keyword" },
  "interests": { "type": "keyword" }
}
```

**Example query:**
```json
{
  "bool": {
    "filter": [
      { "geo_distance": { "distance": "50km", "location": {"lat": 40.7, "lon": -74.0} }},
      { "range": { "age": { "gte": 22, "lte": 30 }}},
      { "term": { "gender": "female" }},
      { "term": { "interested_in": "male" }}
    ],
    "must_not": [
      { "ids": { "values": ["already_swiped_user_ids"] }}
    ]
  }
}
```

---

### Handling Scale (100s of millions of profiles)

1. **Regional clusters** — Route users to nearest ES cluster
2. **Shard by geohash prefix** — Users in same area hit same shard (cache-friendly)
3. **Cache the feed** — Don't query ES per swipe; fetch 50-100 candidates, cache in Redis, serve from there
4. **Refresh on triggers** — Location change, preference change, or cache exhausted → new ES query

---

### Interview Sound Bite

> "For Tinder, I'd use Elasticsearch because we need geo-distance combined with multi-criteria filtering (age, gender, preferences). Redis Geo is great for pure proximity but would require fetching all nearby users then filtering in-app, which is inefficient at scale. We'd shard ES regionally and cache the generated feed in Redis to avoid hitting ES on every swipe."

---

## Low Latency Feed/Stack Generation

### The Problem

When a user opens the app, they want to immediately start swiping. Running a slow query every time is unacceptable:

```sql
SELECT * FROM users
WHERE age BETWEEN 18 AND 35
AND interestedIn = 'female'
AND lat BETWEEN userLat - maxDistance AND userLat + maxDistance
AND long BETWEEN userLong - maxDistance AND userLong + maxDistance
```

### Solution: Hybrid Caching + Indexed Database

Combine pre-computation with real-time querying:

```
┌─────────────────────────────────────────────────────────────┐
│                    FEED SERVING FLOW                        │
└─────────────────────────────────────────────────────────────┘

1. CACHE MORE THAN NEEDED
   - Cache 200 profiles in Redis, serve 20 at a time
   - Gives buffer for runtime filtering

2. RUNTIME FILTER AT SERVE TIME
   - Quick validation: location still valid? preferences match? profile active?
   - Filter out obviously stale profiles
   - Cheap: O(n) in-memory checks

3. BACKGROUND REFRESH TRIGGERS
   - When filtered_remaining < threshold (e.g., 30)
   - When user changes location significantly
   - When user updates preferences
   - Async job → doesn't block user

4. SOFT TTL
   - Even if cache has profiles, refresh if cache age > 1 hour
   - Ensures new users in area get discovered
```

---

### Why Runtime Filtering?

Instead of purely relying on TTL + background recomputation, apply a lightweight filter at serve time:

```python
def get_feed(user_id, count=20):
    cached_feed = redis.get(f"feed:{user_id}")
    user_prefs = get_user_preferences(user_id)
    user_location = get_current_location(user_id)
    
    # Runtime filter
    valid_profiles = []
    for profile in cached_feed:
        if is_still_valid(profile, user_prefs, user_location):
            valid_profiles.append(profile)
    
    # Trigger background refresh if running low
    if len(valid_profiles) < REFRESH_THRESHOLD:
        trigger_async_refresh(user_id)  # Non-blocking
    
    return valid_profiles[:count]

def is_still_valid(profile, user_prefs, user_location):
    distance = haversine(profile.location, user_location)
    if distance > user_prefs.max_distance:
        return False
    if profile.age < user_prefs.age_min or profile.age > user_prefs.age_max:
        return False
    if profile.is_deleted or profile.is_suspended:
        return False
    return True
```

**Trade-offs:**

| Aspect | Runtime Filter | TTL-Only Refresh |
|--------|---------------|------------------|
| Freshness | Better (real-time validation) | May serve stale until TTL expires |
| Read latency | Slightly higher (filter cost) | Lower (direct cache read) |
| Complexity | Needs current profile data access | Simpler |
| New profiles | Won't add newly valid profiles | Full refresh catches them |

---

### Stale Feed Triggers

Profiles become stale when:
- Suggested user changed location (no longer nearby)
- Suggested user changed profile/preferences
- Current user changed their filters
- Current user moved to a different area

**Solution:** User-triggered actions (location change, preference update) kick off async cache refresh.

---

### Interview Sound Bite

> "We cache more profiles than needed (e.g., 200) and apply a lightweight runtime filter at serve time to remove stale ones. We trigger background refresh when the filtered count drops below a threshold, so active users never hit a wall. This hybrid approach balances freshness, latency, and compute cost."

---

## New User Visibility (Cold Start Problem)

### The Problem

New user signs up → Their profile isn't in anyone's cached feeds → They're "invisible" for up to 1 hour until feeds refresh.

This hurts new user experience and retention.

---

### Why "Push to All Feeds" Doesn't Scale

Intuitive solution: When a new profile is created, push it into all matching users' cached feeds.

| Challenge | Impact |
|-----------|--------|
| **Fan-out explosion** | New user in NYC could match 500K+ people = 500K cache writes |
| **Write amplification** | 10K signups/hour × 500K writes = 5 billion cache ops/hour |
| **Ranking at write time** | Computing match score for 500K pairs adds latency |

**Verdict:** Mention this in interview, then explain why it fails at scale.

---

### Better Solutions

#### 1. "New User Boost" in ES Query

When generating/refreshing ANY user's feed, boost recently created profiles:

```json
{
  "bool": {
    "should": [
      {
        "range": {
          "created_at": {
            "gte": "now-24h",
            "boost": 2.0
          }
        }
      }
    ],
    "filter": [
      // ... existing geo + preference filters
    ]
  }
}
```

New users surface organically as feeds refresh. No fan-out problem.

---

#### 2. Hybrid: Reserved "New Profiles" Slice

Reserve a portion of each served feed for fresh profiles:

```
Cached Feed Structure:
┌──────────────────────────────────────────┐
│  [80% cached profiles] + [20% real-time] │
└──────────────────────────────────────────┘
                              │
                              ▼
              Fetched from ES at serve time
              (new profiles from last 6-24 hours)
```

New users appear within hours without fan-out cost.

---

#### 3. Push Only to Active Users

If some push behavior is needed:

```
New profile created
    → Find users currently swiping (small set, ~100K globally)
    → Push only to their feeds
    → Others get it on natural refresh
```

Much smaller fan-out — only pushing to users literally swiping right now.

---

### Comparison

| Approach | Scalability | New User Latency | Interview Take |
|----------|-------------|------------------|----------------|
| Push to all feeds | ❌ Doesn't scale | Instant | Mention, explain failure |
| New user boost in ES | ✅ Scales | 1-2 hours | ✅ Good answer |
| Hybrid (cached + real-time slice) | ✅ Scales | 15-30 mins | ✅ Great answer |
| Push to active users only | ⚠️ Moderate | 5-10 mins for active | ✅ Shows nuance |

---

### Interview Sound Bite

> "New user visibility is critical for growth. Pushing to all matching feeds has a massive fan-out problem at scale. Instead, I'd use a 'new user boost' in the ES query — recently created profiles get higher scores. Combined with natural feed refresh, new users surface within the hour. For faster visibility, we could reserve 20% of each feed for a real-time 'new profiles' slice fetched at serve time."

---

## Swipe Consistency and Match Detection

### The Problem: Race Condition

When two users swipe right on each other nearly simultaneously:

```
Timeline (without atomicity):
────────────────────────────────────────────────────
T1: A's swipe hits server → check for B's swipe → Nothing
T2: B's swipe hits server → check for A's swipe → Nothing
T3: Save A's swipe on B
T4: Save B's swipe on A
────────────────────────────────────────────────────
Result: Both swipes saved, but MATCH NEVER DETECTED!
```

True love lost forever. We need **atomic read+write**.

---

### Why Cassandra Can't Handle This

#### The Core Issue: BATCH Doesn't Support SELECT

```sql
BEGIN BATCH
    INSERT INTO swipes (user_pair, from_user, to_user, direction)
    VALUES (?, ?, ?, ?);
    
    SELECT direction FROM swipes   -- ❌ INVALID!
    WHERE user_pair = ? 
    AND from_user = ?;
APPLY BATCH;
```

**Cassandra BATCH only supports INSERT, UPDATE, DELETE — not SELECT.**

You cannot atomically read-then-write in a single Cassandra operation.

#### Other Cassandra Limitations

| Approach | Problem |
|----------|---------|
| Regular BATCH | Can't include SELECT |
| Separate read + write | Race condition window exists |
| LWT (Lightweight Transactions) | Uses Paxos, 4x slower, creates leader bottleneck |
| QUORUM consistency | Consistency ≠ atomicity; race window still exists |

---

### Solution 1: Redis with Lua Scripts (Recommended)

Redis Lua scripts execute **atomically** — nothing else runs during the script.

**Key structure:**
```
Key: "swipes:123:456"  (sorted user IDs)
Value: {
    "123_swipe": "right",
    "456_swipe": "left"
}
```

**Implementation:**
```python
def get_key(user_a, user_b):
    # Sort IDs so (A→B) and (B→A) map to same key
    sorted_ids = sorted([user_a, user_b])
    return f"swipes:{sorted_ids[0]}:{sorted_ids[1]}"

def handle_swipe(from_user, to_user, direction):
    key = get_key(from_user, to_user)
    
    # Lua script: atomic set + get
    script = """
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    return redis.call('HGET', KEYS[1], ARGV[3])
    """
    
    other_swipe = redis.eval(
        script,
        keys=[key],
        args=[
            f"{from_user}_swipe",  # field to set
            direction,             # our swipe
            f"{to_user}_swipe"     # field to check
        ]
    )
    
    # If both swiped right, it's a match!
    if direction == 'right' and other_swipe == 'right':
        create_match(from_user, to_user)
        return {"matched": True}
    return {"matched": False}
```

**Why this works:**
- Redis is single-threaded per shard
- Lua script = atomic operation
- Read + Write happen as ONE operation
- No race condition possible

---

### Solution 2: OCC (Optimistic Concurrency Control)

For databases with native OCC support (DynamoDB, Cosmos, PostgreSQL):

```python
def handle_swipe(from_user, to_user, direction):
    key = get_user_pair(from_user, to_user)
    
    while True:
        # Read current state + version
        record = db.get(key)
        version = record.version if record else 0
        
        # Prepare update
        updates = {f"{from_user}_swipe": direction}
        
        try:
            # Conditional write with version check
            db.put(key, updates, expected_version=version)
            break  # Success
        except VersionConflict:
            continue  # Retry — will now see both swipes
    
    # Check for match
    final_record = db.get(key)
    if final_record.get(f"{from_user}_swipe") == "right" and \
       final_record.get(f"{to_user}_swipe") == "right":
        return {"matched": True}
    return {"matched": False}
```

**How it resolves the race:**
```
T1: A reads → version=0, no B swipe
T2: B reads → version=0, no A swipe
T3: A writes (version 0→1) → SUCCESS
T4: B writes (version 0→1) → CONFLICT! Retry...
T5: B reads → version=1, sees A's swipe
T6: B writes (version 1→2) → SUCCESS
T7: B checks → both swiped right → MATCH DETECTED
```

At least one of them detects the match. ✅

---

### Comparison: Redis vs OCC

| Factor | Redis Lua Script | OCC (DynamoDB/Postgres) |
|--------|------------------|------------------------|
| Atomicity | Native (single operation) | Achieved via retry loop |
| Latency | Constant (no retries) | Higher on conflict |
| Complexity | Simpler | More code (retry logic) |
| Cassandra support | N/A | ❌ No native OCC |

---

### Hybrid Architecture

Use both Redis and Cassandra for best of both worlds:

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   Client    │──────▶│    Redis    │──────▶│  Cassandra  │
│             │       │  (atomic    │  async│  (durable   │
│             │       │   matching) │       │   storage)  │
└─────────────┘       └─────────────┘       └─────────────┘
                            │
                            ▼
                      Match detected?
                      → Create match record
                      → Send push notification
```

- **Redis**: Handles atomic matching logic (in-memory, fast)
- **Cassandra**: Durable storage of all swipes (async write)
- **If Redis loses data**: Only lose match detection for last ~1 second; user can swipe again

---

### Interview Sound Bite

> "Cassandra can't do atomic read-then-write — its BATCH only supports writes, not SELECT. For match detection, I'd use Redis with Lua scripts for atomic operations, then async persist to Cassandra for durability. Alternatively, with DynamoDB or Postgres, we could use optimistic concurrency control with version checks and retry on conflict. Redis is simpler since it's natively atomic."

