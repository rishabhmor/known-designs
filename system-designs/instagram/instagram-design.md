# Instagram System Design

## Core Entities

### Why Entities Matter
Something becomes an entity when:
- It has **independent identity** (unique ID, own lifecycle)
- It has **its own attributes** that belong together
- It **participates in relationships** (especially many-to-many)
- It needs to be **queried independently**
- It has a **lifecycle** (created, updated, deleted independently)

### Key Entities

```
User
├── user_id (PK)
├── username
├── email
├── profile_picture_url
├── bio
├── follower_count (denormalized)
├── following_count (denormalized)
└── created_at

Post
├── post_id (PK)
├── user_id (FK → User)
├── media_url
├── caption
├── like_count (denormalized)
├── comment_count (denormalized)
└── created_at

Follows (Many-to-Many Relationship)
├── follower_id (FK → User)
├── followee_id (FK → User)
├── created_at
└── (composite PK: follower_id + followee_id)

Like
├── user_id (FK → User)
├── post_id (FK → Post)
├── created_at
└── (composite PK: user_id + post_id)

Comment
├── comment_id (PK)
├── post_id (FK → User)
├── user_id (FK → User)
├── content
└── created_at
```

### Why Follows is a Separate Entity
- Models the **social graph** (many-to-many relationship)
- Enables queries: "Who do I follow?", "Who follows me?", "Does A follow B?"
- Essential for feed generation
- At scale: billions of rows, heavily cached (Redis sets), sharded by user_id

---

## Feed System & Pagination

### The Problem with Offset Pagination
```sql
Page 1: SELECT * FROM feed ORDER BY created_at DESC LIMIT 10 OFFSET 0
Page 2: SELECT * FROM feed ORDER BY created_at DESC LIMIT 10 OFFSET 10
```
If a new post is inserted before fetching page 2, everything shifts — duplicates or missed posts.

### Cursor-Based Pagination (Solution)

Instead of "give me items 10-20", say "give me 10 items older than this timestamp/ID":

```sql
Page 1: SELECT * FROM feed WHERE user_id IN (following) 
        ORDER BY created_at DESC LIMIT 10

→ Returns posts, last one has created_at = '2026-01-16 10:30:00', id = 'abc123'
→ Next page token = encode('2026-01-16 10:30:00', 'abc123')

Page 2: SELECT * FROM feed WHERE user_id IN (following)
        AND (created_at, id) < ('2026-01-16 10:30:00', 'abc123')
        ORDER BY created_at DESC LIMIT 10
```

### Token Structure
```json
{
  "timestamp": "2026-01-16T10:30:00Z",
  "id": "abc123"  // tie-breaker for same-timestamp posts
}
```
Base64 encoded, client treats it as opaque.

### Why Include ID as Tie-Breaker?
Multiple posts can have the same timestamp. Using `(timestamp, id)` as composite cursor ensures deterministic ordering.

---

## Handling New Posts While Scrolling

### Approaches

| Approach | Pros | Cons |
|----------|------|------|
| Reset on pull-to-refresh | Simple, predictable | Lose scroll position |
| Inject mid-scroll | Fresh content without losing place | Jarring, complex state |
| "X new posts" banner | User controls when to see new | Extra tap required |

### Pull-to-Refresh
User explicitly wants fresh content → reset cursor, fetch from top:
```
Client discards current cursor → GET /feed (no token) → fresh page 1
```

### Sprinkling New Posts Mid-Scroll
For algorithmic feeds where strict chronology doesn't matter:
- Client maintains "pending new posts" buffer
- Server interleaves highly-ranked new posts with older content
- Requires two cursors: `older_than` and `newer_than`

---

## Precomputed Algorithmic Feeds

### Feed Storage Structure
```
User's Feed (Redis/Cassandra):
┌─────────────────────────────────────┐
│ Position 0: Post X (score: 0.95)    │  ← newest/highest ranked
│ Position 1: Post Y (score: 0.91)    │
│ Position 2: Post Z (score: 0.87)    │
│ ...                                 │
└─────────────────────────────────────┘
```

Pagination is simple position-based:
```
GET /feed?cursor=position_5 → return positions 5-14
```

### Fan-out on Write
```
New post P arrives from someone you follow
→ Feed service scores it (relevance, recency, engagement prediction)
→ Inserts into your precomputed feed at the right position
→ Feed list updated in storage
```
No merge at read time — already baked in.

---

## Hybrid: Real-time Buffer + Precomputed Feed

### Architecture
```
Per User:
┌──────────────────────────────────────┐
│ Real-time Buffer (Redis sorted set)  │  ← last N minutes, ~50-100 posts max
│ Key: rt_feed:{user_id}               │
│ Score: timestamp or relevance score  │
└──────────────────────────────────────┘
                 +
┌──────────────────────────────────────┐
│ Precomputed Feed (Cassandra/Redis)   │  ← bulk feed, refreshed periodically
│ Key: feed:{user_id}                  │
└──────────────────────────────────────┘
```

### Read Logic

**Page 1 (Top of Feed):**
```python
def get_feed_page_1(user_id):
    realtime = redis.zrevrange(f"rt_feed:{user_id}", 0, 50)
    precomputed = get_precomputed(user_id, limit=50)
    
    merged = merge_by_score(realtime, precomputed)
    return merged[:20], cursor=merged[19].id
```

**Deep Scrolling (Page 5+):**
```python
def get_feed_page_n(user_id, cursor):
    # Skip buffer, just read precomputed
    return get_precomputed(user_id, cursor=cursor)
```

### Why This Works
- Fresh posts appear immediately (real-time buffer)
- No expensive re-ranking of entire feed on every read
- Deep scrolling stays fast (precomputed only)
- Buffer is small, fits in memory, cheap to merge

---

## Buffer Migration (Real-time → Precomputed)

### Why NOT Redis TTL Expiry Events
1. **At-most-once delivery** — miss events if consumer is down
2. **No persistence** — can't replay
3. **Lazy expiration** — timing is unpredictable
4. **Scale issues** — flood of expiry events

### Better Approaches

**A. Scheduled Job (Most Common)**
```
Every 5-15 minutes:
  1. ZRANGEBYSCORE rt_feed:{user_id} with timestamp < now - 30min
  2. Batch merge into precomputed feed
  3. ZREMRANGEBYSCORE to clean up buffer
```

**B. On-Read Lazy Migration**
```python
def get_feed(user_id):
    realtime = get_realtime_buffer(user_id)
    
    stale = [p for p in realtime if p.timestamp < threshold]
    if stale:
        async_queue.enqueue("migrate_to_precomputed", user_id, stale)
    
    # Continue serving...
```

**C. Stream-Based (Kafka/SQS)**
```
Post created → Kafka topic → Consumer writes to real-time buffer
                          → Schedules delayed message for migration
```
Delayed message triggers move to precomputed after N minutes.

### TL;DR
TTL is fine for cleanup, but not for triggering business logic. Use:
- Cron/scheduled jobs (simple)
- Lazy migration on read (opportunistic)  
- Message queues with delayed delivery (robust)

---

## Media Upload & Storage

### Signed URLs for Direct Upload

Client uploads media directly to blob storage (S3/GCS), bypassing your servers. This saves bandwidth and reduces latency.

**Why client determines file size/type locally:**
- Browser/app has access to file metadata via File API
- `file.size` gives bytes, `file.type` gives MIME type
- No upload needed to determine this

**Why file type matters:**
1. Storage optimization — different buckets/paths for images vs videos
2. Processing pipelines — images → thumbnail generation, videos → transcoding
3. Validation & security — reject disallowed types, signed URL can enforce Content-Type
4. Upload strategy — small files use single PUT, large files use multipart
5. Cost management — different quotas/rate limits per type

### Schema Design (Denormalized for DynamoDB)

```
# Posts Table
PK: user#user_123
SK: post#1705420800#post_abc
data: {
  caption: "My vacation!",
  attachments: [
    { attachment_id: "att_1", object_key: "uploads/user_123/att_1.jpg", 
      type: "image", width: 1080, height: 1080 },
    { attachment_id: "att_2", object_key: "uploads/user_123/att_2.mp4", 
      type: "video", duration: 30 }
  ]
}

# Attachments Table (for upload lifecycle tracking)
PK: att_1
data: {
  object_key: "uploads/user_123/att_1.jpg",
  upload_id: null,           # S3 multipart upload ID (cleared after completion)
  status: "pending" | "uploading" | "ready" | "failed",
  post_id: "post_abc",       # backlink after post creation
  user_id: "user_123",
  type: "image",
  file_size: 2097152,
  created_at: 1705420800
}
```

**Why both tables have object_key:**
- Post record: fast reads (single query gets everything to render)
- Attachment record: upload lifecycle, transcoding status, cleanup jobs

**Two IDs to track:**
- `attachment_id` — your domain ID, stable forever, referenced in posts
- `upload_id` — S3's multipart session ID, only relevant during upload

### CDN Integration

**Don't store full CDN URLs** — construct them dynamically:
```
# Stored in DB
object_key: "uploads/user_123/img1.jpg"

# Constructed at read time
cdn_base + "/" + object_key
→ "https://cdn.instagram.com/uploads/user_123/img1.jpg"
```

**Why:**
- CDN provider might change
- Multiple CDN domains for geo-routing
- URL signing happens at request time

### CDN Authorization

**Signed URLs (CloudFront):**
```
Server has: Private Key
CDN has: Public Key (Key-Pair-Id)

Server signs URL → CDN verifies signature
```

```
https://cdn.instagram.com/uploads/user_123/img1.jpg
  ?Expires=1705424400
  &Signature=BASE64_ENCODED_SIG
  &Key-Pair-Id=KAPK1234567890
```

**Signed Cookies (alternative):**
- One auth cookie covers all media for that session
- Better for feeds with many images
- Browser sends cookie automatically

### CDN Cache Miss

Automatic fallback — CDN configured with S3 as origin:
```
Client → CDN: GET /uploads/user_123/img1.jpg

CDN checks cache:
  Cache HIT  → Return cached content
  Cache MISS → Fetch from S3 origin, cache it, return to client
```

---

## Post Creation Flow (Complete)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           POST CREATION FLOW (10 attachments)                   │
└─────────────────────────────────────────────────────────────────────────────────┘

PHASE 1: REQUEST UPLOAD URLs
─────────────────────────────
Client                              Server                              S3
  │                                   │                                  │
  │ POST /upload/init                 │                                  │
  │ { files: [                        │                                  │
  │   {type:"image", size:2MB},       │                                  │
  │   {type:"video", size:500MB},     │                                  │
  │   ... (10 files)                  │                                  │
  │ ]}                                │                                  │
  │──────────────────────────────────>│                                  │
  │                                   │                                  │
  │                                   │  For each file:                  │
  │                                   │  ─────────────────────────────── │
  │                                   │  Generate attachment_id (att_1)  │
  │                                   │  Generate object_key             │
  │                                   │                                  │
  │                                   │  If image (small):               │
  │                                   │    Generate single presigned PUT │
  │                                   │                                  │
  │                                   │  If video (large):               │
  │                                   │    InitiateMultipartUpload ─────>│
  │                                   │<───────────── upload_id ─────────│
  │                                   │    Generate presigned URL per part│
  │                                   │                                  │
  │                                   │  Save to DynamoDB:               │
  │                                   │  attachments table with          │
  │                                   │  status: "pending"               │
  │                                   │                                  │
  │<──────────────────────────────────│                                  │
  │ {                                 │                                  │
  │   attachments: [                  │                                  │
  │     { attachment_id: "att_1",     │                                  │
  │       upload_url: "https://s3...",│                                  │
  │       type: "image" },            │                                  │
  │     { attachment_id: "att_2",     │                                  │
  │       upload_id: "xyz",           │                                  │
  │       part_urls: ["url1","url2"...],                                 │
  │       type: "video" },            │                                  │
  │   ]                               │                                  │
  │ }                                 │                                  │


PHASE 2: UPLOAD FILES
─────────────────────
Client                              Server                              S3
  │                                   │                                  │
  │  For each image:                  │                                  │
  │  PUT upload_url ─────────────────────────────────────────────────────>│
  │<─────────────────────────────────────────────────────────── 200 + ETag│
  │                                   │                                  │
  │  For each video (parallel parts): │                                  │
  │  PUT part_url[1] ────────────────────────────────────────────────────>│
  │<─────────────────────────────────────────────────────────── 200 + ETag│
  │  PUT part_url[2] ────────────────────────────────────────────────────>│
  │<─────────────────────────────────────────────────────────── 200 + ETag│
  │  ... (all parts)                  │                                  │
  │                                   │                                  │
  │  Client stores locally:           │                                  │
  │  { att_2: [{partNum:1, etag:"abc"}, {partNum:2, etag:"def"},...]}    │


PHASE 3: COMPLETE UPLOADS
─────────────────────────
Client                              Server                              S3
  │                                   │                                  │
  │ POST /upload/complete             │                                  │
  │ { attachments: [                  │                                  │
  │   { attachment_id: "att_1" },     │                                  │
  │   { attachment_id: "att_2",       │                                  │
  │     parts: [{partNum:1,etag:"abc"},│                                 │
  │             {partNum:2,etag:"def"}]│                                 │
  │   },                              │                                  │
  │ ]}                                │                                  │
  │──────────────────────────────────>│                                  │
  │                                   │                                  │
  │                                   │  For each video:                 │
  │                                   │  CompleteMultipartUpload ───────>│
  │                                   │<──────────────────────── 200 ────│
  │                                   │                                  │
  │                                   │  Update DynamoDB:                │
  │                                   │  attachments.status = "ready"    │
  │                                   │                                  │
  │<──────────────────────────────────│                                  │
  │ { success: true, ready: ["att_1","att_2",...] }                      │


PHASE 4: CREATE POST
────────────────────
Client                              Server                              DynamoDB
  │                                   │                                  │
  │ POST /posts                       │                                  │
  │ { caption: "My vacation!",        │                                  │
  │   attachment_ids: ["att_1",...,"att_10"] }                           │
  │──────────────────────────────────>│                                  │
  │                                   │                                  │
  │                                   │  1. Validate attachments         │
  │                                   │     (belong to user, status=ready)│
  │                                   │                                  │
  │                                   │  2. Fetch attachment details     │
  │                                   │     BatchGetItem(att_1...att_10) │
  │                                   │────────────────────────────────> │
  │                                   │<─────────── attachment records ──│
  │                                   │                                  │
  │                                   │  3. TransactWriteItems:          │
  │                                   │     - PUT post with embedded     │
  │                                   │       attachment data            │
  │                                   │     - UPDATE each attachment     │
  │                                   │       SET post_id = "post_abc"   │
  │                                   │────────────────────────────────> │
  │                                   │<──────────────────────── 200 ────│
  │                                   │                                  │
  │                                   │  4. Invalidate/update cache      │
  │                                   │  5. Trigger feed fanout (async)  │
  │                                   │                                  │
  │<──────────────────────────────────│                                  │
  │ { post_id: "post_abc", created_at: 1705420800 }                      │
```

### Upload Completion Detection

**Options:**
1. **Client callback** — Client calls `POST /upload/complete` after successful upload
2. **S3 event notification** — S3 triggers Lambda/SQS on `ObjectCreated`, updates DB
3. **Lazy verification** — Check if object exists when first accessed

For multipart uploads, `CompleteMultipartUpload` must be called to assemble parts — S3 `ObjectCreated` event only fires after this.

---

## Cache Invalidation

### Approaches

**1. Application-level (write-through):**
```python
def update_post(post_id, data):
    db.update(posts, data)
    cache.delete(f"post:{post_id}")  # or rebuild and set
```
Simple, immediate. Risk: drift if app crashes between operations.

**2. CDC (Change Data Capture):**
```
DB → Debezium/DynamoDB Streams → Kafka → Cache Invalidation Service
```
Decoupled, reliable. Adds latency (seconds). More infrastructure.

**3. Hybrid (production):**
- Application does immediate invalidation for user-facing writes
- CDC as safety net for edge cases, batch jobs, admin changes

### Production Architecture
```
Write: PostgreSQL/DynamoDB (normalized)
       ↓ CDC
Read:  Cassandra/Redis (denormalized, optimized for feed queries)
```

---

## DynamoDB Time-Based Queries

Using `epoch_UUID` as Sort Key enables range queries:

```
PK: user#user_123
SK: post#1705420800#abc123

# Get posts after timestamp:
Query: PK = "user#user_123" AND SK > "post#1705420800"

# Get latest posts (descending):
Query: PK = "user#user_123" AND begins_with(SK, "post#")
       ScanIndexForward = false
       Limit = 10
```

**Note:** This only works for Sort Key, not Partition Key (PKs are hashed).
