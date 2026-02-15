# Google News Feed - System Design

## Overview

A scalable news feed system that ingests articles from external publishers, caches them for fast retrieval, and delivers personalized feeds to users with real-time "new articles" notifications.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  External Publishers    pollURLs every 5 min                                     │
│  (RSS Feeds)  ◄─────────────────────  Watcher System                            │
│       │                               (watch for change, if changed pull)        │
│       │                                      │                                   │
│       │ fetchContent                         │ enqueueURLs                       │
│       ▼                                      ▼                                   │
│  ┌─────────────┐                    ┌─────────────────┐                         │
│  │   Worker    │ ◄───────────────── │ Article URL     │                         │
│  │ Content Pull│                    │ Queue (SQS)     │                         │
│  │  and Parse  │                    └─────────────────┘                         │
│  └──────┬──────┘                                                                │
│         │                                                                        │
│         │ fetchMedia              ┌─────────────────┐      ┌─────────┐          │
│         ├────────────────────────►│  Media Puller   │─────►│   S3    │          │
│         │                         └─────────────────┘      │  (blob) │          │
│         │                                 ▲                └─────────┘          │
│         │                                 │                                      │
│         │                         ┌───────┴───────┐                             │
│         │                         │ Media URL     │                             │
│         │                         │ Queue (SQS)   │                             │
│         │                         └───────────────┘                             │
│         │                                                                        │
│         ▼                                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐        │
│  │                         DynamoDB                                     │        │
│  │  Article Table:                                                      │        │
│  │  - publisherId, articleId, articleURL, datePublishedAt              │        │
│  │  - checksum, title, content, thumbnail-url, region                  │        │
│  │  GSI: Region, articleId                                             │        │
│  │                                                                      │        │
│  │  Publisher Table:                                                    │        │
│  │  - publisherId, crawlFreq, publisherRSSFeedPath                     │        │
│  │  - publisher-articleId, attachmentId, s3bucket, imageName           │        │
│  └─────────────────────────────────────────────────────────────────────┘        │
│         │                                                                        │
│         │ DynamoDB Streams                                                       │
│         ▼                                                                        │
│  ┌─────────────────┐                                                            │
│  │  CDC Consumer   │  (Worker pool with KCL, not Lambda - avoids cold starts)   │
│  │  (KCL Workers)  │                                                            │
│  └────────┬────────┘                                                            │
│           │                                                                      │
│           ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐        │
│  │                           Redis                                      │        │
│  │  - Sorted sets per region: feed:region:US, feed:region:EU           │        │
│  │  - Category feeds: feed:technology:US, feed:sports:US               │        │
│  │  - Time-bucket counters for SSE deltas                              │        │
│  │  - Bloom filters for deduplication                                  │        │
│  └─────────────────────────────────────────────────────────────────────┘        │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Feed Serving Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              FEED SERVING                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   User ──► API Gateway ──► Feed Service ──► Redis (sorted sets by region)       │
│            (AuthN/AuthZ     /feed?page_token=X                                  │
│             Load Balance)                                                        │
│                                   │                                              │
│                                   │ Cache miss: query DynamoDB GSI              │
│                                   │ WHERE published_at > (now - 60m)            │
│                                   ▼                                              │
│                              DynamoDB                                            │
│                                                                                  │
│   CDN ◄─────────────────────── S3 (origin for media/thumbnails)                 │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## ID Generation: Snowflake vs Epoch+UUID

### The Debate

**Snowflake/ULID (deterministic tie-breaker):**
- `timestamp (ms) + machine_id + sequence_counter`
- Sequence counter is monotonically increasing within same millisecond
- Lexicographic sort = chronological sort = deterministic pagination ✓

**Epoch-ms + UUID (random tie-breaker):**
- `timestamp (ms) + random_uuid`
- Two events at same millisecond get random suffixes
- Lexicographic sort gives arbitrary order within that millisecond

### Practical Impact for News Feeds

For a news feed, the random UUID approach doesn't make things *worse*:

1. **Scrolling down (older):** If you "miss" something due to random ordering within the same millisecond, it's still there — just in a slightly different spot. Nobody notices if post A and post B (both from 12:00:00.123) swap positions.

2. **Pull to refresh (newer):** You fetch everything newer than your last-seen ID. The "missed" item is now above your old cursor, so you'll see it. Dedupe handles any overlap.

**When deterministic ordering matters:**
- Strict audit logs / event sourcing
- Distributed consistency across consumers
- Idempotency keys for conflict resolution
- Analytics / time-series with sub-ms accuracy

**For social/news feeds:** Users pull-to-refresh, dedupe kicks in, and sub-millisecond ordering is irrelevant.

---

## Real-Time "New Articles" Notification (SSE)

### Problem
Users scrolling through their feed should see a "X new articles" badge without overwhelming clients during breaking news (hundreds of articles/minute).

### Solution: Server-Side Batching + Client-Side Aggregation

#### Server Side (Stateless)

```
Article Ingestion (100s of articles/min during breaking news)
                   │
                   ▼
Counter Service (per segment - global, region, or topic)
  - Increment count, don't push yet
  - Store: {segment_id: count, last_push_ts}
                   │
                   ▼
Batched Push Worker (runs every 30-60s)
  - For each segment with count > 0:
    - Broadcast SSE: {"delta": 47}
    - Reset counter
```

**Time-bucketed counters for catch-up:**
```
bucket:2026-01-19T10:00 → 23 articles
bucket:2026-01-19T10:01 → 18 articles
bucket:2026-01-19T10:02 → 31 articles
```

When client reconnects with cursor at 10:01:30, sum buckets from 10:02 onward = instant catch-up count, no DB scan.

#### Client Side (Aggregates Locally)

```javascript
let count = 0;
let debounceTimer = null;

function onSSE(delta) {
  count += delta;
  
  // Debounce: don't re-render on every SSE, wait for pause
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    updateBadgeUI(count);  // "47 new articles"
  }, 500);
}

function onRefreshTapped() {
  // Jitter to prevent thundering herd
  const jitter = Math.random() * 2000;
  setTimeout(() => {
    fetchNewFeed();
    count = 0;
  }, jitter);
}
```

#### Segment Definition

| Segment Type | Example | Use Case |
|--------------|---------|----------|
| Global | `segment:global` | Everyone sees same feed (simplest) |
| Region | `segment:region:us-west` | Regional news |
| Topic | `segment:topic:sports` | Topic subscriptions |

For a simple chronological feed, one global counter is sufficient.

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Server batches by time window (30-60s) | Caps SSE volume regardless of article velocity |
| Push count, not content | Payload is tiny: `{delta: N}` |
| Client debounces UI updates | Handles reconnect bursts, multi-segment subscriptions |
| Jitter on refresh tap, not on badge display | Badge feels responsive; server load is spread |
| Never force refresh | User controls when feed updates; no scroll interruption |
| Cap badge display at 99+ | UX simplicity; still wait for user tap |

### Defense in Depth

```
Server: "I'll try to send at most 1 SSE per 30s"
Client: "Cool, but if you mess up, I'll still only re-render once per 500ms"
```

Both sides protect UX independently.

---

## CDC Pipeline: DynamoDB Streams → Redis

### Why CDC over Dual-Write?

| Approach | Pros | Cons |
|----------|------|------|
| Worker writes to Dynamo + Redis | Simple | Two failure modes, tight coupling, latency added to ingestion |
| CDC from DynamoDB Streams | Decoupled, replayable, evolvable | ~100-500ms latency (acceptable for news) |

### Worker Pool vs Lambda

| Aspect | Lambda | Worker Pool (ECS/EKS) |
|--------|--------|----------------------|
| Cold start | 100-500ms | None (always warm) |
| Cost at high throughput | Expensive | Predictable, cheaper |
| Scaling | Automatic | Manual (but predictable load) |
| Use case | Spiky, low-volume | Steady, high-volume (news feed) |

**Recommendation:** Worker pool with KCL for steady news ingestion.

### Scaling Signal: IteratorAge

```
CloudWatch Alarm:
  Metric: IteratorAge > 10000ms (10 sec behind)
  Action: Scale up ECS worker count

CloudWatch Alarm:
  Metric: IteratorAge < 1000ms for 10 min
  Action: Scale down
```

### Shard-to-Worker Assignment with KCL

DynamoDB Streams shards map 1:1 to table partitions. KCL handles coordination:

```
Pod 1 (KCL) ← leases shard 1, 2
Pod 2 (KCL) ← leases shard 3, 4
Pod 3 (KCL) ← leases shard 5, 6

# Pod 4 joins → KCL rebalances automatically
```

KCL uses a separate DynamoDB table for lease coordination — no manual shard assignment needed.

---

## Personalized Feeds

### Approach: Preference Vectors + Category Cache Assembly

Instead of caching 100M personalized feeds, store lightweight preference vectors and assemble feeds on-demand.

**User preference vector (~500 bytes):**
```json
{
  "user_id": "u123",
  "preferences": {
    "technology": 0.60,
    "business": 0.30,
    "sports": 0.05,
    "entertainment": 0.03,
    "politics": 0.02
  }
}
```

**Feed assembly:**
```
User u123 requests feed
    │
    ▼
Lookup preference vector: {tech: 0.6, business: 0.3, ...}
    │
    ▼
Fetch from pre-computed category caches:
  - feed:technology:US → top 20 articles
  - feed:business:US → top 20 articles
    │
    ▼
Mix based on weights:
  - 12 tech articles (60%)
  - 6 business articles (30%)
  - 2 other articles (10%)
    │
    ▼
Return personalized feed of 20 articles
```

### Storage Comparison

| Approach | Storage for 100M users |
|----------|------------------------|
| Cache full feed per user | 100M × 50KB = 5TB |
| Cache preference vector per user | 100M × 500B = 50GB |

**100x reduction** — store *what they like*, not *what to show them*.

### Building Preference Vectors

| Signal | Vector Update |
|--------|---------------|
| User clicks tech article | Increase `technology` weight |
| User skips sports article | Decrease `sports` weight |
| User follows "Business" | Boost `business` weight |
| Time decay | Recent behavior weighted more |

### Trade-offs

- Reduced personalization depth vs full recommendation engines
- Assembly algorithms need tuning for personalization vs diversity balance
- Very narrow interests might miss important global stories

---

## Data Model (DynamoDB)

### Article Table

| Attribute | Type | Description |
|-----------|------|-------------|
| publisherId | String | Partition key |
| articleId | String | Sort key (Snowflake ID) |
| articleURL | String | Source URL |
| datePublishedAt | Number | Epoch timestamp |
| checksum | String | Content hash for dedup |
| title | String | Article title |
| content | String | Article body |
| thumbnailUrl | String | S3 path |
| region | String | Geographic region |

**GSI:** `region-publishedAt-index` for regional feed queries

### Publisher Table

| Attribute | Type | Description |
|-----------|------|-------------|
| publisherId | String | Partition key |
| crawlFreq | Number | Poll interval (minutes) |
| rssFeedPath | String | RSS feed URL |

---

## Deduplication

**Bloom filter per user** stored in Redis:
- Check before showing article: `BF.EXISTS user:u123:seen articleId`
- Add after showing: `BF.ADD user:u123:seen articleId`

Handles the case where same article appears in multiple category feeds or after refresh.


---

## Priority-Based Polling Architecture

### Problem

Different publishers have different update frequencies and importance levels. Polling all publishers at the same rate is wasteful — high-profile publishers (NYT, BBC) update frequently and need fast ingestion, while smaller blogs update rarely.

### Solution: Tiered Polling with Separation of Concerns

Separate **discovery** (polling for new URLs) from **processing** (fetching and parsing content).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         POLLING LAYER (Discovery)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐        │
│  │ High-Pri Poller  │   │ Mid-Pri Poller   │   │ Low-Pri Poller   │        │
│  │ (every 1 min)    │   │ (every 5 min)    │   │ (every 30 min)   │        │
│  └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘        │
│           │                      │                      │                   │
│           │  Query Publisher table:                                         │
│           │  WHERE lastScraped < (now - interval) AND priority = X          │
│           │                      │                      │                   │
│           ▼                      ▼                      ▼                   │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                     Article URL Queue (SQS)                       │      │
│  │   {publisherId, rssFeedUrl, articleUrls[], metadata}             │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER (Extraction)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                   Content Worker Pool                             │      │
│  │   - Pull from Article URL Queue                                   │      │
│  │   - Fetch full article content from URL                           │      │
│  │   - Parse HTML, extract text, title, metadata                     │      │
│  │   - Identify media URLs (images, videos)                          │      │
│  │   - Write to DynamoDB                                             │      │
│  │   - Enqueue media URLs to Media Queue                             │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                    │                                        │
│                                    ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                     Media URL Queue (SQS)                         │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                    │                                        │
│                                    ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                   Media Worker Pool                               │      │
│  │   - Download images/videos                                        │      │
│  │   - Generate thumbnails (multiple sizes: 150x100, 300x200, etc)  │      │
│  │   - Upload to S3                                                  │      │
│  │   - Update article record with S3 paths                           │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Publisher Priority Tiers

| Priority | Poll Interval | Examples | Rationale |
|----------|---------------|----------|-----------|
| High | 1 min | NYT, BBC, Reuters, CNN | Breaking news, high traffic |
| Medium | 5 min | Regional papers, tech blogs | Regular updates |
| Low | 30 min | Niche blogs, infrequent publishers | Rarely update |

### Polling Worker Logic

```python
# High-priority poller (runs every 1 min)
def poll_high_priority():
    publishers = db.query(
        "SELECT * FROM publishers WHERE priority = 'high' AND lastScraped < now() - 1min"
    )
    for pub in publishers:
        rss_items = fetch_rss(pub.rssFeedUrl)
        new_urls = filter_already_seen(rss_items)  # checksum/URL dedup
        if new_urls:
            sqs.send({
                publisherId: pub.id,
                urls: new_urls,
                metadata: extract_rss_metadata(rss_items)
            })
        db.update(pub.id, lastScraped=now())
```

### Content Worker Logic

```python
# Doesn't care about priority — just processes whatever is in queue
def process_article(msg):
    for url in msg.urls:
        content = fetch_and_parse(url)
        article_id = generate_snowflake_id()
        db.put(article_id, content, publisherId=msg.publisherId)
        
        for media_url in content.media_urls:
            media_queue.send({articleId: article_id, url: media_url})
```

### Why Separate Polling from Processing?

| Aspect | Combined (3 separate pipelines) | Separated (pollers + shared queue) |
|--------|--------------------------------|-----------------------------------|
| Code duplication | Processing logic in each tier | Single content worker pool |
| Scaling | Each tier scales independently | Workers scale based on queue depth |
| Load balancing | Manual per-tier | Queue naturally distributes work |
| Maintenance | 3x code paths | DRY — polling logic separate from processing |
| Resource utilization | May have idle workers in low-pri tier | Shared pool, better utilization |

---

## RSS Feed vs Web Scraping

### What RSS Provides (Lightweight)

RSS feeds are standardized XML — parsing is simple and consistent across publishers.

```xml
<item>
  <title>Breaking: Major Event Happens</title>
  <link>https://publisher.com/articles/12345</link>
  <pubDate>Mon, 20 Jan 2026 10:30:00 GMT</pubDate>
  <description>Short summary of the article...</description>
  <media:thumbnail url="https://publisher.com/img/thumb.jpg" />
</item>
```

**Data from RSS (cheap to extract):**
- Title
- Article URL (link)
- Publish date
- Summary/description
- Sometimes thumbnail URL

### What Requires Full Page Fetch

**Data requiring HTML fetch (expensive):**
- Full article content (body text)
- High-resolution images
- Additional metadata (author, tags, reading time)
- Related articles

### Two-Phase Ingestion Flow

```
Phase 1: RSS Polling (Lightweight)
─────────────────────────────────
Poller
  │
  ├─► Fetch RSS feed (~10-50KB XML)
  │   - Standard XML parser
  │   - Extract: title, link, pubDate, summary
  │
  ├─► Dedup check: have we seen this URL/checksum?
  │
  └─► If new: enqueue {url, title, pubDate, summary}


Phase 2: Content Extraction (Heavy)
───────────────────────────────────
Content Worker
  │
  ├─► Fetch full article page (~100-500KB HTML)
  │   - Site-specific parsing (readability algorithms)
  │   - Extract full text, images, videos
  │
  └─► Write to DynamoDB, enqueue media URLs
```

### RSS vs Web Scraping Complexity

| Aspect | RSS | Web Scraping |
|--------|-----|--------------|
| Format | Standardized XML (Atom/RSS 2.0) | Arbitrary HTML per site |
| Parsing | Simple XML parser | Site-specific selectors, changes frequently |
| Rate limiting | Usually generous | Often aggressive anti-bot measures |
| Content completeness | Metadata + summary only | Full article body |
| Maintenance | Rarely breaks | Selectors break when sites redesign |

### Full Content in RSS (Rare but Ideal)

Some publishers include full content in RSS via `<content:encoded>`:

```xml
<item>
  <title>Article Title</title>
  <content:encoded><![CDATA[
    <p>Full article HTML content here...</p>
    <img src="https://..." />
  ]]></content:encoded>
</item>
```

When available, this eliminates the need for Phase 2 fetch — but most publishers only provide summaries to drive traffic to their sites.

### Publisher Table Schema (Updated)

| Attribute | Type | Description |
|-----------|------|-------------|
| publisherId | String | Partition key |
| name | String | Publisher name |
| rssFeedUrl | String | RSS feed URL |
| scrapeType | String | `RSS` \| `Web` \| `Webhook` |
| priority | String | `high` \| `medium` \| `low` |
| lastScraped | Number | Epoch timestamp of last poll |
| crawlFreq | Number | Poll interval (minutes) |
| contentInRss | Boolean | Whether RSS includes full content |

---

## Multi-Region CDC with Global Tables

### Architecture

Each region has a DynamoDB Global Table replica and processes only its relevant articles:

```
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│ DynamoDB US     │          │ DynamoDB EU     │          │ DynamoDB APAC   │
│ (replica)       │          │ (replica)       │          │ (replica)       │
└────────┬────────┘          └────────┬────────┘          └────────┬────────┘
         │                            │                            │
         ▼                            ▼                            ▼
   DynamoDB Stream            DynamoDB Stream            DynamoDB Stream
   (all articles)             (all articles)             (all articles)
         │                            │                            │
         ▼                            ▼                            ▼
   CDC Consumer US             CDC Consumer EU             CDC Consumer APAC
   if region=="US"             if region=="EU"             if region=="APAC"
   → process                   → process                   → process
   else → skip                 else → skip                 else → skip
         │                            │                            │
         ▼                            ▼                            ▼
    Redis US                     Redis EU                    Redis APAC
```

### How It Works

1. Global Tables replicate all articles to all regions automatically
2. Each region's DynamoDB Stream contains ALL articles (full replication)
3. CDC consumer in each region filters: `if article.region != my_region: skip`
4. Only matching articles are written to local Redis

### Trade-offs

| Aspect | Impact |
|--------|--------|
| Stream read cost | Each region reads 100% of stream, processes ~33% (if 3 regions) |
| Simplicity | High — no complex routing, each region is self-contained |
| Latency | Low — CDC consumer co-located with its Redis |
| Fault isolation | Region failure doesn't affect others |

The "waste" of reading full stream is acceptable because:
- DynamoDB Streams reads are cheap
- Filtering is O(1) — just check region field
- Full regional isolation and fault tolerance

---

## Redis Sorted Sets for Feed Storage

### Using Snowflake ID as Score

```
ZADD feed:region:US <snowflake_id> <article_id>

# Example
ZADD feed:region:US 1737388800000 "article:abc123"
ZADD feed:region:US 1737388800500 "article:def456"
ZADD feed:region:US 1737388801000 "article:ghi789"
```

### Fetching Latest Articles

```
# Get newest 20 articles
ZREVRANGE feed:region:US 0 19
# Returns: ["article:ghi789", "article:def456", "article:abc123"]
```

### Cursor-Based Pagination

```
# Page 1: get top 20
ZREVRANGE feed:region:US 0 19 WITHSCORES
# Last item score: 1737388800000

# Page 2: get next 20 older than cursor
ZREVRANGEBYSCORE feed:region:US (1737388800000 -inf LIMIT 0 20
```

### Relevance-Based Ordering (Personalized Feeds)

For personalized feeds, use relevance score instead of timestamp:

```
ZADD feed:user:12345 0.95 "article:abc"  # highly relevant
ZADD feed:user:12345 0.72 "article:def"  # somewhat relevant
ZADD feed:user:12345 0.45 "article:ghi"  # less relevant
```

### Hybrid: Time-Decayed Relevance

```
score = (epoch_timestamp * 0.001) + relevance_score
# or
score = relevance_score * time_decay_factor(age)
```

Newer + relevant articles bubble up naturally.


---

## Webhook Integration

### Overview

Webhooks provide real-time push notifications from publishers, eliminating polling latency. They replace the polling step but feed into the same processing pipeline.

### Webhook Payload Patterns

**Pattern 1: URL-only (Most Common)**

```json
{
  "event": "article.published",
  "article_url": "https://publisher.com/articles/12345",
  "published_at": "2026-01-20T10:30:00Z"
}
```

Flow:
```
Webhook endpoint → validate signature → enqueue URL to Article Queue → content worker fetches
```

**Pattern 2: Full Content (Rare but Ideal)**

```json
{
  "event": "article.published",
  "article": {
    "title": "Breaking News",
    "content": "Full article body...",
    "author": "Jane Doe",
    "images": ["https://cdn.publisher.com/img1.jpg"],
    "published_at": "2026-01-20T10:30:00Z"
  }
}
```

Flow:
```
Webhook endpoint → validate → write directly to DynamoDB → enqueue media URLs only
```

Skips content fetch entirely — faster ingestion.

### Webhook Service Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Webhook Service                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  POST /webhooks/articles                                         │
│    │                                                             │
│    ├─► Validate signature (HMAC, API key per publisher)         │
│    │                                                             │
│    ├─► Check payload type:                                       │
│    │     - URL only? → enqueue to Article URL Queue             │
│    │     - Full content? → write to DynamoDB, enqueue media     │
│    │                                                             │
│    ├─► Store idempotency key (prevent duplicate processing)     │
│    │                                                             │
│    └─► Return 200 OK immediately (async processing)             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Why Return 200 Immediately?

- Publishers expect fast webhook responses (<500ms)
- Synchronous processing causes timeouts → publisher retries → duplicates
- Enqueue and process async — idempotency key handles retries

### Comparison: Polling vs Webhook

| Aspect | Polling | Webhook (URL) | Webhook (Full) |
|--------|---------|---------------|----------------|
| Latency | Minutes (poll interval) | Seconds | Seconds |
| Content fetch | Required | Required | Not needed |
| Publisher effort | None | Must implement webhook | Must implement + full payload |
| Reliability | You control | Depends on publisher retry logic | Same |
| Cost | Your compute for polling | Publisher pushes to you | Same |

**Reality:** Most publishers send URL-only webhooks — full content requires more effort on their side, and they often want page visits for ad impressions and analytics.

---

## Capacity Estimation

### Article Size Breakdown

| Field | Typical Size |
|-------|--------------|
| articleId | 20 bytes |
| publisherId | 20 bytes |
| title | 100-200 bytes |
| summary/description | 500-1000 bytes |
| full content (text only) | 5-15 KB |
| articleURL | 100 bytes |
| thumbnailUrl (S3 path) | 100 bytes |
| author | 50 bytes |
| tags/categories | 200 bytes |
| timestamps, region, etc. | 100 bytes |
| **Total** | **~10-15 KB** |

**Safe estimate: 10 KB per article** (text + metadata, no media)

### Media Size Breakdown

| Asset | Size |
|-------|------|
| Thumbnail (150x100) | 10-20 KB |
| Medium image (300x200) | 30-50 KB |
| Large image (600x400) | 80-150 KB |
| Hero image (1200x800) | 200-400 KB |
| **Total per article (3-5 images)** | **300 KB - 1 MB** |

**Safe estimate: 500 KB media per article**

### Storage Estimation (1M articles/day)

**DynamoDB:**
```
Daily:   1M articles × 10 KB = 10 GB/day
Monthly: 10 GB × 30 days = 300 GB
Yearly:  ~3.6 TB (with TTL, likely keep 30-90 days)
```

**S3 (media):**
```
Daily:   1M articles × 500 KB = 500 GB/day
Monthly: 500 GB × 30 days = 15 TB
Yearly:  ~180 TB (consider lifecycle policies for old content)
```

**Redis (feed cache):**
```
Sorted set entry: ~50 bytes (score + articleId reference)
Top 1000 articles × 100 regions/categories = 100K entries
100K × 50 bytes = 5 MB (negligible)

With user preference vectors (100M users):
100M × 500 bytes = 50 GB
```

### Throughput Estimation

**Ingestion:**
```
1M articles/day = ~12 articles/second average
Peak (breaking news): 10-100x = 120-1200 articles/second
```

**Feed reads:**
```
100M DAU, 10 feed requests/user/day = 1B requests/day
= ~12,000 requests/second average
Peak: 50,000+ requests/second
```

### Infrastructure Sizing

| Component | Baseline | Peak Capacity |
|-----------|----------|---------------|
| Polling workers | 3-5 instances | Auto-scale on queue depth |
| Content workers | 5-10 instances | Auto-scale on queue depth |
| Media workers | 10-20 instances | CPU-heavy, scale on queue |
| CDC workers (KCL) | Match DynamoDB shard count | Scale with IteratorAge |
| Feed service | 20-50 instances | Scale on request rate |
| Redis cluster | 3-node cluster, 100GB | Scale on memory/connections |

---

## Personalized Feed: Fanout Strategies

### Problem

When a new article arrives, how do we identify which users should see it in their personalized feed?

### Option 1: Inverted Index by Topic (Simple)

Don't search users — index them by interest.

```
topic:technology → [user1, user2, user5, user99, ...]
topic:sports → [user3, user4, user5, ...]
publisher:nytimes → [user1, user7, ...]
```

When article arrives tagged `[technology, AI]`:
```
1. Lookup topic:technology → get 2M user IDs
2. Lookup topic:AI → get 500K user IDs
3. Union/intersect based on scoring logic
4. Fan out to those users' feeds
```

**Pros:** Simple, fast lookups
**Cons:** Coarse-grained personalization

### Option 2: Vector Similarity Search

Embed both articles and user profiles in the same vector space.

```
Article embedding:  [0.8, 0.2, 0.1, 0.6, ...]  (from title + content via BERT/etc)
User profile embedding: [0.7, 0.3, 0.1, 0.5, ...]  (aggregated from past clicks)
```

When article arrives:
```
1. Generate article embedding
2. Query vector DB: "find users with profile similarity > 0.8"
3. Fan out to matched users
```

**Tools:** Pinecone, Weaviate, Milvus, pgvector, Redis VSS

**Cons:** Searching 50M user vectors per article is expensive (~10-50ms per query × thousands of articles/hour)

### Option 3: Hybrid — Coarse Filter + Fine Rank (Recommended)

```
Article arrives: tagged [technology, US, breaking]
        │
        ▼
Coarse filter (inverted index):
  - topic:technology ∩ region:US → 1M candidate users
        │
        ▼
Fine rank (vector similarity or ML model):
  - For each candidate, compute relevance score
  - Only push to users with score > threshold
        │
        ▼
Fan out to ~200K highly relevant users
```

Avoids searching all 50M users — narrow down first, then score.

### Option 4: Pull-Based Assembly (What We Use)

Don't push to users at all:
- Push article to *category feeds* (cheap, bounded)
- When user requests feed, *pull* from relevant categories and rank in real-time

This is the preference vector + category assembly approach — avoids the fanout problem entirely.

### Comparison

| Approach | Fanout Cost | Personalization Depth |
|----------|-------------|----------------------|
| Inverted index by topic | O(users in topic) | Coarse |
| Vector search all users | O(all users) — expensive | Deep |
| Coarse filter + fine rank | O(filtered subset) | Balanced |
| Pull-based assembly | O(1) per user request | Moderate |

**Recommendation:** For 50M+ users, use pull-based assembly with preference vectors. Pure push-based fanout doesn't scale.
