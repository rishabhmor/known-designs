# Multipart Upload Design (S3/Blob Storage)

## Overview

Multipart upload allows large files to be uploaded in chunks, enabling parallel uploads, resumability, and better handling of network failures.

---

## When to Use Multipart vs Single Upload

| File Type | Size | Strategy |
|-----------|------|----------|
| Images | < 10MB | Single presigned PUT URL |
| Videos | > 10MB | Multipart upload |
| Large files | > 100MB | Multipart (mandatory for reliability) |

**Typical thresholds:**
- < 5-10MB → single upload
- > 10MB or video type → multipart

---

## S3 Multipart Constraints

```
Min part size: 5MB (except last part)
Max part size: 5GB
Max parts: 10,000
Max object size: 5TB
```

**Part size strategy:**
```python
if file_size < 100MB: part_size = 10MB
elif file_size < 1GB: part_size = 50MB
else: part_size = 100MB

num_parts = ceil(file_size / part_size)
```

---

## How Multipart Upload Works

### The Three-Step Process

```
1. InitiateMultipartUpload(bucket, key)
   → S3 returns uploadId
   → S3 now has an "open" multipart upload session

2. UploadPart(bucket, key, uploadId, partNumber, body)
   → Upload each part (can be parallel, out of order)
   → Each part returns an ETag

3. CompleteMultipartUpload(bucket, key, uploadId, parts)
   → S3 assembles all parts into final object
   → Object becomes accessible
   → ObjectCreated event fires
```

**Critical:** Until `CompleteMultipartUpload` is called, the file doesn't exist as a usable object. Parts are fragments in S3 limbo.

### What S3 Tracks Per uploadId

- Bucket + key this upload is for
- All uploaded parts (partNumber, ETag, size)
- Created timestamp

---

## Two IDs to Track

| ID | Source | Purpose | Lifetime |
|----|--------|---------|----------|
| `attachment_id` | Your server | Domain entity, referenced in posts | Permanent |
| `upload_id` | S3 | Multipart session identifier | Only during upload |

**Your server maps both:**
```
attachments:
  id: "att_123"              ← your ID
  upload_id: "s3_xyz789"     ← S3's ID
  object_key: "uploads/user_456/att_123.mp4"
  status: "uploading"
```

After upload completes, `upload_id` can be cleared — it's no longer needed.

---

## Assembly Options

### Option A: Client-Side Assembly (Simpler)

```
1. Client uploads all parts, collects ETags locally
2. Client calls: POST /upload/complete { attachment_id, parts: [...] }
3. Server calls S3 CompleteMultipartUpload
4. Server updates DB status to "ready"
```

**Pros:** Simple, fewer HTTP calls during upload
**Cons:** If client crashes, must re-query S3 for part status

### Option B: Server Tracks Parts

```
1. Each part upload, client notifies: POST /upload/part-done { part_number, etag }
2. Server tracks progress in DB/Redis
3. When all parts received, server calls CompleteMultipartUpload
```

**Pros:** Server has visibility, can show progress, supports multi-device resume
**Cons:** More HTTP calls, more complexity

---

## Resume After Crash

### The Problem

Client crashes at part 45/50. How to resume?

### Client Must Persist Locally

```
// Mobile app local DB
pending_uploads:
  local_file_path: "/videos/cat.mp4"
  attachment_id: "att_123"
  upload_id: "s3_xyz"
  created_at: 1705420800
```

Without this local state, there's no way to correlate the in-progress upload to a local file.

### Resume Flow

**Option A: Ask S3 directly**
```
S3.ListParts(bucket, key, uploadId)
→ Returns all successfully uploaded parts with ETags
```

**Option B: Ask your server (if tracking parts)**
```
GET /upload/status/{attachmentId}
→ { uploadId, completedParts: [...], nextPart: 45 }
```

Then continue uploading remaining parts.

---

## S3 Event Limitations

**S3 does NOT emit events for individual part uploads.**

Only fires for:
- `s3:ObjectCreated:*` (after CompleteMultipartUpload)
- `s3:ObjectRemoved:*`
- etc.

**No `s3:PartUploaded` event exists.** Parts are internal/transient state.

**For tracking part completion, use:**
- Client reports each part to your server
- Or just track at completion time

---

## Orphaned Upload Cleanup

If client abandons upload (crashes, user cancels), parts sit in S3 indefinitely.

**Solution: S3 Lifecycle Rules**
```json
{
  "Rules": [{
    "ID": "AbortIncompleteMultipartUpload",
    "Status": "Enabled",
    "AbortIncompleteMultipartUpload": {
      "DaysAfterInitiation": 7
    }
  }]
}
```

S3 automatically cleans up incomplete multipart uploads after 7 days.

---

## Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           MULTIPART UPLOAD FLOW                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

PHASE 1: INITIATE
─────────────────
Client                              Server                              S3
  │                                   │                                  │
  │ POST /upload/init                 │                                  │
  │ { type: "video", size: 500MB }    │                                  │
  │──────────────────────────────────>│                                  │
  │                                   │                                  │
  │                                   │  Generate attachment_id          │
  │                                   │  Generate object_key             │
  │                                   │  Calculate parts (50 × 10MB)     │
  │                                   │                                  │
  │                                   │  InitiateMultipartUpload ───────>│
  │                                   │<───────────── upload_id ─────────│
  │                                   │                                  │
  │                                   │  Generate presigned URL per part │
  │                                   │                                  │
  │                                   │  Save to DB:                     │
  │                                   │  { attachment_id, upload_id,     │
  │                                   │    object_key, status: "pending"}│
  │                                   │                                  │
  │<──────────────────────────────────│                                  │
  │ { attachment_id: "att_123",       │                                  │
  │   upload_id: "xyz",               │                                  │
  │   part_urls: [url1, url2, ...],   │                                  │
  │   part_size: 10485760 }           │                                  │


PHASE 2: UPLOAD PARTS (Parallel)
────────────────────────────────
Client                                                                  S3
  │                                                                      │
  │  Read file in chunks                                                 │
  │  For each part (can be parallel):                                    │
  │                                                                      │
  │  PUT part_url[1] + bytes[0:10MB] ───────────────────────────────────>│
  │<─────────────────────────────────────────────────────────── ETag: abc│
  │                                                                      │
  │  PUT part_url[2] + bytes[10MB:20MB] ────────────────────────────────>│
  │<─────────────────────────────────────────────────────────── ETag: def│
  │                                                                      │
  │  ... (all 50 parts)                                                  │
  │                                                                      │
  │  Client collects: [                                                  │
  │    { partNumber: 1, etag: "abc" },                                   │
  │    { partNumber: 2, etag: "def" },                                   │
  │    ...                                                               │
  │  ]                                                                   │


PHASE 3: COMPLETE & ASSEMBLE
────────────────────────────
Client                              Server                              S3
  │                                   │                                  │
  │ POST /upload/complete             │                                  │
  │ { attachment_id: "att_123",       │                                  │
  │   parts: [                        │                                  │
  │     {partNumber:1, etag:"abc"},   │                                  │
  │     {partNumber:2, etag:"def"},   │                                  │
  │     ...                           │                                  │
  │   ]                               │                                  │
  │ }                                 │                                  │
  │──────────────────────────────────>│                                  │
  │                                   │                                  │
  │                                   │  Lookup upload_id from DB        │
  │                                   │                                  │
  │                                   │  CompleteMultipartUpload ───────>│
  │                                   │  (bucket, key, uploadId, parts)  │
  │                                   │                                  │
  │                                   │<──── S3 assembles parts ─────────│
  │                                   │<──────────────────────── 200 ────│
  │                                   │                                  │
  │                                   │  Update DB:                      │
  │                                   │  status = "ready"                │
  │                                   │  upload_id = null                │
  │                                   │                                  │
  │                                   │  (Optional: trigger transcoding) │
  │                                   │                                  │
  │<──────────────────────────────────│                                  │
  │ { success: true,                  │                                  │
  │   attachment_id: "att_123" }      │                                  │


PHASE 4: USE IN POST
────────────────────
Client                              Server                              DB
  │                                   │                                  │
  │ POST /posts                       │                                  │
  │ { caption: "...",                 │                                  │
  │   attachment_ids: ["att_123"] }   │                                  │
  │──────────────────────────────────>│                                  │
  │                                   │                                  │
  │                                   │  Validate attachment status=ready│
  │                                   │  Create post with attachment data│
  │                                   │  Update attachment.post_id       │
  │                                   │────────────────────────────────> │
  │                                   │                                  │
  │<──────────────────────────────────│                                  │
  │ { post_id: "post_abc" }           │                                  │
```

---

## Resume Flow (After Crash)

```
Client                              Server                              S3
  │                                   │                                  │
  │  App restarts, checks local DB    │                                  │
  │  Finds: { attachment_id: att_123, │                                  │
  │           upload_id: xyz,         │                                  │
  │           local_file: /video.mp4 }│                                  │
  │                                   │                                  │
  │  Option A: Query S3 directly      │                                  │
  │  ListParts(bucket, key, uploadId) ───────────────────────────────────>│
  │<─────────────────────── parts 1-44 uploaded ─────────────────────────│
  │                                   │                                  │
  │  Option B: Query server           │                                  │
  │  GET /upload/status/att_123 ─────>│                                  │
  │<───── { completedParts: [1-44] } ─│                                  │
  │                                   │                                  │
  │  Resume from part 45...           │                                  │
```

---

## Key Takeaways

1. **Multipart is mandatory for large files** — single PUT will timeout, no retry granularity
2. **Two IDs:** `attachment_id` (yours, permanent) + `upload_id` (S3's, transient)
3. **Client must persist state locally** for resume capability
4. **CompleteMultipartUpload is required** — parts don't become an object until assembled
5. **S3 has no per-part events** — track via client callbacks or query ListParts
6. **Use lifecycle rules** to clean up abandoned uploads


---

## Async Assembly with Lambda

Instead of synchronous assembly, decouple it for better UX:

```
Client: POST /upload/complete { attachments: [...] }
Server: Immediately returns { accepted: true }
        Queues assembly job (Lambda/SQS)

Lambda: Calls CompleteMultipartUpload
        Updates attachment status to "ready"
        Pushes event via SSE/WebSocket
```

### Post Creation Sequencing

**Problem:** Client might hit `POST /posts` before assembly completes.

**Solutions:**

| Approach | How it works |
|----------|--------------|
| Poll/SSE | Client waits for "ready" event before posting |
| Draft state | Post created as "pending_media", published when ready |
| Retry | Server rejects if not ready, client retries |

**Draft state approach (Instagram-like):**
```
Client: POST /posts { attachment_ids: [...] }
Server: Creates post with status: "pending_media"
        When all attachments ready → flips to "published"
        Post appears in feed only after published
```

User sees optimistic local preview while backend finalizes.

---

## Cross-Device Resume with Checksums

Server-side state enables resuming uploads from a different device.

### Enhanced Schema

```
attachments:
  id: "att_123"
  user_id: "user_456"
  upload_id: "s3_xyz"
  object_key: "uploads/..."
  status: "pending" | "uploading" | "failed" | "ready"
  
  # For cross-device resume
  file_checksum: "sha256:abc123..."   ← computed client-side
  file_size: 524288000
  file_name: "vacation.mp4"           ← optional, for UI
  total_parts: 50
  completed_parts: [1,2,3...44]       ← track progress
  last_activity: 1705420800
  created_at: 1705420000
```

### Cross-Device Resume Flow

```
Device B                            Server                              
  │                                   │
  │ User opens app, selects same file │
  │ Client computes checksum locally  │
  │                                   │
  │ GET /uploads/pending              │
  │──────────────────────────────────>│
  │                                   │
  │<──────────────────────────────────│
  │ { pending: [                      │
  │   { attachment_id: "att_123",     │
  │     file_checksum: "sha256:abc",  │
  │     file_size: 524288000,         │
  │     file_name: "vacation.mp4",    │
  │     completed_parts: 44,          │
  │     total_parts: 50 }             │
  │ ]}                                │
  │                                   │
  │ Client matches checksum → same file!
  │                                   │
  │ POST /upload/resume               │
  │ { attachment_id: "att_123",       │
  │   checksum: "sha256:abc" }        │
  │──────────────────────────────────>│
  │                                   │
  │                                   │  Validate checksum matches
  │                                   │  Generate URLs for parts 45-50
  │                                   │
  │<──────────────────────────────────│
  │ { remaining_parts: [45,46...50],  │
  │   part_urls: [...] }              │
  │                                   │
  │ Resume upload from part 45        │
```

### Checksum Strategies

| Method | Speed | Use Case |
|--------|-------|----------|
| MD5 | Fast | Simple validation |
| SHA-256 | Slower | Strong integrity |
| Partial hash | Very fast | Large files |

**Partial hash for large videos:**
```
hash = SHA256(first_1MB + last_1MB + file_size)
```
Fast to compute, extremely unlikely to collide for different files.

### Edge Cases

| Scenario | Action |
|----------|--------|
| Checksum mismatch | Reject resume, start fresh upload |
| Upload expired (S3 timeout) | Abort old multipart, start new |
| Multiple pending uploads | Show list, let user pick or cleanup |
| File modified since upload started | Checksum won't match, start fresh |

This is how Dropbox, Google Drive, and similar services handle cross-device resume.
