# Elasticsearch - Things to Remember

## High Indexing Rate Issues

### The Core Issue: Immutable Segments

Elasticsearch (built on Apache Lucene) stores data in **immutable segments**. This architecture creates challenges at high indexing rates:

1. **New documents** are written to an in-memory buffer
2. **Refresh operation** (default: every 1 second) flushes this buffer to disk as a new **immutable segment**
3. These segments **cannot be modified** - they must be discarded and recreated for any updates

### The Bottleneck: Segment Merging

At high indexing rates, the main bottleneck is the segment merge process:

- **Too many small segments** are created rapidly during high-volume ingestion
- ES must continuously **merge** these small segments into larger ones in the background
- The merge process involves:
  - Reading multiple old segments from disk
  - Creating a new merged segment
  - Deleting old segments after merge completes
- This merge process is **I/O and CPU intensive** and struggles to keep up with very high write rates

### Why Throttling/Buffering is Necessary

When segments accumulate faster than they can be merged:
- **Search performance degrades** - more segments means slower searches (each query must check all segments)
- **Indexing slows down** due to merge backpressure and resource contention
- **Disk space increases** temporarily (old segments + new segments exist until merge completes)
- **Memory pressure** from maintaining segment metadata

## Common Solutions

1. **Increase refresh interval**
   - Reduce segment creation rate (e.g., set to 30s or 60s instead of 1s)
   - Trade-off: data not searchable immediately

2. **Use bulk requests**
   - Batch documents together in bulk API calls
   - Reduces overhead and improves throughput

3. **Tune merge policy**
   - Control when and how merges happen
   - Adjust `index.merge.scheduler.max_thread_count`
   - Configure segment size thresholds

4. **Add buffering layer**
   - Use message queue (Kafka, RabbitMQ, etc.) upstream
   - Control ingestion rate to match ES capacity

5. **Increase merge throttling limits**
   - Adjust `indices.store.throttle.max_bytes_per_sec`
   - Balance between indexing speed and cluster stability

6. **Optimize indexing settings**
   - Disable replicas during bulk indexing
   - Increase `index.translog.flush_threshold_size`
   - Use appropriate shard count

## How Search Works Across Segments (vs Key-Value Stores)

### The Critical Difference

**Key-Value Stores (SSTables/LSM):**
- Query for key "user:123" → Read **latest segment first**, return when found
- Older segments ignored once key is found
- Only need to check segments until you get the answer
- "Latest wins" optimization works perfectly

**Lucene/Elasticsearch (Inverted Index):**
- Query for "mountain" → Must check **ALL segments**
- Can't stop early - "mountain" might exist in documents across every segment
- Must **merge all results** from all segments at query time
- No "latest wins" - need complete picture from all segments

### Query-Time Segment Merging

When you search for "mountain":

1. **Each segment is queried independently**:
   ```
   Segment 1: mountain → [doc1, doc5, doc8] with scores
   Segment 2: mountain → [doc2, doc3] with scores
   Segment 3: mountain → [doc7, doc15, doc20] with scores
   ```

2. Lucene **merges results** from all segments in memory
3. Scores and ranks the combined result set
4. Returns top N results

**This is why many segments = slow searches** - every query must hit every single segment!

### Background Segment Merging Process

During background merge:

1. **Read inverted indexes** from old segments:
   ```
   Segment 1: mountain → [doc1:pos[3,8], doc5:pos[1], doc8:pos[5,6]]
   Segment 2: mountain → [doc2:pos[2], doc3:pos[1,4,7]]
   ```

2. **Combine posting lists** for each term:
   ```
   New Segment: mountain → [doc1, doc2, doc3, doc5, doc8]
                           (with all positions, frequencies, norms merged)
   ```

3. **Reassign document IDs** (doc IDs are segment-relative)
4. **Create new unified inverted index** in merged segment
5. **Delete old segments** once merge completes

### Why This is More Expensive Than KV Stores

- **Every term** in the inverted index needs its posting lists merged
- **Every document** gets new ID assignment and rewriting
- **All positional data, frequencies, field norms** must be rewritten
- Can't do "latest wins" optimization - need to merge all occurrences
- Must rebuild entire inverted index structure for merged segment

This architectural difference is why Elasticsearch requires different tuning strategies compared to key-value databases like Cassandra or DynamoDB.

## Key Takeaway

The immutable segment architecture is fundamental to Elasticsearch's design. High-rate ingestion requires buffering and rate limiting because the merge process (creating new segments from old ones) cannot keep up indefinitely with segment creation.

Search engines must query ALL segments (unlike KV stores that can stop at the first match), which is why reducing segment count through merging is critical for search performance.
