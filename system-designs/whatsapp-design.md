# WhatsApp System Design

## Create Message Flow

### Overview
- Client generates unique `message_id` (UUID)
- Write to DynamoDB first (durability), then push to Redis Pub/Sub (real-time delivery)
- Polling for undelivered messages handles transient failures

### Flow

```
Client sends {message_id, chat_id, content, ...}
                    │
                    ▼
      ┌─────────────────────────────┐
      │   DynamoDB Transaction      │
      │   • Message table           │
      │     (condition:             │
      │     attribute_not_exists)   │
      │   • Inbox (each recipient)  │
      └─────────────┬───────────────┘
                    │
          ┌─────────┴─────────┐
          │                   │
       Success         ConditionalCheckFailed
          │                   │
          │            (already exists,
          │             skip to Pub/Sub)
          └─────────┬─────────┘
                    │
                    ▼
          ┌─────────────────┐
          │ Redis Pub/Sub   │
          │ (notify recipient)
          └────────┬────────┘
                   │
             ┌─────┴─────┐
             │           │
          Success      Fail
             │           │
             │        Retry once
             │           │
             │     ┌─────┴─────┐
             │   Success     Fail
             │     │           │
             │     │     Return retryable error
             │     │     (client retries, or
             │     │      polling catches it)
             ▼     ▼
          Return 200 OK
```

### Key Design Decisions

1. **Client-generated message ID** - Acts as idempotency key; no separate idempotency table needed
2. **Conditional write** - `attribute_not_exists(message_id)` on Message table prevents duplicates
3. **DynamoDB transaction limit** - 100 items per transaction; supports groups up to 100 members
4. **Durability first** - Message persisted before real-time notification attempted
5. **Fallback** - Periodic polling for undelivered messages handles edge cases (Redis failures, disconnections)

### Tables Involved

| Table | Purpose |
|-------|---------|
| Message | Stores message content, creator, timestamp |
| Inbox | Maps recipient_id → message_id for undelivered messages |
