# LLD: LLM Request Routing Layer

**Company Context:** Emergent (agentic vibe-coding platform)
**Round:** Low-Level Design (45–60 min)
**Domain Fit:** Emergent routes agent requests across multiple LLM providers (OpenAI, Anthropic, Google, open-source). The routing layer decides which model, which endpoint, whether to throttle, and how to handle failure — all under a 5ms latency budget because agents are in a hot loop.

## Prerequisites (LLD round)

Low-Level Design means zooming into one box from an HLD diagram and making every interface concrete — data models, API contracts, concurrency, storage layouts, and migration strategies.

**For each topic in this doc, aim for:** real interface signatures (not pseudocode), database schemas with indexes and constraints, and explicit walkthroughs of concurrent access (who races, what guard applies, what the client observes on success vs conflict).

### Core concepts

| Area | Know this |
|------|-----------|
| **Database schema** | Normalization, indexing strategies, composite keys, foreign keys. Design for concurrent writes and efficient reads. |
| **API contracts** | REST and gRPC signatures; input validation; error codes; pagination; idempotency keys. |
| **Concurrency** | Optimistic concurrency (version guards, CAS), pessimistic locking, when each fits; thundering herd and how to mitigate it. |
| **Event sourcing / CQRS** | When to store events vs snapshots; rebuilding state; compaction. |
| **Migrations** | Online schema migration (expand–contract), dual-writes, backfills; migrating without downtime. |
| **Storage efficiency** | When full snapshots are expensive; deltas, checkpoints, compaction. |

### What interviewers look for

| Criterion | What “good” looks like |
|-----------|-------------------------|
| **Data model** | Concrete schemas: column types, indexes, constraints — not hand-wavy “we’ll store it in a database.” |
| **API / interface** | Clear signatures with typed parameters, return types, and error cases; idempotency where needed. |
| **Concurrency** | You identify contention and design explicit guards — versions, atomic ops, conflict resolution — not vague “we’ll add a lock.” |
| **Storage** | You reason about growth and propose compaction, TTLs, archival. |
| **Edge cases** | Interactions like rollback vs compaction, migration vs concurrent writes, undo vs irreversible side effects. |

### Common mistakes

- Staying abstract (“a table for messages”) instead of showing DDL, columns, and indexes.
- Ignoring concurrent access: if two clients can touch the same resource, specify lock type or CAS, scope, and behavior on conflict.
- Forgetting side effects: if undo/rollback is possible, what happens to charges, notifications, or external API calls you cannot reverse?
- No migration story: schema will change — expand–contract beats “take downtime.”

---

## "The routing decision needs to happen in under 5ms. But I need to check rate limits, health, and model config. Can I query Postgres for every request?"

> At 10K–50K requests/minute, a Postgres round-trip per request would add 2–5ms of latency just for the routing decision. That's the entire budget.

No. The routing hot path must be entirely in-memory or Redis. Postgres is the durable source of truth for configuration and audit, but the per-request decision path never touches it.

### Two-layer data architecture

| Layer | What Lives Here | Refresh Interval |
|---|---|---|
| **Redis (hot)** | Endpoint health, rate limit counters, active request counts, model config cache | Health: 5s. Config: 60s. Counters: real-time |
| **Postgres (durable)** | Model registry, endpoint definitions, rate limit configs, request logs | Written on config change or async after each request |

**Redis structures for the hot path:**

```
# Endpoint health (updated every 5s by health checker background job)
HSET endpoint:health:{endpoint_id}  status "healthy"  avg_latency_ms "230"  failure_rate "0.02"

# Tenant rate limiting (sliding window — sorted sets with timestamp scores)
ZADD ratelimit:rpm:{tenant_id} {timestamp_ms} {request_id}
ZADD ratelimit:tpm:{tenant_id} {timestamp_ms} {token_count}

# Concurrency counter (atomic increment/decrement)
INCR concurrent:{tenant_id}:{model_alias}
DECR concurrent:{tenant_id}:{model_alias}

# Model config cache (refreshed every 60s from Postgres)
HSET model:config:{model_alias}  model_id "{uuid}"  context_window "128000"  endpoints "[...]"
```

A single routing decision does: 1 HGET for model config + 2 ZCARD for rate limits + 1 GET for concurrency + 1 HGET for endpoint health = **~5 Redis commands, pipelined into a single round-trip (~0.5ms)**. Well within the 5ms budget.

**What to say:**

> "Postgres is the durable source of truth for model registry and config. Redis holds the hot-path data: endpoint health, rate limit counters, concurrency counts, and a cached model config. A routing decision is 5 pipelined Redis commands in a single round-trip — about 0.5ms. Postgres is never on the request path."

---

## "How does the routing pipeline actually work? A request comes in — walk me through the decision."

> End-to-end: agent needs an LLM call. What happens step by step before the first token streams back?

### Five-stage pipeline

```
Request In → [Rate Limit] → [Model Resolution] → [Endpoint Selection] → [Execute + Retry] → [Async Post-Processing]
```

**Stage 1: Rate Limit Check** — can this tenant make this request right now?

Three checks, all against Redis:
- Tenant requests-per-minute (RPM)
- Tenant tokens-per-minute (TPM, estimated from input tokens)
- Tenant concurrent request count

If any exceeds the limit → `429 Too Many Requests` with `Retry-After` header. Short-circuit, no further stages.

**Stage 2: Model Resolution** — which model should handle this?

The request specifies a `model_alias` ("fast", "smart", "code") rather than a specific model. Resolution:
1. Look up alias in Redis cache → get primary model
2. Filter by required capabilities (tool use? vision? cost ceiling?)
3. Append fallback models in preference order

```typescript
interface ModelPreference {
  alias?: string;                // "fast", "smart", "code"
  model_name?: string;           // explicit override
  require_tool_use?: boolean;
  require_vision?: boolean;
  max_cost_per_mtok?: number;    // cost ceiling
  fallback_aliases?: string[];   // ordered fallback list
}
```

This indirection is key — you can swap the model behind "fast" from GPT-4o-mini to Claude Haiku without any agent code changes. Just update the DB, Redis refreshes in 60 seconds.

**Stage 3: Endpoint Selection** — which specific endpoint for the chosen model?

A model can have multiple endpoints (different regions, different API keys, primary vs secondary). Selection uses **weighted random with latency bias**:

```python
for endpoint in healthy_endpoints:
    health = redis.hgetall(f"endpoint:health:{endpoint.id}")
    latency_factor = 1000 / max(int(health['avg_latency_ms']), 100)
    score = endpoint.weight * latency_factor

# Weighted random selection using scores
```

Lower-latency endpoints get proportionally more traffic. Unhealthy or circuit-open endpoints are excluded entirely.

**Stage 4: Execute with Retry** — call the provider, handle failures (detailed in the next section).

**Stage 5: Async Post-Processing** — fire-and-forget after response starts streaming:
- Record request in Postgres (request log)
- Update health metrics in Redis
- Update rate limit counters with actual token usage

**What to say:**

> "Five-stage pipeline: rate limit, model resolution, endpoint selection, execute with retry, async post-processing. Stages 1–3 are all Redis and complete in under 2ms. Model aliases decouple agents from specific models — swapping the model behind 'fast' is a DB update, not a code deploy. Endpoint selection uses weighted random with latency bias so faster endpoints naturally absorb more traffic."

---

## "OpenAI starts returning 500s. What happens? How does the circuit breaker work?"

> Provider degradation is the most common failure mode. How do you detect it and what do you do?

### Circuit breaker per endpoint

Each endpoint has an independent circuit breaker with three states:

```
CLOSED (healthy) ──failure rate > 50% over 20 requests──→ OPEN (broken)
                                                              │
                                                   after 30s cooldown
                                                              ↓
                                                        HALF_OPEN (probing)
                                                      ╱              ╲
                                              3 successes         any failure
                                                  ↓                   ↓
                                               CLOSED              OPEN (reset cooldown)
```

**Configuration:**

```typescript
const CIRCUIT_CONFIG = {
  failure_threshold: 0.5,     // 50% failure rate triggers open
  min_requests: 20,           // need 20 requests before evaluating (avoid false triggers on small samples)
  cooldown_ms: 30_000,        // 30s before allowing probe requests
  half_open_max: 3,           // 3 successful probes → close the circuit
};
```

Health state is stored in Redis (updated on every request completion) and persisted to Postgres every 5 seconds for dashboards.

**Why per-endpoint, not per-provider?** A provider might have multiple endpoints in different regions. US-East can be down while EU-West is fine. Per-endpoint granularity means you route around the broken region, not abandon the entire provider.

### The retry and fallback decision tree

When a request fails, the error is classified to determine what to do next:

```typescript
function classifyError(error): 'retry_same' | 'retry_different_endpoint' | 'fallback_model' | 'no_retry' {
  if (error.status in [502, 503, 504])        return 'retry_same';           // transient
  if (error.status === 429) {
    if (retryAfterHeader > 30s)               return 'fallback_model';       // long wait → switch
    else                                       return 'retry_same';           // short wait → backoff and retry
  }
  if (error.status in [400, 401, 403, 413])   return 'no_retry';             // client error
  if (error instanceof ConnectionError)        return 'retry_different_endpoint';
  if (error instanceof TimeoutError)           return 'retry_same';
  return 'no_retry';
}
```

**Execution flow:**

```
Request fails
  → classify error
  → retry_same?              → exponential backoff (200ms, 400ms, 800ms + jitter), max 3 attempts
  → retry_different_endpoint? → pick alternate endpoint for same model
  → fallback_model?           → pick next model from the fallback list (e.g., "smart" → "fast")
  → no_retry?                 → return error to caller
```

The fallback chain means a user-facing agent doesn't fail just because one provider is down — it transparently degrades to an alternate model.

**What to say:**

> "Circuit breaker per endpoint, not per provider — US-East can be down while EU-West is fine. Three states: closed, open, half-open. Failure classification drives retry strategy: transient 5xx errors retry the same endpoint with backoff; connection errors retry a different endpoint; provider-level issues or long 429s fall back to an alternate model entirely. The fallback chain means a user-facing agent transparently degrades instead of failing."

---

## "Circuit breaks, pending requests pile up. Then the circuit closes. Won't all the queued requests slam the provider at once?"

> This is the thundering herd problem on circuit recovery.

Exactly. If you immediately route 100% of traffic back to a recovered endpoint, you'll likely trip the circuit again within seconds.

### Solution: Gradual traffic ramp on recovery

When a circuit transitions OPEN → HALF_OPEN → CLOSED, the endpoint gets a **traffic ramp** — starting at 10% weight, linearly increasing to 100% over 60 seconds:

```python
def get_effective_weight(endpoint, ramp):
    if not ramp:
        return endpoint.weight

    elapsed = now() - ramp.start_time
    progress = min(elapsed / ramp.duration_ms, 1.0)
    percentage = 0.1 + (0.9 * progress)       # 10% → 100% over 60s
    return int(endpoint.weight * percentage)
```

During the ramp, other healthy endpoints absorb the overflow proportionally. The recovering endpoint proves it can handle increasing load before getting full traffic back.

This is stored in Redis alongside the circuit state:

```
HSET endpoint:health:{id}  ramp_start "1711929000"  ramp_duration_ms "60000"
```

**What to say:**

> "On circuit recovery, don't immediately route 100% traffic back. Use a linear ramp from 10% to 100% over 60 seconds. Other healthy endpoints absorb the overflow during the ramp. This prevents the thundering herd from immediately re-tripping the circuit."

---

## "We have free-tier users running background tasks and enterprise users watching agents work live. How does priority work?"

> An enterprise user staring at a live agent session should not be blocked because free-tier background indexing jobs are consuming all the LLM capacity.

### Four priority tiers with differentiated treatment

```typescript
const PRIORITY_CONFIG = {
  critical: { rate_limit_multiplier: 2.0, max_wait_ms: 0,     shed_at_utilization: never },
  high:     { rate_limit_multiplier: 1.0, max_wait_ms: 1000,  shed_at_utilization: 0.99 },
  normal:   { rate_limit_multiplier: 1.0, max_wait_ms: 5000,  shed_at_utilization: 0.95 },
  low:      { rate_limit_multiplier: 0.5, max_wait_ms: 30000, shed_at_utilization: 0.80 },
};
```

- **Critical** (user watching an agent live): Gets 2x the normal rate limit allowance. Never queued, never shed.
- **High** (agent in active DAG execution): Standard limits. Can wait up to 1 second in queue.
- **Normal**: Standard. Shed at 95% system utilization.
- **Low** (background jobs — embedding generation, re-indexing): Gets only 50% of rate limit allocation. Shed first at 80% utilization.

**Load shedding** — when the system is near capacity, low-priority requests are rejected first:

```python
def should_admit(request, current_utilization):
    config = PRIORITY_CONFIG[request.priority]
    if current_utilization >= config.shed_at_utilization:
        return False
    return True
```

This ensures that a flood of low-priority background work can never impact the user-facing experience. The background jobs get `429`s with `Retry-After` and try again later — they're designed to be tolerant of delays.

**What to say:**

> "Four priority tiers: critical for live user sessions (2x rate limit, never shed), high for active DAG tasks, normal for standard, low for background jobs (50% rate limit, shed first at 80% utilization). This guarantees that a flood of background embedding jobs can never starve a user watching their agent work. Low-priority callers are designed to tolerate 429s with retry."

---

## "How does rate limiting actually work? Fixed window? Token bucket? Sliding window?"

> Walk me through the algorithm choice and the Redis implementation.

### Sliding window log (Redis sorted sets)

**Why not fixed window?** Fixed windows have the boundary burst problem — if the limit is 100 RPM and a client sends 100 requests at T=59s, then 100 more at T=61s, they've sent 200 requests in 2 seconds while technically staying within each individual window.

**Why not token bucket?** Token bucket is great conceptually but harder to implement atomically in Redis when you need to track both RPM and TPM simultaneously. Sliding window log is simpler and equally correct.

**Implementation:**

```python
def check_rate_limit(tenant_id, estimated_input_tokens):
    now_ms = int(time.time() * 1000)
    window_start = now_ms - 60_000

    config = get_rate_limit_config(tenant_id)  # from Redis cache

    pipe = redis.pipeline()

    # RPM check
    rpm_key = f"ratelimit:rpm:{tenant_id}"
    pipe.zremrangebyscore(rpm_key, 0, window_start)   # prune expired entries
    pipe.zcard(rpm_key)                                # count entries in window

    # TPM check
    tpm_key = f"ratelimit:tpm:{tenant_id}"
    pipe.zremrangebyscore(tpm_key, 0, window_start)
    pipe.zrangebyscore(tpm_key, window_start, '+inf')  # get all entries to sum tokens

    _, current_rpm, _, token_entries = pipe.execute()

    if current_rpm >= config.requests_per_minute * config.burst_allowance:
        oldest = redis.zrangebyscore(rpm_key, window_start, '+inf', start=0, num=1)
        retry_after = int(oldest[0]) + 60_000 - now_ms if oldest else 1000
        return RateLimitResult(blocked=True, retry_after_ms=retry_after, limit_type='rpm')

    current_tpm = sum(int(entry.split(':')[1]) for entry in token_entries)
    if current_tpm >= config.tokens_per_minute:
        return RateLimitResult(blocked=True, retry_after_ms=5000, limit_type='tpm')

    # Concurrency check
    concurrent = int(redis.get(f"concurrent:{tenant_id}") or 0)
    if concurrent >= config.concurrent_max:
        return RateLimitResult(blocked=True, retry_after_ms=1000, limit_type='concurrent')

    return RateLimitResult(blocked=False)
```

**On request start:** `ZADD` the request into the RPM and TPM sorted sets. `INCR` the concurrency counter.
**On request end:** `DECR` the concurrency counter. The sorted set entries auto-expire via the `ZREMRANGEBYSCORE` prune on the next check.

**Provider-level rate limits** use the same pattern, scoped to `provider_id` instead of `tenant_id`. If a provider is near its global limit, the router prefers endpoints on other providers even if the tenant has headroom.

**What to say:**

> "Sliding window log using Redis sorted sets. Scores are timestamps, members are request IDs. On each check, prune entries older than 60 seconds, count what's left. Avoids the boundary burst problem of fixed windows. Both RPM and TPM are tracked — TPM entries encode the token count in the member string. Provider-level limits use the same pattern scoped by provider_id. All checks are pipelined into a single Redis round-trip."

---

## "We sign a deal with a new LLM provider. How does it get added?"

> Adding a new provider — is it a code deploy or a config change?

### Most providers: pure data change, no code deploy

```sql
INSERT INTO model_providers (name, base_url, auth_type) VALUES ('new_provider', 'https://api.new.com/v1', 'api_key');
INSERT INTO models (provider_id, model_name, model_alias, ...) VALUES ($id, 'new-model-v1', 'fast', ...);
INSERT INTO model_endpoints (model_id, url, region, weight) VALUES ($id, 'https://api.new.com/v1/chat', 'us-east-1', 50);
INSERT INTO rate_limit_configs (scope_type, scope_id, ...) VALUES ('provider', $id, ...);
INSERT INTO endpoint_health (endpoint_id, status) VALUES ($id, 'healthy');
```

Start with `weight=50` (half of default 100) for gradual rollout. Monitor health. Ramp up weight as confidence grows. The routing layer picks up new models within 60 seconds (Redis cache refresh).

### When code changes are needed: provider adapter interface

Most LLM providers follow the OpenAI-compatible API format. When one doesn't:

```typescript
interface ProviderAdapter {
  name: string;
  formatRequest(req: LLMRequest): ProviderSpecificRequest;
  parseResponse(raw: ProviderSpecificResponse): LLMResponse;
  parseStreamChunk(chunk: string): StreamDelta;
  classifyError(error: HttpError): ErrorClassification;
}

const ADAPTERS: Record<string, ProviderAdapter> = {
  openai: new OpenAIAdapter(),
  anthropic: new AnthropicAdapter(),
  google: new GoogleAdapter(),
};
```

Adding a non-standard provider = implement this 4-method interface + register it. The routing layer doesn't change.

**What to say:**

> "Standard providers are a pure data change — insert rows, Redis refreshes in 60 seconds, traffic starts flowing. Start with half weight for gradual rollout. Non-standard API formats are handled by a provider adapter interface — four methods: format request, parse response, parse stream chunk, classify error. The routing pipeline itself never changes."

---

## "How do you track LLM costs? A tenant wants their monthly bill. Finance wants total spend."

> Real-time cost tracking at both tenant and system level.

### Per-request cost calculation

```typescript
function calculateCost(model, usage): number {
  return (usage.input_tokens / 1_000_000) * model.input_cost_per_mtok
       + (usage.output_tokens / 1_000_000) * model.output_cost_per_mtok;
}
```

After each request completes, the actual cost is recorded in the request log (Postgres). But for **real-time budget enforcement**, you can't wait for the Postgres write. A running counter in Redis tracks spend:

```
INCRBYFLOAT cost:tenant:{tenant_id}:monthly {cost}
EXPIRE cost:tenant:{tenant_id}:monthly 2764800    # 32 days
```

The admission check at Stage 1 of the pipeline compares this counter against the tenant's budget ceiling. If `current_spend >= 0.95 * monthly_budget`, the system can either reject with `402`, alert the tenant, or downgrade to cheaper models automatically.

### Aggregation queries (for dashboards and billing)

```sql
-- Tenant monthly spend
SELECT DATE_TRUNC('month', started_at), SUM(actual_cost), COUNT(*), SUM(input_tokens + output_tokens)
FROM llm_requests
WHERE tenant_id = $1 AND started_at >= DATE_TRUNC('month', now())
GROUP BY 1;

-- Cost by model (for optimization decisions)
SELECT m.model_name, COUNT(*), AVG(actual_cost), SUM(actual_cost)
FROM llm_requests r JOIN models m ON r.model_id = m.model_id
WHERE started_at >= now() - INTERVAL '7 days'
GROUP BY m.model_name ORDER BY SUM(actual_cost) DESC;
```

The `llm_requests` table is partitioned by month (at 50K req/min = ~2B rows/month — partitioning is mandatory, not optional).

**What to say:**

> "Per-request cost is computed from token usage times model pricing and stored in the request log. Real-time budget enforcement uses a Redis counter — INCRBYFLOAT per request, compared against the tenant's budget ceiling at admission time. The request log table is partitioned by month because at 50K requests/minute, you're looking at 2 billion rows/month — unpartitioned Postgres would choke on any aggregation query."

---

## Appendix: Full Database Schema

```sql
CREATE TABLE model_providers (
    provider_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL UNIQUE,
    base_url        TEXT        NOT NULL,
    auth_type       TEXT        NOT NULL CHECK (auth_type IN ('api_key', 'oauth', 'iam')),
    is_enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE models (
    model_id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id           UUID        NOT NULL REFERENCES model_providers(provider_id),
    model_name            TEXT        NOT NULL,
    model_alias           TEXT        NOT NULL UNIQUE,
    context_window        INT         NOT NULL,
    input_cost_per_mtok   DECIMAL(10,4) NOT NULL,
    output_cost_per_mtok  DECIMAL(10,4) NOT NULL,
    supports_streaming    BOOLEAN     NOT NULL DEFAULT TRUE,
    supports_tool_use     BOOLEAN     NOT NULL DEFAULT FALSE,
    supports_vision       BOOLEAN     NOT NULL DEFAULT FALSE,
    max_output_tokens     INT,
    is_enabled            BOOLEAN     NOT NULL DEFAULT TRUE,
    priority              INT         NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (provider_id, model_name)
);

CREATE INDEX idx_models_alias ON models (model_alias) WHERE is_enabled = TRUE;
CREATE INDEX idx_models_capabilities ON models (supports_tool_use, supports_vision) WHERE is_enabled = TRUE;


CREATE TABLE model_endpoints (
    endpoint_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    model_id        UUID        NOT NULL REFERENCES models(model_id),
    url             TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    weight          INT         NOT NULL DEFAULT 100,
    is_enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_endpoints_model_region ON model_endpoints (model_id, region) WHERE is_enabled = TRUE;


CREATE TABLE endpoint_health (
    endpoint_id         UUID    PRIMARY KEY REFERENCES model_endpoints(endpoint_id),
    status              TEXT    NOT NULL DEFAULT 'healthy'
                        CHECK (status IN ('healthy', 'degraded', 'unhealthy', 'circuit_open')),
    success_count       INT    NOT NULL DEFAULT 0,
    failure_count       INT    NOT NULL DEFAULT 0,
    avg_latency_ms      INT    NOT NULL DEFAULT 0,
    p99_latency_ms      INT    NOT NULL DEFAULT 0,
    last_success_at     TIMESTAMPTZ,
    last_failure_at     TIMESTAMPTZ,
    last_error          TEXT,
    circuit_open_until  TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE rate_limit_configs (
    config_id           UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    scope_type          TEXT    NOT NULL CHECK (scope_type IN ('tenant', 'model', 'provider', 'global')),
    scope_id            TEXT    NOT NULL,
    requests_per_minute INT    NOT NULL,
    tokens_per_minute   INT    NOT NULL,
    concurrent_max      INT    NOT NULL DEFAULT 100,
    burst_allowance     DECIMAL(3,1) NOT NULL DEFAULT 1.5,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (scope_type, scope_id)
);


CREATE TABLE llm_requests (
    request_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         UUID        NOT NULL,
    workspace_id      UUID        NOT NULL,
    agent_task_id     UUID,
    model_id          UUID        NOT NULL REFERENCES models(model_id),
    endpoint_id       UUID        NOT NULL REFERENCES model_endpoints(endpoint_id),
    input_tokens      INT         NOT NULL,
    output_tokens     INT,
    estimated_cost    DECIMAL(10,6),
    actual_cost       DECIMAL(10,6),
    routing_reason    TEXT,
    routing_latency_us INT       NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'streaming', 'completed', 'failed', 'cancelled')),
    priority          TEXT        NOT NULL CHECK (priority IN ('critical', 'high', 'normal', 'low')),
    http_status       INT,
    error_code        TEXT,
    error_message     TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_token_at    TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    total_latency_ms  INT,
    attempt_number    INT         NOT NULL DEFAULT 1,
    parent_request_id UUID       REFERENCES llm_requests(request_id)
) PARTITION BY RANGE (started_at);

CREATE INDEX idx_requests_tenant ON llm_requests (tenant_id, started_at DESC);
CREATE INDEX idx_requests_model ON llm_requests (model_id, started_at DESC);
CREATE INDEX idx_requests_active ON llm_requests (status) WHERE status IN ('pending', 'streaming');
```

---

## "Who owns the routing decision vs. who tracks health? Where does the circuit breaker live?"

> The router calls the model. Health tracker tracks the outcome via defer. But does health tracker make the routing call, or just update state?

The separation is clean: **Health Tracker owns state, Router owns decisions.**

```go
func RouteRequest() {
    response := router.RouteRequest()       // reads health state, makes routing call
    defer healthTracker.Track(response)     // updates health state async after response
}
```

Router is stateless per-request — it reads whatever Health Tracker has written to Redis and makes a point-in-time routing call. Health Tracker is the longitudinal observer — it sees every outcome and updates the aggregate state.

**Where does the circuit breaker fit?**

In a traditional single-process system (Hystrix, gobreaker), the circuit breaker is a self-contained in-process wrapper. It observes calls going through it, maintains its own failure counters, and manages its own CLOSED → OPEN → HALF_OPEN state machine. There is no separate "Health Tracker" because the CB sees every call directly.

In this distributed routing layer, that is not possible. You have multiple router pods, and they all need to agree on circuit state. So the CB state is externalized to Redis. This splits the classic CB's four jobs across boundaries:

- **Observation** → Health Tracker (each router pod reports outcomes via defer)
- **State** → Redis (shared across all pods)
- **State transitions** → Health Tracker evaluates aggregate metrics and flips circuit state
- **Routing decision** → Router reads the resulting status per-request

**Circuit Breaker as a strategy inside Health Tracker** is the cleanest model:

```go
type HealthTracker struct {
    id             string
    circuitBreaker CircuitBreakerImpl  // pluggable strategy
}

func (h *HealthTracker) Track(response) {
    h.updateMetrics(response)                         // raw data update
    newStatus := h.circuitBreaker.Evaluate(h.metrics) // CB analyses that data
    h.writeStatus(newStatus)                          // status lives in HT
}
```

CB here is a pure analysis function — it takes health metrics as input, returns a status signal. It does not own data, it just does analysis. The status it returns (`healthy`, `degraded`, `circuit_open`) is still one of Health Tracker's statuses. Single source of truth stays in Health Tracker. And the CB impl is substitutable — you can swap a simple threshold impl for an adaptive one based on rolling percentiles without touching Health Tracker's data layer.

**Why not two parallel defers?**

```go
// Don't do this
defer healthTracker.Track(response)
defer circuitBreaker.Track(response)
```

This creates two independent components both observing raw responses. Circuit Breaker would need to maintain its own failure counters, creating a second source of truth. And CB needs the *aggregate* (failure rate over last N requests), not just the single response — so it would need to call back into Health Tracker anyway, creating a dependency in the wrong direction.

**When would you make Circuit Breaker a fully separate component?**

- Multiple systems feeding health signals (router + synthetic health-check poller + alerting system). No single Health Tracker "owns" all observations.
- Circuit state needs to be shared across multiple different routing services (agent router + batch router both need to respect the same endpoint's circuit state).
- The state machine logic is complex enough to be its own domain — adaptive cooldowns, per-tenant circuit state, A/B experiments on recovery ramp curves.
- Regulatory requirement: every circuit state transition must be auditable, manually overridable via an ops API.

For this system, none of these apply. One router, one health observer, straightforward state machine. CB baked into Health Tracker wins.

**How Netflix does it vs. this design:**

Hystrix/resilience4j model the CB as a separate abstraction, but it is an *in-process library* wrapping individual calls. Each service instance has its own independent CB view — 20 router pods would have 20 independent circuit states. That is fine if you are okay with eventual consistency on circuit state. Here, all pods need to agree, so state is externalized to Redis, and the CB concept gets distributed across Health Tracker + Redis.

Envoy/Istio (service mesh) are actually closer — the sidecar proxy does health tracking AND circuit breaking as one unit, baked together. The application does not know it is happening. Same pattern, just at the infra layer.

---

## "The router proxies all responses — how does streaming work? What about an LB in front?"

> If the router tracks health via defer, it must see every response. Does that work for streaming? And with an LB in front, do we need sticky connections?

**Router must be a reverse proxy, not a redirect.** If it issued a 302, the client would talk directly to OpenAI. Router would never see the response, latency, status code, or token counts — Health Tracker gets nothing. All responses must flow back through the router. That is what makes `defer Track(response)` possible.

**Streaming — two long-lived connections held simultaneously:**

```
Client <──────── Router ────────── OpenAI
        (SSE/chunked)    (SSE/chunked)
```

Router holds both connections open, piping chunks from provider to client. One goroutine per active stream. The `defer Track()` fires when the stream ends or errors.

What counts as failure in a streaming context:
- Connection refused before first token → clear failure
- First token arrives, stream cuts off mid-response → partial failure
- Full stream completes → success

Track **TTFT (time to first token)** separately from total latency — for agents in a hot loop, TTFT is what the user actually feels.

**SSE vs HTTP/2 — two different layers:**

- **HTTP/2 is the transport.** It multiplexes multiple logical streams over a single TCP connection. One TCP connection between Router and OpenAI can carry many concurrent requests simultaneously. This is connection pooling.
- **SSE is the application-level streaming protocol.** The LLM provider sends tokens back incrementally as `data: {...}\n\n` chunks until `data: [DONE]`. This rides on whatever transport is underneath.

OpenAI's API in practice often uses HTTP/1.1 with chunked transfer encoding rather than full HTTP/2. The SSE format is the same either way.

**LB in front — long-lived, not sticky:**

The key distinction: **sticky = artificial affinity, engineered to route the same client to the same server repeatedly. Long-lived = a connection that persists naturally for the duration of a stream.**

```
Client → LB → Router Pod A  →  OpenAI (connection pool)
               Router Pod B  →  OpenAI
               Router Pod C  →  OpenAI
```

**LB → Router: not sticky.** All health state, rate limit counters, circuit state — everything is in Redis. Every router pod has the same view. Any pod can correctly route any request.

**Router → Provider: connection pooled.** Router maintains a pool of persistent HTTP/2 connections to each endpoint. Multiple requests multiplex over the same connection. You reuse connections, but you are not stuck to one per client.

**For streaming during a single request:** the connection naturally stays put for its duration — that is just TCP, not engineered stickiness. LB will not re-route mid-stream. The next request from the same client starts fresh and LB can send it anywhere.

Stickiness would only be required if Router pods cached session state locally (conversation history in-memory). Since all state is in Redis, it is not needed.

---

## "When a model fails, how do we retry? Can the client pass a latency budget?"

> Once a model is selected and fails — same endpoint retry, different endpoint, different model? And should acceptable latency be a request parameter?

The retry progression follows error classification:

```
Request fails on OpenAI US-East
    → transient 5xx?           retry same endpoint (backoff: 200ms, 400ms, 800ms)
    → still failing?           try different OpenAI endpoint (EU-West)
    → provider-level issue
      or long 429?             fallback to next model in chain (GPT-4o → Claude Haiku)
    → that fails too?          next fallback
    → exhausted all?           return error to client
```

Same model, different endpoint first — because a different endpoint is cheaper than a model fallback. No capability degradation, just a regional switch. Only escalate to a different model when the provider itself is the problem.

**Making the latency budget a request param is the right extension.** It adds a time-budget dimension alongside error classification:

```go
type RoutingHints struct {
    MaxLatencyMs     int   // client's deadline
    FallbackAllowed  bool  // can we degrade to a cheaper model?
    MaxRetryAttempts int
}
```

If `MaxLatencyMs` is 2000ms and you have already spent 1800ms on the first attempt, there is no point retrying — you will blow the client's deadline anyway. The retry loop should check remaining time budget before each attempt, not just attempt count.

This is what Google's **deadline propagation** does — the original caller's deadline flows through the entire call chain, and every hop respects it rather than independently retrying indefinitely.

**Retry decision is two-dimensional:**

| Dimension | Drives |
|---|---|
| Error type | *What* to retry (same endpoint / different endpoint / different model / no retry) |
| Remaining time budget | *Whether* to retry at all |

Both as config makes it flexible per caller type — a background job might have a 30s budget and allow the full fallback chain, a live user session might have 3s and only allow one retry before returning an error.

---

## Quick Reference: Interview Talking Points

| Topic | Key Point |
|---|---|
| **Hot Path** | All routing decisions in Redis (pipelined, ~0.5ms). Postgres is durable source of truth, never on request path |
| **Pipeline** | 5 stages: rate limit → model resolution → endpoint selection → execute with retry → async post-processing |
| **Model Aliases** | Agents request "fast" not "gpt-4o-mini". Swap models via DB update, no code deploy |
| **Rate Limiting** | Sliding window log in Redis sorted sets. Per-tenant RPM/TPM + per-provider global + concurrency cap |
| **Circuit Breaker** | Per-endpoint (not per-provider). Gradual traffic ramp on recovery prevents thundering herd |
| **Fallback** | Error classification drives strategy: retry same, different endpoint, alternate model, or no retry |
| **Priority** | 4 tiers. Low-priority shed first at 80% utilization. Critical gets 2x rate limit headroom |
| **Cost** | Redis counter for real-time budget enforcement. Postgres request log (partitioned by month) for billing |
| **Adding Providers** | Standard: pure data change. Non-standard: implement 4-method adapter interface |
