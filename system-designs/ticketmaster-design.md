# Ticketmaster System Design

## Book Tickets Flow

### Overview
- Client initiates booking → Reserve tickets (DB transaction) + Redis TTL lock
- Create Stripe PaymentIntent with `booking_id` as idempotency key
- Client redirects to Stripe Checkout, polls status on return
- Payment confirmation via: Webhook (push) OR Client poll (pull) OR Reconciliation job (background)
- TTL expiry auto-releases unpaid reservations

### Booking States

```
INITIATED ──► RESERVED ──► PAYMENT_PENDING ──► CONFIRMED
                 │               │
                 │               │
                 ▼               ▼
              EXPIRED       PAYMENT_FAILED
                 │               │
                 └───────────────┴──► (release tickets)
```

### Flow

```
Client                    Your API                    Stripe              Background
  │                          │                          │                     │
  │  POST /bookings          │                          │                     │
  │─────────────────────────►│                          │                     │
  │                          │                          │                     │
  │                    ┌─────┴─────┐                    │                     │
  │                    │ Transaction│                   │                     │
  │                    │ • Booking  │                   │                     │
  │                    │   RESERVED │                   │                     │
  │                    │ • Tickets  │                   │                     │
  │                    │   LOCKED   │                   │                     │
  │                    │ • Redis TTL│                   │                     │
  │                    │   (10 min) │                   │                     │
  │                    └─────┬─────┘                    │                     │
  │                          │                          │                     │
  │                          │ Create PaymentIntent     │                     │
  │                          │ (idempotency_key=        │                     │
  │                          │  booking_{id})           │                     │
  │                          │─────────────────────────►│                     │
  │                          │◄─────────────────────────│                     │
  │                          │  checkout_url            │                     │
  │                          │                          │                     │
  │  {checkout_url}          │                          │                     │
  │◄─────────────────────────│                          │                     │
  │                          │                          │                     │
  │  Redirect to Stripe      │                          │                     │
  │─────────────────────────────────────────────────────►                     │
  │                          │                          │                     │
  │        ... user pays ... │                          │                     │
  │                          │                          │                     │
  │◄─────────────────────────────────────────────────────                     │
  │  Redirect back (success) │                          │                     │
  │                          │                          │                     │
  │  GET /bookings/{id}/status                          │                     │
  │─────────────────────────►│                          │                     │
  │                          │                          │                     │
  │                    ┌─────┴─────┐                    │                     │
  │                    │ status =  │                    │                     │
  │                    │ PENDING?  │                    │                     │
  │                    └─────┬─────┘                    │                     │
  │                          │                          │                     │
  │                          │  GET /payment_intents    │                     │
  │                          │─────────────────────────►│                     │
  │                          │◄─────────────────────────│                     │
  │                          │  {status: succeeded}     │                     │
  │                          │                          │                     │
  │                    ┌─────┴─────┐                    │                     │
  │                    │ Confirm   │                    │                     │
  │                    │ booking   │                    │                     │
  │                    │ Clear TTL │                    │                     │
  │                    └─────┬─────┘                    │                     │
  │                          │                          │                     │
  │  {status: CONFIRMED}     │                          │                     │
  │◄─────────────────────────│                          │                     │
  │                          │                          │                     │
  │                          │   Webhook: success       │                     │
  │                          │◄─────────────────────────│                     │
  │                          │   (already confirmed,    │                     │
  │                          │    idempotent - no-op)   │                     │
  │                          │                          │                     │
  │                          │                          │    ┌────────────────┤
  │                          │                          │    │ Every 5 min:   │
  │                          │                          │    │ Reconcile      │
  │                          │                          │    │ PENDING        │
  │                          │                          │    │ bookings       │
  │                          │                          │    │                │
  │                          │                          │    │ TTL expired?   │
  │                          │                          │    │ Release        │
  │                          │                          │    │ tickets        │
  │                          │                          │    └────────────────┘
```

### Key Design Decisions

1. **No Workflow Engine needed** - Simple two-step flow with retries; state machine + TTL + reconciliation is sufficient
2. **Redis TTL for reservation** - `SETNX ticket:{id}:lock {booking_id} EX 600` auto-expires unpaid reservations
3. **Idempotency key = booking_id** - Safe retries to Stripe, prevents duplicate charges
4. **Client polls AND checks Stripe** - Real-time confirmation even if webhook is delayed
5. **Webhook is idempotent** - If client poll already confirmed, webhook is a no-op
6. **Reconciliation job as fallback** - Catches missed webhooks, handles edge cases

### Payment Confirmation: Belt and Suspenders

| Mechanism | Type | Latency | When it catches payment |
|-----------|------|---------|------------------------|
| **Client Poll → Stripe Check** | Pull | Real-time | When user returns to site |
| **Webhook** | Push | ~1-5s | Immediately after payment |
| **Reconciliation Job** | Background | 5 min | Catches everything else |

### Status Endpoint Logic

```
GET /bookings/{booking_id}/status

1. Fetch booking from DB

2. If status == CONFIRMED:
     return {status: CONFIRMED, tickets: [...]}

3. If status == PAYMENT_PENDING:
     payment = stripe.PaymentIntent.retrieve(booking.stripe_payment_id)
     
     if payment.status == 'succeeded':
         booking.status = CONFIRMED  # Webhook was slow
         clear_redis_ttl(booking)
         return {status: CONFIRMED, tickets: [...]}
     
     if payment.status in ['canceled', 'requires_payment_method']:
         booking.status = FAILED
         release_tickets(booking)
         return {status: FAILED}

4. return {status: PENDING}  # Still waiting
```

### Client Polling

```javascript
// After redirect back from Stripe
async function pollBookingStatus(bookingId) {
    const maxAttempts = 30;  // 30 attempts
    const interval = 2000;   // 2 seconds
    
    for (let i = 0; i < maxAttempts; i++) {
        const { status, tickets } = await fetch(`/bookings/${bookingId}/status`);
        
        if (status === 'CONFIRMED') return showSuccess(tickets);
        if (status === 'FAILED') return showError();
        
        await sleep(interval);  // Still PENDING
    }
    
    showTimeout("We're processing your payment...");
}
```

### Why No Workflow Engine?

| Aspect | This Flow | When You'd Need Workflow Engine |
|--------|-----------|--------------------------------|
| Steps | 2 (reserve → pay) | 5+ with complex branching |
| Duration | Minutes | Days/weeks (approvals, etc.) |
| Recovery | Retry + TTL timeout | Complex compensation logic |
| Visibility | Status field sufficient | Need full execution history |

Workflow engines (Temporal, Cadence, Step Functions) are overkill here. The combination of state machine + idempotent ops + TTL + reconciliation is exactly what production booking systems use.
