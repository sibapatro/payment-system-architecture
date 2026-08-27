# $1B Payment System — Architectural Blueprint

> **Production-grade microservices backend in Java 21**  
> Implementing 7 critical patterns for resilience, consistency, and scale at 10,000 TPS.

---

## Architecture Overview

```
Client → API Gateway → [Payment Service ←→ Fraud Service]
                              ↕                  ↕
                       Bank API Client     Event Store
                              ↕                  ↕
                       Ledger Service      Read Model (CQRS)
```

## 7 Patterns Implemented

| Pattern | Class | Problem Solved |
|---|---|---|
| **API Gateway** | `ApiGateway.java` | Single entry point: rate limiting, auth, routing |
| **Saga Orchestrator** | `PaymentSagaOrchestrator.java` | Distributed transactions with compensating rollback |
| **CQRS** | `PaymentReadModel.java` | Separate read/write models — dashboards don't slow writes |
| **Event Sourcing** | `EventStore.java` + `PaymentAggregate.java` | Immutable audit trail, full state replay |
| **Circuit Breaker** | `CircuitBreaker.java` | Fail-fast on bank timeouts, CLOSED→OPEN→HALF_OPEN |
| **Bulkhead** | `BulkheadManager.java` | Isolated thread pools — fraud spike can't starve payments |
| **Sidecar / Service Mesh** | `ApiGateway.java` | Correlation IDs, mTLS-ready, distributed tracing hooks |

---

## Module Structure

```
payment-system/
├── common/                          # Shared domain kernel
│   └── src/main/java/com/payments/common/
│       ├── events/                  # DomainEvent, PaymentEvents (10 typed events)
│       ├── eventsourcing/           # EventStore (append-only, optimistic concurrency)
│       ├── domain/                  # PaymentAggregate, IdempotencyRegistry
│       ├── saga/                    # PaymentSagaOrchestrator (state machine)
│       ├── circuitbreaker/          # CircuitBreaker (CLOSED/OPEN/HALF_OPEN)
│       ├── bulkhead/                # BulkheadManager (5 isolated thread pools)
│       └── cqrs/                    # PaymentReadModel (denormalized projections)
│
├── api-gateway/                     # API Gateway service
│   └── ApiGateway.java              # Token bucket rate limiter + JWT auth
│
├── fraud-service/                   # Fraud Detection service
│   └── FraudDetectionService.java   # 5-layer rules engine + ML circuit breaker
│
├── payment-service/                 # Core orchestration service
│   ├── PaymentService.java          # Saga coordinator — wires all patterns
│   ├── BankApiClient.java           # Per-bank circuit breakers + retry backoff
│   ├── LedgerService.java           # Double-entry bookkeeping + integrity checks
│   └── PaymentSystemDemo.java       # End-to-end integration demo (7 scenarios)
│
└── notification-service/            # (stub — extend for SMS/email/webhook)
```

---

## Payment Lifecycle (Saga Steps)

```
INITIATED → FRAUD_CHECK_REQUESTED → FRAUD_CLEARED → BANK_AUTH_REQUESTED
         → BANK_AUTH_APPROVED → LEDGER_ENTRY_REQUESTED → LEDGER_RECORDED → COMPLETED

Failure paths (with compensation):
  FRAUD_REJECTED      → saga compensates → no money moved
  BANK_AUTH_DECLINED  → saga compensates → fraud hold released
  LEDGER_FAILURE      → saga compensates → bank reversal triggered
```

---

## Event Sourcing — Audit Trail

Every state change is an **immutable event** appended to the `EventStore`.  
Current state is derived by **replaying events** — no direct DB mutations.

```
PaymentInitiated        seq=0
FraudCheckPassed        seq=1
BankAuthorizationRequested seq=2
BankAuthorizationApproved  seq=3
LedgerEntryRecorded     seq=4
PaymentCompleted        seq=5
```

---

## Circuit Breaker — Bank API Protection

```
CLOSED  → Normal. Requests flow through.
  ↓ (3 failures)
OPEN    → Fail fast in <10ms. No bank call made. Returns fallback.
  ↓ (after 30s)
HALF_OPEN → Probe: one request let through.
  ↓ (success)     ↓ (failure)
CLOSED           OPEN (back to protection)
```

---

## Bulkhead Thread Pools

| Pool | Threads | Queue | Protects |
|---|---|---|---|
| `PAYMENT_PROCESSING` | 100 | 200 | Core payment flow |
| `FRAUD_DETECTION` | 50 | 100 | ML model evaluation |
| `BANK_API_CALLS` | 20 | 40 | External bank latency |
| `LEDGER_WRITES` | 30 | 60 | DB write consistency |
| `NOTIFICATIONS` | 10 | 500 | Async delivery |

---

## Running the Demo

```bash
# Compile (Java 21 required)
find . -name "*.java" | xargs javac --release 21 -d build/classes

# Run all 7 integration scenarios
java -cp build/classes com.payments.PaymentSystemDemo
```

**Scenarios covered:**
- ✅ A: Happy path — full lifecycle, ledger balanced
- ✅ B: Fraud rejection — blacklist hit, saga compensates, no ledger entry
- ✅ C: Bank decline — fraud cleared, bank says no, saga rolls back
- ✅ D: Idempotency — duplicate request returns same paymentId, no double charge
- ✅ E: Circuit breaker — trips OPEN in 3 failures, fail-fast in 4ms, recovers
- ✅ F: Event sourcing — full state rebuilt from events alone
- ✅ G: Concurrent load — 50 payments × 10 threads, ledger stays balanced

---

## Key Design Decisions

**Why Saga over 2PC?**  
Two-phase commit requires a distributed lock across all participants. At 10,000 TPS this becomes a bottleneck. Sagas use local transactions with compensating actions — each service owns its own data.

**Why Event Sourcing?**  
In payments, "what happened" matters as much as "what is". The event log is the source of truth. If the DB crashes, replay the events. If you need to audit a dispute from 6 months ago, replay to that point in time.

**Why CQRS?**  
At scale, read patterns (dashboards, reports) and write patterns (transaction processing) have completely different requirements. Separating them lets each side scale and optimise independently.

**Why per-bank Circuit Breakers?**  
HDFC going down shouldn't trip the ICICI breaker. One breaker per bank isolates failure domains so a single partner outage doesn't cascade.

---

## Tech Stack

- **Java 21** — Records, sealed classes, pattern matching, virtual threads-ready
- **Zero runtime dependencies** — pure JDK, no Spring/Quarkus required to understand the patterns
- **Production mappings**: EventStore → PostgreSQL/EventStoreDB, ReadModel → Redis + Elasticsearch, Bulkhead → Spring Cloud + Resilience4j, Gateway → Spring Cloud Gateway

---

*Built as an architectural reference for BFSI domain engineers.*  
*Author: Siba Prasad Patro — Principal Applications Engineer, Oracle Financial Services Software*
