package com.payments.common.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IDEMPOTENCY REGISTRY — Prevent Double Charges
 *
 * Critical for payment systems: clients retry on network timeouts.
 * Without idempotency: retry → second charge → customer dispute → chargeback.
 *
 * With idempotency:
 *   1st call  → process payment, store result under idempotency key
 *   Retry     → look up key, return SAME result without reprocessing
 *
 * The idempotency key is client-generated (UUID) and sent in the header:
 *   Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
 *
 * Keys expire after 24 hours (configurable).
 * In production: backed by Redis with TTL.
 */
public class IdempotencyRegistry {

    private final ConcurrentHashMap<String, IdempotencyRecord> store
            = new ConcurrentHashMap<>();

    private final Duration keyTtl;

    public IdempotencyRegistry(Duration keyTtl) {
        this.keyTtl = keyTtl;
    }

    public static IdempotencyRegistry withDefaultTtl() {
        return new IdempotencyRegistry(Duration.ofHours(24));
    }

    // ─── Check / Reserve / Complete ──────────────────────────────────────────

    /**
     * Check if this idempotency key has already been used.
     * Returns the stored result if it exists.
     */
    public Optional<IdempotencyRecord> lookup(String idempotencyKey) {
        IdempotencyRecord record = store.get(idempotencyKey);
        if (record == null) return Optional.empty();

        // Expired keys are treated as absent
        if (isExpired(record)) {
            store.remove(idempotencyKey);
            return Optional.empty();
        }

        return Optional.of(record);
    }

    /**
     * Reserve an idempotency key for a payment being processed.
     * Atomically claims the key — concurrent duplicate requests get CONFLICT.
     *
     * @return true if reserved (first request), false if already reserved (duplicate)
     */
    public boolean reserve(String idempotencyKey, String paymentId) {
        IdempotencyRecord record = new IdempotencyRecord(
                idempotencyKey, paymentId, RecordState.PROCESSING,
                null, Instant.now(), null);

        return store.putIfAbsent(idempotencyKey, record) == null;
    }

    /**
     * Mark an idempotency key as complete with the final result.
     * Future lookups return this result instead of reprocessing.
     */
    public void complete(String idempotencyKey, String paymentId,
                         String resultPayload) {
        store.compute(idempotencyKey, (k, existing) -> {
            if (existing == null) {
                throw new IllegalStateException(
                        "Cannot complete unreserved idempotency key: " + k);
            }
            return new IdempotencyRecord(
                    k, paymentId, RecordState.COMPLETED,
                    resultPayload, existing.reservedAt(), Instant.now());
        });
    }

    /**
     * Mark as failed — client will need a NEW idempotency key to retry.
     * (We don't want to cache failures indefinitely, just long enough
     *  for the client to stop the retry storm.)
     */
    public void fail(String idempotencyKey, String paymentId, String errorPayload) {
        store.compute(idempotencyKey, (k, existing) -> {
            if (existing == null) return null;
            return new IdempotencyRecord(
                    k, paymentId, RecordState.FAILED,
                    errorPayload, existing.reservedAt(), Instant.now());
        });
    }

    private boolean isExpired(IdempotencyRecord record) {
        return record.reservedAt().plus(keyTtl).isBefore(Instant.now());
    }

    public int size()      { return store.size(); }

    public void evictExpired() {
        store.entrySet().removeIf(e -> isExpired(e.getValue()));
    }

    // ─── Types ────────────────────────────────────────────────────────────────

    public record IdempotencyRecord(
            String      idempotencyKey,
            String      paymentId,
            RecordState state,
            String      resultPayload,   // JSON of the original response
            Instant     reservedAt,
            Instant     completedAt
    ) {
        public boolean isProcessing() { return state == RecordState.PROCESSING; }
        public boolean isCompleted()  { return state == RecordState.COMPLETED; }
        public boolean isFailed()     { return state == RecordState.FAILED; }
    }

    public enum RecordState { PROCESSING, COMPLETED, FAILED }
}
