package com.payments.common.circuitbreaker;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * CIRCUIT BREAKER — Cascade Failure Prevention
 *
 * States:
 *  CLOSED   → Normal operation. Requests flow through.
 *  OPEN     → Failure threshold exceeded. Requests fail fast (no bank API call).
 *  HALF_OPEN → Trial period. One request let through to probe recovery.
 *
 * Example: External Bank API starts timing out.
 * Without circuit breaker → every payment thread hangs for 30s → OOM.
 * With circuit breaker    → after 3 failures, trip open → return fallback instantly.
 *
 * The bank gets breathing room; we stop hammering a dying service.
 */
public class CircuitBreaker {

    // ─── Configuration ────────────────────────────────────────────────────────
    private final String   name;
    private final int      failureThreshold;       // trip open after N failures
    private final int      successThresholdToClose;// consecutive successes to re-close
    private final Duration openDuration;           // how long to stay OPEN before HALF_OPEN
    private final Duration callTimeout;            // per-call timeout

    // ─── Runtime State ────────────────────────────────────────────────────────
    private final AtomicReference<State> state           = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger          failureCount     = new AtomicInteger(0);
    private final AtomicInteger          successCount     = new AtomicInteger(0);
    private final AtomicLong             lastFailureTime  = new AtomicLong(0);
    private final AtomicLong             totalCalls       = new AtomicLong(0);
    private final AtomicLong             rejectedCalls    = new AtomicLong(0);
    private final AtomicLong             successfulCalls  = new AtomicLong(0);
    private final AtomicLong             failedCalls      = new AtomicLong(0);

    public CircuitBreaker(String name, int failureThreshold,
                          int successThresholdToClose,
                          Duration openDuration, Duration callTimeout) {
        this.name                    = name;
        this.failureThreshold        = failureThreshold;
        this.successThresholdToClose = successThresholdToClose;
        this.openDuration            = openDuration;
        this.callTimeout             = callTimeout;
    }

    // ─── Core Execute ─────────────────────────────────────────────────────────

    /**
     * Execute a supplier through the circuit breaker.
     * Throws CircuitOpenException if the circuit is OPEN (fail fast).
     */
    public <T> T execute(Supplier<T> supplier) throws CircuitOpenException {
        totalCalls.incrementAndGet();

        State current = evaluateState();

        if (current == State.OPEN) {
            rejectedCalls.incrementAndGet();
            throw new CircuitOpenException(
                    String.format("Circuit '%s' is OPEN. " +
                            "Failing fast to protect downstream. " +
                            "Will retry after %s. [failures=%d]",
                            name, openDuration, failureCount.get()));
        }

        // CLOSED or HALF_OPEN: attempt the call
        try {
            T result = executeWithTimeout(supplier);
            onSuccess();
            return result;

        } catch (CircuitOpenException e) {
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException(e);
        } catch (Exception e) {
            onFailure(e);
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute with a fallback if the circuit is open or the call fails.
     */
    public <T> T executeWithFallback(Supplier<T> supplier, Supplier<T> fallback) {
        try {
            return execute(supplier);
        } catch (CircuitOpenException e) {
            System.out.printf("[CircuitBreaker:%s] Circuit OPEN → using fallback%n", name);
            return fallback.get();
        } catch (Exception e) {
            System.out.printf("[CircuitBreaker:%s] Call failed → using fallback: %s%n",
                    name, e.getMessage());
            return fallback.get();
        }
    }

    // ─── State Machine ────────────────────────────────────────────────────────

    private State evaluateState() {
        State current = state.get();

        if (current == State.OPEN) {
            long elapsed = System.currentTimeMillis() - lastFailureTime.get();
            if (elapsed > openDuration.toMillis()) {
                // Transition to HALF_OPEN to probe recovery
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    System.out.printf("[CircuitBreaker:%s] OPEN → HALF_OPEN (probe)%n", name);
                }
            }
        }

        return state.get();
    }

    private void onSuccess() {
        successfulCalls.incrementAndGet();
        failureCount.set(0); // reset rolling failure count

        State current = state.get();
        if (current == State.HALF_OPEN) {
            int successes = successCount.incrementAndGet();
            if (successes >= successThresholdToClose) {
                if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                    successCount.set(0);
                    System.out.printf("[CircuitBreaker:%s] HALF_OPEN → CLOSED (recovered)%n", name);
                }
            }
        }
    }

    private void onFailure(Exception cause) {
        failedCalls.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());
        successCount.set(0);

        int failures = failureCount.incrementAndGet();
        State current = state.get();

        if (current == State.HALF_OPEN) {
            // Probe failed → back to OPEN
            state.set(State.OPEN);
            System.out.printf("[CircuitBreaker:%s] HALF_OPEN → OPEN (probe failed: %s)%n",
                    name, cause.getMessage());

        } else if (current == State.CLOSED && failures >= failureThreshold) {
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                System.out.printf("[CircuitBreaker:%s] CLOSED → OPEN " +
                        "(threshold=%d reached, cause=%s)%n",
                        name, failureThreshold, cause.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T executeWithTimeout(Supplier<T> supplier) throws Exception {
        // In production: wrap with CompletableFuture.orTimeout()
        // Simplified here for clarity
        long startMs = System.currentTimeMillis();
        T result = supplier.get();
        long elapsedMs = System.currentTimeMillis() - startMs;

        if (elapsedMs > callTimeout.toMillis()) {
            throw new CallTimeoutException(
                    String.format("Call to '%s' timed out after %dms (limit: %dms)",
                            name, elapsedMs, callTimeout.toMillis()));
        }
        return result;
    }

    // ─── Manual Controls ──────────────────────────────────────────────────────

    public void forceOpen()   { state.set(State.OPEN);   System.out.printf("[CircuitBreaker:%s] Manually OPENED%n", name); }
    public void forceClose()  { state.set(State.CLOSED); failureCount.set(0); System.out.printf("[CircuitBreaker:%s] Manually CLOSED%n", name); }
    public void reset()       { state.set(State.CLOSED); failureCount.set(0); successCount.set(0); }

    // ─── Metrics ──────────────────────────────────────────────────────────────

    public CircuitBreakerMetrics getMetrics() {
        return new CircuitBreakerMetrics(
                name, state.get(), failureCount.get(),
                totalCalls.get(), rejectedCalls.get(),
                successfulCalls.get(), failedCalls.get(),
                lastFailureTime.get() > 0
                        ? Instant.ofEpochMilli(lastFailureTime.get()) : null
        );
    }

    public State getState()  { return state.get(); }
    public String getName()  { return name; }

    // ─── Types ────────────────────────────────────────────────────────────────

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public static class CircuitOpenException extends RuntimeException {
        public CircuitOpenException(String message) { super(message); }
    }

    public static class CallTimeoutException extends RuntimeException {
        public CallTimeoutException(String message) { super(message); }
    }

    public record CircuitBreakerMetrics(
            String  name,
            State   state,
            int     currentFailureCount,
            long    totalCalls,
            long    rejectedCalls,
            long    successfulCalls,
            long    failedCalls,
            Instant lastFailureAt
    ) {
        public double errorRate() {
            if (totalCalls == 0) return 0.0;
            return (double) failedCalls / totalCalls * 100.0;
        }

        @Override
        public String toString() {
            return String.format(
                    "CircuitBreaker[%s] state=%s failures=%d total=%d rejected=%d errorRate=%.1f%%",
                    name, state, currentFailureCount, totalCalls, rejectedCalls, errorRate());
        }
    }

    // ─── Builder ──────────────────────────────────────────────────────────────

    public static Builder builder(String name) { return new Builder(name); }

    public static class Builder {
        private final String name;
        private int      failureThreshold        = 5;
        private int      successThresholdToClose  = 2;
        private Duration openDuration            = Duration.ofSeconds(30);
        private Duration callTimeout             = Duration.ofSeconds(10);

        private Builder(String name) { this.name = name; }

        public Builder failureThreshold(int n)          { this.failureThreshold = n; return this; }
        public Builder successThreshold(int n)          { this.successThresholdToClose = n; return this; }
        public Builder openDuration(Duration d)         { this.openDuration = d; return this; }
        public Builder callTimeout(Duration d)          { this.callTimeout = d; return this; }

        public CircuitBreaker build() {
            return new CircuitBreaker(name, failureThreshold,
                    successThresholdToClose, openDuration, callTimeout);
        }
    }
}
