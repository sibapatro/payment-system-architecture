package com.payments.payment.service;

import com.payments.common.circuitbreaker.CircuitBreaker;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * BANK API CLIENT — External Integration with Circuit Breaker
 *
 * Wraps all calls to external bank APIs with:
 *  1. Circuit Breaker       → fail fast when bank is down
 *  2. Retry with backoff    → transient failures auto-recovered
 *  3. Timeout enforcement   → never hang longer than callTimeout
 *  4. Response normalization → translate bank codes to our domain model
 *
 * Supported banks are configured with independent circuit breakers
 * so that a failure at Bank A doesn't trip the breaker for Bank B.
 */
public class BankApiClient {

    // One circuit breaker per bank — isolated failure domains
    private final Map<String, CircuitBreaker> bankBreakers = new ConcurrentHashMap<>();

    private static final int    MAX_RETRIES       = 3;
    private static final long   INITIAL_BACKOFF_MS = 100;

    public BankApiClient() {
        // Register known banking partners
        registerBank("HDFC_BANK");
        registerBank("ICICI_BANK");
        registerBank("SBI");
        registerBank("AXIS_BANK");
        registerBank("DEFAULT");
    }

    private void registerBank(String bankCode) {
        bankBreakers.put(bankCode, CircuitBreaker.builder("BANK_API_" + bankCode)
                .failureThreshold(3)
                .successThreshold(2)
                .openDuration(Duration.ofSeconds(30))
                .callTimeout(Duration.ofSeconds(5))
                .build());
    }

    // ─── Authorization ────────────────────────────────────────────────────────

    /**
     * Request authorization from the bank for a payment.
     * This is the most critical external call — failure must be handled gracefully.
     */
    public AuthorizationResponse authorize(AuthorizationRequest request) {
        CircuitBreaker breaker = getBreakerForBank(request.bankCode());

        return breaker.executeWithFallback(
                () -> doAuthorizeWithRetry(request),
                () -> AuthorizationResponse.serviceUnavailable(
                        request.paymentId(), request.bankCode()));
    }

    private AuthorizationResponse doAuthorizeWithRetry(AuthorizationRequest request) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return callBankAuthorizationEndpoint(request);

            } catch (BankTransientException e) {
                lastException = e;
                System.out.printf(
                        "[BankApiClient] Transient failure attempt %d/%d for payment=%s: %s%n",
                        attempt, MAX_RETRIES, request.paymentId(), e.getMessage());

                if (attempt < MAX_RETRIES) {
                    backoff(attempt);
                }

            } catch (BankPermanentException e) {
                // No point retrying a permanent decline
                System.out.printf(
                        "[BankApiClient] Permanent failure for payment=%s: %s%n",
                        request.paymentId(), e.getMessage());
                return AuthorizationResponse.declined(
                        request.paymentId(), e.getDeclineCode(), e.getMessage());
            }
        }

        // All retries exhausted
        throw new RuntimeException(
                "Bank authorization failed after " + MAX_RETRIES + " attempts",
                lastException);
    }

    /**
     * Simulates the actual HTTP call to the bank's authorization endpoint.
     * Production: RestTemplate / WebClient with SSL pinning and mTLS.
     */
    private AuthorizationResponse callBankAuthorizationEndpoint(
            AuthorizationRequest request) throws BankTransientException, BankPermanentException {

        simulateNetworkLatency();

        // Simulate real-world bank response scenarios
        double outcome = ThreadLocalRandom.current().nextDouble();

        if (outcome < 0.01) {
            // 1% → bank server error (transient)
            throw new BankTransientException("Bank returned 503 Service Unavailable");
        }

        if (outcome < 0.03) {
            // 2% → network timeout (transient)
            throw new BankTransientException("Connection timed out after 5000ms");
        }

        if (outcome < 0.08) {
            // 5% → hard decline (permanent)
            throw new BankPermanentException("INSUFFICIENT_FUNDS",
                    "Account does not have sufficient funds");
        }

        if (outcome < 0.10) {
            // 2% → fraud hold (permanent)
            throw new BankPermanentException("FRAUD_HOLD",
                    "Account is under fraud investigation");
        }

        // 90% → approved
        String authCode = "AUTH-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String reference = "REF-" + request.bankCode() + "-" +
                System.currentTimeMillis();

        return AuthorizationResponse.approved(
                request.paymentId(), authCode, reference);
    }

    // ─── Settlement ───────────────────────────────────────────────────────────

    public SettlementResponse settle(SettlementRequest request) {
        CircuitBreaker breaker = getBreakerForBank(request.bankCode());

        return breaker.executeWithFallback(
                () -> callBankSettlementEndpoint(request),
                () -> SettlementResponse.queued(request.paymentId(),
                        "Settlement queued — bank temporarily unavailable")
        );
    }

    private SettlementResponse callBankSettlementEndpoint(SettlementRequest request) {
        simulateNetworkLatency();
        String settlementRef = "SETL-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
        return SettlementResponse.completed(request.paymentId(), settlementRef, request.amount());
    }

    // ─── Reversal ─────────────────────────────────────────────────────────────

    public ReversalResponse reverse(ReversalRequest request) {
        CircuitBreaker breaker = getBreakerForBank(request.bankCode());

        return breaker.executeWithFallback(
                () -> callBankReversalEndpoint(request),
                () -> ReversalResponse.queued(request.paymentId(),
                        "Reversal queued — will retry when bank recovers")
        );
    }

    private ReversalResponse callBankReversalEndpoint(ReversalRequest request) {
        simulateNetworkLatency();
        String reversalRef = "REV-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
        return ReversalResponse.completed(request.paymentId(), reversalRef);
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private CircuitBreaker getBreakerForBank(String bankCode) {
        return bankBreakers.getOrDefault(bankCode, bankBreakers.get("DEFAULT"));
    }

    private void simulateNetworkLatency() {
        try {
            // Bank APIs typically respond in 50-300ms
            Thread.sleep(50 + ThreadLocalRandom.current().nextLong(250));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void backoff(int attempt) {
        try {
            long delay = INITIAL_BACKOFF_MS * (long) Math.pow(2, attempt - 1);
            Thread.sleep(Math.min(delay, 2000)); // cap at 2s
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Map<String, CircuitBreaker.CircuitBreakerMetrics> getAllBreakerMetrics() {
        Map<String, CircuitBreaker.CircuitBreakerMetrics> result = new ConcurrentHashMap<>();
        bankBreakers.forEach((k, v) -> result.put(k, v.getMetrics()));
        return result;
    }

    // ─── Request / Response Types ─────────────────────────────────────────────

    public record AuthorizationRequest(
            String     paymentId,
            String     bankCode,
            String     sourceAccountId,
            String     destinationAccountId,
            BigDecimal amount,
            String     currency,
            String     merchantCategory
    ) {}

    public record AuthorizationResponse(
            String   paymentId,
            boolean  approved,
            String   authorizationCode,
            String   bankReference,
            String   declineCode,
            String   declineReason,
            ResponseStatus status
    ) {
        public static AuthorizationResponse approved(String paymentId,
                String authCode, String ref) {
            return new AuthorizationResponse(paymentId, true, authCode,
                    ref, null, null, ResponseStatus.APPROVED);
        }
        public static AuthorizationResponse declined(String paymentId,
                String declineCode, String reason) {
            return new AuthorizationResponse(paymentId, false, null,
                    null, declineCode, reason, ResponseStatus.DECLINED);
        }
        public static AuthorizationResponse serviceUnavailable(
                String paymentId, String bankCode) {
            return new AuthorizationResponse(paymentId, false, null, null,
                    "SERVICE_UNAVAILABLE",
                    "Bank " + bankCode + " is temporarily unavailable. Please retry later.",
                    ResponseStatus.SERVICE_UNAVAILABLE);
        }
    }

    public record SettlementRequest(
            String paymentId, String bankCode,
            String authorizationCode, BigDecimal amount, String currency) {}

    public record SettlementResponse(
            String paymentId, String settlementReference,
            BigDecimal settledAmount, ResponseStatus status, String message) {
        public static SettlementResponse completed(String pid, String ref, BigDecimal amt) {
            return new SettlementResponse(pid, ref, amt, ResponseStatus.APPROVED, "Settled");
        }
        public static SettlementResponse queued(String pid, String msg) {
            return new SettlementResponse(pid, null, null, ResponseStatus.QUEUED, msg);
        }
    }

    public record ReversalRequest(
            String paymentId, String bankCode,
            String originalAuthorizationCode, BigDecimal amount) {}

    public record ReversalResponse(
            String paymentId, String reversalReference,
            ResponseStatus status, String message) {
        public static ReversalResponse completed(String pid, String ref) {
            return new ReversalResponse(pid, ref, ResponseStatus.APPROVED, "Reversed");
        }
        public static ReversalResponse queued(String pid, String msg) {
            return new ReversalResponse(pid, null, ResponseStatus.QUEUED, msg);
        }
    }

    public enum ResponseStatus { APPROVED, DECLINED, SERVICE_UNAVAILABLE, QUEUED }

    // ─── Exceptions ───────────────────────────────────────────────────────────

    public static class BankTransientException extends Exception {
        public BankTransientException(String message) { super(message); }
    }

    public static class BankPermanentException extends Exception {
        private final String declineCode;
        public BankPermanentException(String declineCode, String message) {
            super(message);
            this.declineCode = declineCode;
        }
        public String getDeclineCode() { return declineCode; }
    }
}
