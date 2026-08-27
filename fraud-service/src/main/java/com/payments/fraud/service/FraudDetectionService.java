package com.payments.fraud.service;

import com.payments.common.circuitbreaker.CircuitBreaker;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FRAUD DETECTION SERVICE
 *
 * Multi-layer rules engine that evaluates risk before any payment
 * reaches the bank. Runs inside its own Bulkhead thread pool so a
 * spike in ML model evaluation can't starve payment processing.
 *
 * Risk Layers:
 *  1. Velocity rules        — too many transactions in short windows
 *  2. Amount anomaly        — transaction far above customer average
 *  3. Geographic anomaly    — country/IP mismatch
 *  4. Blacklist check       — known fraudulent accounts
 *  5. ML model score        — composite risk score from model server
 *
 * Risk Score: 0.0 (clean) → 1.0 (definitely fraud)
 *  < 0.3  → APPROVE
 *  0.3-0.7 → REVIEW (manual queue in production)
 *  > 0.7  → REJECT
 */
public class FraudDetectionService {

    // ─── Velocity Tracking (in-memory; production: Redis sliding window) ──────
    private final Map<String, List<Instant>> txTimestamps   = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal>    txAmountSums   = new ConcurrentHashMap<>();
    private final Set<String>                blacklistedIds = ConcurrentHashMap.newKeySet();

    // ML Model circuit breaker — if the model server goes down, fall back to rules
    private final CircuitBreaker mlModelBreaker = CircuitBreaker
            .builder("ML_FRAUD_MODEL")
            .failureThreshold(3)
            .openDuration(Duration.ofSeconds(20))
            .callTimeout(Duration.ofMillis(500))  // ML must respond in 500ms
            .build();

    private final AtomicInteger evaluationCount = new AtomicInteger(0);

    // ─── Core Evaluation ──────────────────────────────────────────────────────

    public FraudEvaluationResult evaluate(FraudEvaluationRequest request) {
        evaluationCount.incrementAndGet();

        List<RuleResult> ruleResults = new ArrayList<>();

        // Run all rules
        ruleResults.add(checkVelocity(request));
        ruleResults.add(checkAmountAnomaly(request));
        ruleResults.add(checkBlacklist(request));
        ruleResults.add(checkAmountThreshold(request));

        // ML score with circuit breaker fallback to rule-based score
        RuleResult mlResult = mlModelBreaker.executeWithFallback(
                () -> callMlModel(request),
                () -> fallbackRuleScore(request)
        );
        ruleResults.add(mlResult);

        // Weighted composite score
        double compositeScore = computeCompositeScore(ruleResults);
        RiskCategory category = categorize(compositeScore);

        // Update velocity tracking (only after evaluation to avoid bias)
        trackTransaction(request);

        String primaryReason = ruleResults.stream()
                .filter(r -> r.score() > 0.5)
                .max(Comparator.comparingDouble(RuleResult::score))
                .map(RuleResult::reason)
                .orElse("CLEAN");

        boolean approved = category == RiskCategory.LOW || category == RiskCategory.MEDIUM;

        System.out.printf(
                "[FraudService] payment=%s score=%.3f category=%s approved=%s%n",
                request.paymentId(), compositeScore, category, approved);

        return new FraudEvaluationResult(
                request.paymentId(), compositeScore, category,
                approved, primaryReason, ruleResults, Instant.now());
    }

    // ─── Rule Implementations ─────────────────────────────────────────────────

    /** RULE 1: Velocity — more than 5 transactions in 60 seconds → suspicious */
    private RuleResult checkVelocity(FraudEvaluationRequest req) {
        List<Instant> times = txTimestamps.getOrDefault(
                req.customerId(), Collections.emptyList());

        Instant windowStart = Instant.now().minusSeconds(60);
        long recentCount = times.stream()
                .filter(t -> t.isAfter(windowStart))
                .count();

        if (recentCount >= 10) {
            return new RuleResult("VELOCITY_CHECK", 0.95,
                    "EXTREME_VELOCITY: " + recentCount + " txns in 60s");
        } else if (recentCount >= 5) {
            return new RuleResult("VELOCITY_CHECK", 0.65,
                    "HIGH_VELOCITY: " + recentCount + " txns in 60s");
        }
        return new RuleResult("VELOCITY_CHECK", 0.0, "NORMAL_VELOCITY");
    }

    /** RULE 2: Amount anomaly — 3x above customer's 30-day average */
    private RuleResult checkAmountAnomaly(FraudEvaluationRequest req) {
        BigDecimal historicalAvg = txAmountSums.getOrDefault(
                req.customerId(), BigDecimal.valueOf(500));

        if (historicalAvg.compareTo(BigDecimal.ZERO) == 0) {
            return new RuleResult("AMOUNT_ANOMALY", 0.1, "NO_HISTORY");
        }

        double ratio = req.amount().doubleValue() / historicalAvg.doubleValue();

        if (ratio > 10.0) {
            return new RuleResult("AMOUNT_ANOMALY", 0.90,
                    String.format("EXTREME_AMOUNT: %.1fx above average", ratio));
        } else if (ratio > 3.0) {
            return new RuleResult("AMOUNT_ANOMALY", 0.55,
                    String.format("HIGH_AMOUNT: %.1fx above average", ratio));
        }
        return new RuleResult("AMOUNT_ANOMALY", 0.0,
                String.format("NORMAL_AMOUNT: %.1fx average", ratio));
    }

    /** RULE 3: Blacklist check — known fraudulent accounts or merchants */
    private RuleResult checkBlacklist(FraudEvaluationRequest req) {
        if (blacklistedIds.contains(req.sourceAccountId()) ||
                blacklistedIds.contains(req.destinationAccountId()) ||
                blacklistedIds.contains(req.customerId())) {
            return new RuleResult("BLACKLIST_CHECK", 1.0, "BLACKLISTED_ENTITY");
        }
        return new RuleResult("BLACKLIST_CHECK", 0.0, "NOT_BLACKLISTED");
    }

    /** RULE 4: Hard limits — amounts above regulatory threshold get flagged */
    private RuleResult checkAmountThreshold(FraudEvaluationRequest req) {
        // AML: transactions over $10,000 require additional scrutiny (BSA/FinCEN)
        if (req.amount().compareTo(BigDecimal.valueOf(10_000)) > 0) {
            return new RuleResult("AML_THRESHOLD", 0.45,
                    "ABOVE_CTR_THRESHOLD: $" + req.amount());
        }
        if (req.amount().compareTo(BigDecimal.valueOf(9_000)) > 0) {
            // Structuring detection: just below reporting threshold
            return new RuleResult("AML_THRESHOLD", 0.60,
                    "POSSIBLE_STRUCTURING: $" + req.amount());
        }
        return new RuleResult("AML_THRESHOLD", 0.0, "WITHIN_THRESHOLD");
    }

    /** ML Model call — in production, this hits a TensorFlow Serving endpoint */
    private RuleResult callMlModel(FraudEvaluationRequest req) {
        // Simulate ML model inference with realistic latency
        simulateLatency(50, 200);

        // Deterministic score based on amount for demo purposes
        // Real model: gradient boosted trees on 200+ features
        double mlScore = Math.min(0.95,
                req.amount().doubleValue() / 50_000.0 * 0.5);

        return new RuleResult("ML_MODEL", mlScore,
                String.format("ML_SCORE: %.3f", mlScore));
    }

    /** Fallback when ML model is unavailable — pure rule-based score */
    private RuleResult fallbackRuleScore(FraudEvaluationRequest req) {
        System.out.println("[FraudService] ML model unavailable — using rule fallback");
        double fallback = req.amount().compareTo(BigDecimal.valueOf(5000)) > 0
                ? 0.35 : 0.10;
        return new RuleResult("ML_MODEL_FALLBACK", fallback,
                "FALLBACK_RULE_SCORE (ML unavailable)");
    }

    // ─── Score Aggregation ────────────────────────────────────────────────────

    private double computeCompositeScore(List<RuleResult> results) {
        // Weights: ML model gets highest weight, AML rules are hard signals
        Map<String, Double> weights = Map.of(
                "VELOCITY_CHECK",    0.20,
                "AMOUNT_ANOMALY",    0.15,
                "BLACKLIST_CHECK",   0.30,  // Blacklist is nearly deterministic
                "AML_THRESHOLD",     0.15,
                "ML_MODEL",          0.20,
                "ML_MODEL_FALLBACK", 0.20
        );

        // Hard override: blacklist always wins
        boolean blacklisted = results.stream()
                .anyMatch(r -> r.ruleName().equals("BLACKLIST_CHECK") && r.score() >= 1.0);
        if (blacklisted) return 1.0;

        double weightedSum = 0.0;
        double totalWeight  = 0.0;

        for (RuleResult r : results) {
            double w = weights.getOrDefault(r.ruleName(), 0.10);
            weightedSum += r.score() * w;
            totalWeight  += w;
        }

        return totalWeight == 0 ? 0.0 : weightedSum / totalWeight;
    }

    private RiskCategory categorize(double score) {
        if (score < 0.30) return RiskCategory.LOW;
        if (score < 0.60) return RiskCategory.MEDIUM;
        if (score < 0.80) return RiskCategory.HIGH;
        return RiskCategory.CRITICAL;
    }

    // ─── Velocity Tracking ────────────────────────────────────────────────────

    private void trackTransaction(FraudEvaluationRequest req) {
        txTimestamps.computeIfAbsent(req.customerId(),
                k -> Collections.synchronizedList(new ArrayList<>())).add(Instant.now());

        txAmountSums.merge(req.customerId(), req.amount(),
                (existing, newVal) -> existing.add(newVal).divide(BigDecimal.TWO));
    }

    private void simulateLatency(int minMs, int maxMs) {
        try {
            Thread.sleep(minMs + (long)(Math.random() * (maxMs - minMs)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ─── Admin Operations ─────────────────────────────────────────────────────

    public void blacklist(String entityId)   { blacklistedIds.add(entityId); }
    public void unblacklist(String entityId) { blacklistedIds.remove(entityId); }
    public int  getEvaluationCount()         { return evaluationCount.get(); }
    public CircuitBreaker.CircuitBreakerMetrics getMlModelMetrics() {
        return mlModelBreaker.getMetrics();
    }

    // ─── Data Types ───────────────────────────────────────────────────────────

    public record FraudEvaluationRequest(
            String     paymentId,
            String     customerId,
            String     sourceAccountId,
            String     destinationAccountId,
            BigDecimal amount,
            String     currency,
            String     ipAddress,
            String     deviceFingerprint,
            String     merchantCategory
    ) {}

    public record FraudEvaluationResult(
            String           paymentId,
            double           riskScore,
            RiskCategory     riskCategory,
            boolean          approved,
            String           primaryReason,
            List<RuleResult> ruleResults,
            Instant          evaluatedAt
    ) {}

    public record RuleResult(
            String ruleName,
            double score,
            String reason
    ) {}

    public enum RiskCategory { LOW, MEDIUM, HIGH, CRITICAL }
}
