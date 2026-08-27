package com.payments.common.cqrs;

import com.payments.common.domain.PaymentAggregate.PaymentStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * CQRS — Command Query Responsibility Segregation
 *
 * Problem: Dashboard queries joining 10M rows crush the transactional DB.
 * Every "SELECT *" during peak hours degrades write performance.
 *
 * Solution:
 *  WRITE SIDE → Append-only event log. Fast. No complex joins. 
 *               Handles 10,000 TPS without reads touching it.
 *
 *  READ SIDE  → Denormalized, pre-aggregated projections.
 *               Rebuilt asynchronously from events.
 *               Complex queries run against a read-optimized store.
 *
 * Eventual consistency: the read model may be ~100ms behind the write model.
 * That is acceptable for reporting. It is NOT acceptable for balance checks
 * (those should go through the aggregate directly).
 */
public class PaymentReadModel {

    // ─── In-memory read store (production: Redis + Elasticsearch) ────────────
    private final Map<String, PaymentView>            byPaymentId   = new ConcurrentHashMap<>();
    private final Map<String, List<PaymentView>>      byCustomerId  = new ConcurrentHashMap<>();
    private final Map<PaymentStatus, Long>            byStatus      = new ConcurrentHashMap<>();
    private final Map<String, DailyStats>             dailyStats    = new ConcurrentHashMap<>();

    // Projection checkpoint — last global event seq we've processed
    private volatile long lastProcessedSeq = 0L;

    // ─── Projections (called by event handlers) ───────────────────────────────

    /**
     * Upsert a payment view whenever the aggregate emits an event.
     * This is the "write to the read model" half of CQRS.
     */
    public void upsert(PaymentView view) {
        byPaymentId.put(view.paymentId(), view);

        byCustomerId.computeIfAbsent(view.customerId(),
                k -> Collections.synchronizedList(new ArrayList<>()));
        List<PaymentView> customerPayments = byCustomerId.get(view.customerId());
        customerPayments.removeIf(p -> p.paymentId().equals(view.paymentId()));
        customerPayments.add(view);

        // Update status counters
        byStatus.merge(view.status(), 1L, Long::sum);

        // Update daily stats
        String dateKey = view.initiatedAt().toString().substring(0, 10);
        dailyStats.merge(dateKey,
                new DailyStats(dateKey, 1, view.amount(), 0, BigDecimal.ZERO),
                DailyStats::merge);
    }

    // ─── Queries (the "Read" side) ────────────────────────────────────────────

    public Optional<PaymentView> findByPaymentId(String paymentId) {
        return Optional.ofNullable(byPaymentId.get(paymentId));
    }

    public List<PaymentView> findByCustomer(String customerId) {
        return List.copyOf(
                byCustomerId.getOrDefault(customerId, Collections.emptyList()));
    }

    public List<PaymentView> findByCustomerAndStatus(String customerId,
                                                     PaymentStatus status) {
        return byCustomerId.getOrDefault(customerId, Collections.emptyList())
                .stream()
                .filter(p -> p.status() == status)
                .sorted(Comparator.comparing(PaymentView::initiatedAt).reversed())
                .collect(Collectors.toList());
    }

    public List<PaymentView> findRecentPayments(int limit) {
        return byPaymentId.values().stream()
                .sorted(Comparator.comparing(PaymentView::initiatedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    public List<PaymentView> findLargePayments(BigDecimal threshold) {
        return byPaymentId.values().stream()
                .filter(p -> p.amount().compareTo(threshold) > 0)
                .sorted(Comparator.comparing(PaymentView::amount).reversed())
                .collect(Collectors.toList());
    }

    public StatusSummary getStatusSummary() {
        return new StatusSummary(Map.copyOf(byStatus));
    }

    public Optional<DailyStats> getDailyStats(String date) {
        return Optional.ofNullable(dailyStats.get(date));
    }

    public List<DailyStats> getDailyStatsRange(String fromDate, String toDate) {
        return dailyStats.values().stream()
                .filter(s -> s.date().compareTo(fromDate) >= 0
                          && s.date().compareTo(toDate)   <= 0)
                .sorted(Comparator.comparing(DailyStats::date))
                .collect(Collectors.toList());
    }

    public long getTotalPaymentCount()  { return byPaymentId.size(); }
    public long getLastProcessedSeq()   { return lastProcessedSeq; }
    public void setLastProcessedSeq(long seq) { this.lastProcessedSeq = seq; }

    // ─── Read Model Types ─────────────────────────────────────────────────────

    /**
     * Denormalized payment view — everything a dashboard needs in one record.
     * No joins required. Pre-computed for fast reads.
     */
    public record PaymentView(
            String        paymentId,
            String        customerId,
            String        sourceAccountId,
            String        destinationAccountId,
            BigDecimal    amount,
            String        currency,
            PaymentStatus status,
            double        riskScore,
            String        authorizationCode,
            String        settlementReference,
            String        failureReason,
            Instant       initiatedAt,
            Instant       completedAt,
            long          processingTimeMs
    ) {}

    public record StatusSummary(Map<PaymentStatus, Long> countByStatus) {
        public long total() {
            return countByStatus.values().stream().mapToLong(Long::longValue).sum();
        }
        public double successRate() {
            long completed = countByStatus.getOrDefault(PaymentStatus.COMPLETED, 0L);
            long total = total();
            return total == 0 ? 0.0 : (double) completed / total * 100.0;
        }
    }

    public record DailyStats(
            String     date,
            long       transactionCount,
            BigDecimal totalVolume,
            long       failureCount,
            BigDecimal failedVolume
    ) {
        public static DailyStats merge(DailyStats a, DailyStats b) {
            return new DailyStats(
                    a.date(),
                    a.transactionCount() + b.transactionCount(),
                    a.totalVolume().add(b.totalVolume()),
                    a.failureCount()  + b.failureCount(),
                    a.failedVolume().add(b.failedVolume())
            );
        }

        public double averageTransactionSize() {
            return transactionCount == 0 ? 0.0
                    : totalVolume.doubleValue() / transactionCount;
        }
    }
}
