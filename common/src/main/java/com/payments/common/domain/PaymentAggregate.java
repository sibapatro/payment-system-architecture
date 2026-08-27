package com.payments.common.domain;

import com.payments.common.events.*;
import com.payments.common.eventsourcing.EventStore;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;

/**
 * PAYMENT AGGREGATE ROOT
 *
 * Implements Event Sourcing: state is NOT stored directly.
 * It is rebuilt by replaying domain events in sequence.
 *
 * Key invariants enforced here:
 *  - A payment cannot be processed twice (idempotency)
 *  - State transitions are strictly ordered
 *  - All changes emit events before being applied (no silent mutations)
 */
public class PaymentAggregate {

    // ─── Identity ────────────────────────────────────────────────────────────
    private String    paymentId;
    private String    idempotencyKey;
    private String    customerId;

    // ─── Financial ───────────────────────────────────────────────────────────
    private String    sourceAccountId;
    private String    destinationAccountId;
    private BigDecimal amount;
    private Currency  currency;

    // ─── State Machine ───────────────────────────────────────────────────────
    private PaymentStatus status;
    private long          version;          // used for optimistic concurrency
    private Instant       createdAt;
    private Instant       updatedAt;

    // ─── Audit fields ────────────────────────────────────────────────────────
    private double  riskScore;
    private String  authorizationCode;
    private String  declineCode;
    private String  failureReason;
    private String  settlementReference;
    private String  ledgerEntryId;

    // Uncommitted events staged for persistence
    private final List<DomainEvent> pendingEvents = new ArrayList<>();

    private PaymentAggregate() { this.version = -1; }

    // ─── Factory: new payment ────────────────────────────────────────────────

    public static PaymentAggregate initiate(String paymentId,
                                            String sourceAccountId,
                                            String destinationAccountId,
                                            BigDecimal amount,
                                            Currency currency,
                                            String customerId,
                                            String idempotencyKey) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Payment amount must be positive");
        }

        PaymentAggregate agg = new PaymentAggregate();
        agg.raiseEvent(new PaymentEvents.PaymentInitiated(
                paymentId, sourceAccountId, destinationAccountId,
                amount, currency, customerId, idempotencyKey, 0L));
        return agg;
    }

    // ─── Factory: reconstitute from event store ──────────────────────────────

    public static PaymentAggregate reconstitute(
            String aggregateId, EventStore store) {

        List<EventStore.StoredEvent> stored = store.loadEvents(aggregateId);
        if (stored.isEmpty()) {
            throw new IllegalArgumentException(
                    "No events found for payment: " + aggregateId);
        }

        PaymentAggregate agg = new PaymentAggregate();
        for (EventStore.StoredEvent se : stored) {
            agg.apply(se.getEvent());
            agg.version = se.getSequenceNumber();
        }
        return agg;
    }

    // ─── Business Commands ───────────────────────────────────────────────────

    public void markFraudPassed(double riskScore, String riskCategory) {
        ensureStatus(PaymentStatus.INITIATED,
                "Fraud check can only run on INITIATED payments");
        raiseEvent(new PaymentEvents.FraudCheckPassed(
                paymentId, riskScore, riskCategory, nextSeq()));
    }

    public void markFraudFailed(double riskScore, String reason) {
        ensureStatus(PaymentStatus.INITIATED,
                "Fraud check can only run on INITIATED payments");
        raiseEvent(new PaymentEvents.FraudCheckFailed(
                paymentId, riskScore, reason, nextSeq()));
    }

    public void requestBankAuthorization(String bankReferenceId) {
        ensureStatus(PaymentStatus.FRAUD_CLEARED,
                "Bank auth can only be requested after fraud clearance");
        raiseEvent(new PaymentEvents.BankAuthorizationRequested(
                paymentId, bankReferenceId, nextSeq()));
    }

    public void approveBankAuthorization(String authCode) {
        ensureStatus(PaymentStatus.BANK_AUTH_PENDING,
                "Cannot approve: bank auth not in pending state");
        raiseEvent(new PaymentEvents.BankAuthorizationApproved(
                paymentId, authCode, nextSeq()));
    }

    public void declineBankAuthorization(String declineCode, String reason) {
        ensureStatus(PaymentStatus.BANK_AUTH_PENDING,
                "Cannot decline: bank auth not in pending state");
        raiseEvent(new PaymentEvents.BankAuthorizationDeclined(
                paymentId, declineCode, reason, nextSeq()));
    }

    public void recordLedgerEntry(String ledgerEntryId,
                                  BigDecimal debitAmt, BigDecimal creditAmt) {
        ensureStatus(PaymentStatus.BANK_AUTH_APPROVED,
                "Ledger entry requires bank authorization approval");
        raiseEvent(new PaymentEvents.LedgerEntryRecorded(
                paymentId, ledgerEntryId, debitAmt, creditAmt, nextSeq()));
    }

    public void complete(BigDecimal settledAmount, String settlementRef) {
        ensureStatus(PaymentStatus.LEDGER_RECORDED,
                "Payment can only complete after ledger entry");
        raiseEvent(new PaymentEvents.PaymentCompleted(
                paymentId, settledAmount, settlementRef, nextSeq()));
    }

    public void fail(String reason, String code) {
        if (status == PaymentStatus.COMPLETED || status == PaymentStatus.REVERSED) {
            throw new IllegalStateException("Cannot fail a terminal payment");
        }
        raiseEvent(new PaymentEvents.PaymentFailed(
                paymentId, reason, code, nextSeq()));
    }

    public void reverse(String reversalId, String reason) {
        ensureStatus(PaymentStatus.COMPLETED,
                "Only completed payments can be reversed");
        raiseEvent(new PaymentEvents.PaymentReversed(
                paymentId, reversalId, reason, amount, nextSeq()));
    }

    // ─── Event Application (rebuilds state) ──────────────────────────────────

    private void apply(DomainEvent event) {
        updatedAt = Instant.now();

        if (event instanceof PaymentEvents.PaymentInitiated e) {
            this.paymentId            = e.getPaymentId();
            this.sourceAccountId      = e.getSourceAccountId();
            this.destinationAccountId = e.getDestinationAccountId();
            this.amount               = e.getAmount();
            this.currency             = e.getCurrency();
            this.customerId           = e.getCustomerId();
            this.idempotencyKey       = e.getIdempotencyKey();
            this.status               = PaymentStatus.INITIATED;
            this.createdAt            = Instant.now();

        } else if (event instanceof PaymentEvents.FraudCheckPassed e) {
            this.riskScore = e.getRiskScore();
            this.status    = PaymentStatus.FRAUD_CLEARED;

        } else if (event instanceof PaymentEvents.FraudCheckFailed) {
            this.status = PaymentStatus.FRAUD_REJECTED;

        } else if (event instanceof PaymentEvents.BankAuthorizationRequested) {
            this.status = PaymentStatus.BANK_AUTH_PENDING;

        } else if (event instanceof PaymentEvents.BankAuthorizationApproved e) {
            this.authorizationCode = e.getAuthorizationCode();
            this.status            = PaymentStatus.BANK_AUTH_APPROVED;

        } else if (event instanceof PaymentEvents.BankAuthorizationDeclined e) {
            this.declineCode = e.getDeclineCode();
            this.status      = PaymentStatus.BANK_AUTH_DECLINED;

        } else if (event instanceof PaymentEvents.LedgerEntryRecorded e) {
            this.ledgerEntryId = e.getLedgerEntryId();
            this.status        = PaymentStatus.LEDGER_RECORDED;

        } else if (event instanceof PaymentEvents.PaymentCompleted e) {
            this.settlementReference = e.getSettlementReference();
            this.status              = PaymentStatus.COMPLETED;

        } else if (event instanceof PaymentEvents.PaymentFailed e) {
            this.failureReason = e.getFailureReason();
            this.status        = PaymentStatus.FAILED;

        } else if (event instanceof PaymentEvents.PaymentReversed) {
            this.status = PaymentStatus.REVERSED;
        }
    }

    // ─── Event Infrastructure ─────────────────────────────────────────────────

    private void raiseEvent(DomainEvent event) {
        apply(event);           // apply immediately so invariants see new state
        pendingEvents.add(event);
    }

    public List<DomainEvent> getPendingEvents() {
        return List.copyOf(pendingEvents);
    }

    public void clearPendingEvents() {
        pendingEvents.clear();
    }

    private long nextSeq() {
        return pendingEvents.size() + (version == -1 ? 0 : version) + 1;
    }

    private void ensureStatus(PaymentStatus required, String message) {
        if (this.status != required) {
            throw new IllegalStateException(
                    message + " (current: " + status + ", required: " + required + ")");
        }
    }

    // ─── Getters ─────────────────────────────────────────────────────────────

    public String      getPaymentId()            { return paymentId; }
    public String      getIdempotencyKey()        { return idempotencyKey; }
    public String      getCustomerId()            { return customerId; }
    public String      getSourceAccountId()       { return sourceAccountId; }
    public String      getDestinationAccountId()  { return destinationAccountId; }
    public BigDecimal  getAmount()                { return amount; }
    public Currency    getCurrency()              { return currency; }
    public PaymentStatus getStatus()             { return status; }
    public long        getVersion()              { return version; }
    public Instant     getCreatedAt()            { return createdAt; }
    public double      getRiskScore()            { return riskScore; }
    public String      getAuthorizationCode()    { return authorizationCode; }
    public String      getSettlementReference()  { return settlementReference; }
    public String      getLedgerEntryId()        { return ledgerEntryId; }
    public boolean     isTerminal() {
        return status == PaymentStatus.COMPLETED
                || status == PaymentStatus.FAILED
                || status == PaymentStatus.REVERSED
                || status == PaymentStatus.FRAUD_REJECTED
                || status == PaymentStatus.BANK_AUTH_DECLINED;
    }

    // ─── Payment State Machine ────────────────────────────────────────────────

    public enum PaymentStatus {
        INITIATED,
        FRAUD_CLEARED,
        FRAUD_REJECTED,
        BANK_AUTH_PENDING,
        BANK_AUTH_APPROVED,
        BANK_AUTH_DECLINED,
        LEDGER_RECORDED,
        COMPLETED,
        FAILED,
        REVERSED
    }
}
