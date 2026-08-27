package com.payments.common.events;

import java.math.BigDecimal;
import java.util.Currency;

/**
 * All payment lifecycle events.
 * Event Sourcing guarantees: if we replay these events from the event store,
 * we can reconstruct any account/transaction state at any point in time.
 */
public final class PaymentEvents {

    // ─── Payment Initiated ───────────────────────────────────────────────────

    public static class PaymentInitiated extends DomainEvent {
        private final String paymentId;
        private final String sourceAccountId;
        private final String destinationAccountId;
        private final BigDecimal amount;
        private final Currency currency;
        private final String customerId;
        private final String idempotencyKey;

        public PaymentInitiated(String paymentId, String sourceAccountId,
                                String destinationAccountId, BigDecimal amount,
                                Currency currency, String customerId,
                                String idempotencyKey, long seq) {
            super(paymentId, "Payment", seq, "PaymentInitiated");
            this.paymentId            = paymentId;
            this.sourceAccountId      = sourceAccountId;
            this.destinationAccountId = destinationAccountId;
            this.amount               = amount;
            this.currency             = currency;
            this.customerId           = customerId;
            this.idempotencyKey       = idempotencyKey;
        }

        public String getPaymentId()            { return paymentId; }
        public String getSourceAccountId()      { return sourceAccountId; }
        public String getDestinationAccountId() { return destinationAccountId; }
        public BigDecimal getAmount()           { return amount; }
        public Currency getCurrency()           { return currency; }
        public String getCustomerId()           { return customerId; }
        public String getIdempotencyKey()       { return idempotencyKey; }
    }

    // ─── Fraud Check Events ──────────────────────────────────────────────────

    public static class FraudCheckPassed extends DomainEvent {
        private final String paymentId;
        private final double riskScore;
        private final String riskCategory;

        public FraudCheckPassed(String paymentId, double riskScore,
                                String riskCategory, long seq) {
            super(paymentId, "Payment", seq, "FraudCheckPassed");
            this.paymentId    = paymentId;
            this.riskScore    = riskScore;
            this.riskCategory = riskCategory;
        }

        public String getPaymentId()   { return paymentId; }
        public double getRiskScore()   { return riskScore; }
        public String getRiskCategory(){ return riskCategory; }
    }

    public static class FraudCheckFailed extends DomainEvent {
        private final String paymentId;
        private final double riskScore;
        private final String reason;

        public FraudCheckFailed(String paymentId, double riskScore,
                                String reason, long seq) {
            super(paymentId, "Payment", seq, "FraudCheckFailed");
            this.paymentId = paymentId;
            this.riskScore = riskScore;
            this.reason    = reason;
        }

        public String getPaymentId() { return paymentId; }
        public double getRiskScore() { return riskScore; }
        public String getReason()    { return reason; }
    }

    // ─── Bank Authorization Events ───────────────────────────────────────────

    public static class BankAuthorizationRequested extends DomainEvent {
        private final String paymentId;
        private final String bankReferenceId;

        public BankAuthorizationRequested(String paymentId,
                                          String bankReferenceId, long seq) {
            super(paymentId, "Payment", seq, "BankAuthorizationRequested");
            this.paymentId      = paymentId;
            this.bankReferenceId = bankReferenceId;
        }

        public String getPaymentId()      { return paymentId; }
        public String getBankReferenceId(){ return bankReferenceId; }
    }

    public static class BankAuthorizationApproved extends DomainEvent {
        private final String paymentId;
        private final String authorizationCode;

        public BankAuthorizationApproved(String paymentId,
                                         String authorizationCode, long seq) {
            super(paymentId, "Payment", seq, "BankAuthorizationApproved");
            this.paymentId         = paymentId;
            this.authorizationCode = authorizationCode;
        }

        public String getPaymentId()        { return paymentId; }
        public String getAuthorizationCode(){ return authorizationCode; }
    }

    public static class BankAuthorizationDeclined extends DomainEvent {
        private final String paymentId;
        private final String declineCode;
        private final String reason;

        public BankAuthorizationDeclined(String paymentId, String declineCode,
                                         String reason, long seq) {
            super(paymentId, "Payment", seq, "BankAuthorizationDeclined");
            this.paymentId  = paymentId;
            this.declineCode = declineCode;
            this.reason     = reason;
        }

        public String getPaymentId()  { return paymentId; }
        public String getDeclineCode(){ return declineCode; }
        public String getReason()     { return reason; }
    }

    // ─── Ledger Events ───────────────────────────────────────────────────────

    public static class LedgerEntryRecorded extends DomainEvent {
        private final String paymentId;
        private final String ledgerEntryId;
        private final BigDecimal debitAmount;
        private final BigDecimal creditAmount;

        public LedgerEntryRecorded(String paymentId, String ledgerEntryId,
                                   BigDecimal debitAmount, BigDecimal creditAmount,
                                   long seq) {
            super(paymentId, "Payment", seq, "LedgerEntryRecorded");
            this.paymentId    = paymentId;
            this.ledgerEntryId = ledgerEntryId;
            this.debitAmount  = debitAmount;
            this.creditAmount = creditAmount;
        }

        public String getPaymentId()    { return paymentId; }
        public String getLedgerEntryId(){ return ledgerEntryId; }
        public BigDecimal getDebitAmount() { return debitAmount; }
        public BigDecimal getCreditAmount(){ return creditAmount; }
    }

    // ─── Terminal States ─────────────────────────────────────────────────────

    public static class PaymentCompleted extends DomainEvent {
        private final String paymentId;
        private final BigDecimal settledAmount;
        private final String settlementReference;

        public PaymentCompleted(String paymentId, BigDecimal settledAmount,
                                String settlementReference, long seq) {
            super(paymentId, "Payment", seq, "PaymentCompleted");
            this.paymentId           = paymentId;
            this.settledAmount       = settledAmount;
            this.settlementReference = settlementReference;
        }

        public String getPaymentId()          { return paymentId; }
        public BigDecimal getSettledAmount()  { return settledAmount; }
        public String getSettlementReference(){ return settlementReference; }
    }

    public static class PaymentFailed extends DomainEvent {
        private final String paymentId;
        private final String failureReason;
        private final String failureCode;

        public PaymentFailed(String paymentId, String failureReason,
                             String failureCode, long seq) {
            super(paymentId, "Payment", seq, "PaymentFailed");
            this.paymentId     = paymentId;
            this.failureReason = failureReason;
            this.failureCode   = failureCode;
        }

        public String getPaymentId()    { return paymentId; }
        public String getFailureReason(){ return failureReason; }
        public String getFailureCode()  { return failureCode; }
    }

    // ─── Compensating Events (Saga rollback) ──────────────────────────────────

    public static class PaymentReversed extends DomainEvent {
        private final String originalPaymentId;
        private final String reversalId;
        private final String reversalReason;
        private final BigDecimal reversalAmount;

        public PaymentReversed(String originalPaymentId, String reversalId,
                               String reversalReason, BigDecimal reversalAmount,
                               long seq) {
            super(originalPaymentId, "Payment", seq, "PaymentReversed");
            this.originalPaymentId = originalPaymentId;
            this.reversalId        = reversalId;
            this.reversalReason    = reversalReason;
            this.reversalAmount    = reversalAmount;
        }

        public String getOriginalPaymentId() { return originalPaymentId; }
        public String getReversalId()        { return reversalId; }
        public String getReversalReason()    { return reversalReason; }
        public BigDecimal getReversalAmount(){ return reversalAmount; }
    }

    private PaymentEvents() {}
}
