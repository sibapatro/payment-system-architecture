package com.payments.payment.service;

import com.payments.common.bulkhead.BulkheadManager;
import com.payments.common.cqrs.PaymentReadModel;
import com.payments.common.domain.IdempotencyRegistry;
import com.payments.common.domain.PaymentAggregate;
import com.payments.common.eventsourcing.EventStore;
import com.payments.common.saga.PaymentSagaOrchestrator;
import com.payments.common.saga.PaymentSagaOrchestrator.SagaContext;
import com.payments.common.saga.PaymentSagaOrchestrator.SagaStep;
import com.payments.fraud.service.FraudDetectionService;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Currency;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * PAYMENT PROCESSING SERVICE — The Saga Conductor
 *
 * This is the heart of the system. It coordinates:
 *
 *   PaymentAggregate   → domain model + Event Sourcing
 *   SagaOrchestrator   → distributed transaction management
 *   FraudService       → risk evaluation (Bulkhead-isolated)
 *   BankApiClient      → external authorization (Circuit Breaker-protected)
 *   EventStore         → immutable audit trail
 *   ReadModel          → CQRS projection
 *   IdempotencyRegistry→ double-charge prevention
 *
 * Processing flow (Saga steps):
 *  1. Validate & reserve idempotency key
 *  2. Create PaymentAggregate, persist INITIATED event
 *  3. Submit to FRAUD_DETECTION bulkhead
 *  4. On fraud pass → submit BANK_AUTH to BANK_API_CALLS bulkhead
 *  5. On bank approval → record ledger entry
 *  6. Complete saga
 *  7. On any failure → compensate (reverse what succeeded)
 */
public class PaymentService {

    private final EventStore             eventStore;
    private final PaymentSagaOrchestrator sagaOrchestrator;
    private final FraudDetectionService  fraudService;
    private final BankApiClient          bankClient;
    private final LedgerService          ledgerService;
    private final PaymentReadModel       readModel;
    private final IdempotencyRegistry    idempotencyRegistry;
    private final BulkheadManager        bulkheads;

    public PaymentService(EventStore eventStore,
                          PaymentSagaOrchestrator sagaOrchestrator,
                          FraudDetectionService fraudService,
                          BankApiClient bankClient,
                          LedgerService ledgerService,
                          PaymentReadModel readModel,
                          IdempotencyRegistry idempotencyRegistry,
                          BulkheadManager bulkheads) {
        this.eventStore          = eventStore;
        this.sagaOrchestrator    = sagaOrchestrator;
        this.fraudService        = fraudService;
        this.bankClient          = bankClient;
        this.ledgerService       = ledgerService;
        this.readModel           = readModel;
        this.idempotencyRegistry = idempotencyRegistry;
        this.bulkheads           = bulkheads;
    }

    // ─── COMMAND: Initiate Payment ────────────────────────────────────────────

    public PaymentResult initiatePayment(PaymentRequest request) {

        // ── Step 0: Idempotency guard ──────────────────────────────────────
        Optional<IdempotencyRegistry.IdempotencyRecord> existing =
                idempotencyRegistry.lookup(request.idempotencyKey());

        if (existing.isPresent()) {
            IdempotencyRegistry.IdempotencyRecord record = existing.get();
            if (record.isProcessing()) {
                return PaymentResult.conflict(
                        "Payment with this idempotency key is already being processed. " +
                                "paymentId=" + record.paymentId());
            }
            // Replay the original result
            System.out.printf("[PaymentService] Idempotent replay for key=%s paymentId=%s%n",
                    request.idempotencyKey(), record.paymentId());
            return PaymentResult.idempotentReplay(record.paymentId(), record.resultPayload());
        }

        String paymentId = UUID.randomUUID().toString();

        if (!idempotencyRegistry.reserve(request.idempotencyKey(), paymentId)) {
            return PaymentResult.conflict("Concurrent duplicate request detected");
        }

        try {
            return processPayment(paymentId, request);
        } catch (Exception e) {
            idempotencyRegistry.fail(request.idempotencyKey(), paymentId, e.getMessage());
            throw e;
        }
    }

    // ─── SAGA Execution ───────────────────────────────────────────────────────

    private PaymentResult processPayment(String paymentId, PaymentRequest request) {

        // ── Step 1: Create aggregate & persist INITIATED event ─────────────
        PaymentAggregate payment = PaymentAggregate.initiate(
                paymentId,
                request.sourceAccountId(),
                request.destinationAccountId(),
                request.amount(),
                Currency.getInstance(request.currency()),
                request.customerId(),
                request.idempotencyKey()
        );

        persistEvents(payment);

        // ── Step 2: Start Saga ──────────────────────────────────────────────
        SagaContext context = new SagaContext(
                paymentId, request.customerId(),
                request.sourceAccountId(), request.destinationAccountId(),
                request.amount(), request.currency(), request.idempotencyKey(),
                Map.of("bankCode", request.bankCode())
        );

        PaymentSagaOrchestrator.SagaInstance saga = sagaOrchestrator.startSaga(paymentId, context);
        sagaOrchestrator.transitionTo(paymentId, SagaStep.FRAUD_CHECK_REQUESTED);

        // ── Step 3: Fraud Detection (Bulkhead isolated) ────────────────────
        FraudDetectionService.FraudEvaluationResult fraudResult;
        try {
            fraudResult = bulkheads.executeSync(
                    "FRAUD_DETECTION",
                    () -> fraudService.evaluate(new FraudDetectionService.FraudEvaluationRequest(
                            paymentId, request.customerId(),
                            request.sourceAccountId(), request.destinationAccountId(),
                            request.amount(), request.currency(),
                            request.ipAddress(), request.deviceFingerprint(),
                            request.merchantCategory()
                    )),
                    Duration.ofSeconds(3)
            );
        } catch (Exception e) {
            return compensateAndFail(payment, saga, "FRAUD_SERVICE_UNAVAILABLE",
                    "Fraud detection unavailable: " + e.getMessage(), request);
        }

        if (fraudResult.approved()) {
            payment.markFraudPassed(fraudResult.riskScore(),
                    fraudResult.riskCategory().name());
            sagaOrchestrator.transitionTo(paymentId, SagaStep.FRAUD_CLEARED);
        } else {
            payment.markFraudFailed(fraudResult.riskScore(), fraudResult.primaryReason());
            payment.fail(fraudResult.primaryReason(), "FRAUD_REJECTED");
            persistEvents(payment);
            sagaOrchestrator.transitionTo(paymentId, SagaStep.FRAUD_REJECTED);
            sagaOrchestrator.failSaga(paymentId, "Fraud rejected: " + fraudResult.primaryReason());

            String result = "REJECTED:FRAUD:" + fraudResult.primaryReason();
            idempotencyRegistry.complete(request.idempotencyKey(), paymentId, result);
            updateReadModel(payment);
            return PaymentResult.fraudRejected(paymentId, fraudResult.primaryReason(),
                    fraudResult.riskScore());
        }
        persistEvents(payment);

        // ── Step 4: Bank Authorization (Circuit Breaker protected) ─────────
        sagaOrchestrator.transitionTo(paymentId, SagaStep.BANK_AUTH_REQUESTED);

        BankApiClient.AuthorizationResponse authResponse;
        try {
            authResponse = bulkheads.executeSync(
                    "BANK_API_CALLS",
                    () -> bankClient.authorize(new BankApiClient.AuthorizationRequest(
                            paymentId, request.bankCode(),
                            request.sourceAccountId(), request.destinationAccountId(),
                            request.amount(), request.currency(),
                            request.merchantCategory()
                    )),
                    Duration.ofSeconds(10)
            );
        } catch (Exception e) {
            return compensateAndFail(payment, saga, "BANK_API_ERROR",
                    "Bank API call failed: " + e.getMessage(), request);
        }

        String bankRef = "BREF-" + UUID.randomUUID().toString().substring(0, 8);
        payment.requestBankAuthorization(bankRef);
        persistEvents(payment);

        if (authResponse.approved()) {
            payment.approveBankAuthorization(authResponse.authorizationCode());
            sagaOrchestrator.transitionTo(paymentId, SagaStep.BANK_AUTH_APPROVED);
        } else {
            payment.declineBankAuthorization(authResponse.declineCode(), authResponse.declineReason());
            payment.fail(authResponse.declineReason(), authResponse.declineCode());
            persistEvents(payment);
            sagaOrchestrator.transitionTo(paymentId, SagaStep.BANK_AUTH_DECLINED);
            sagaOrchestrator.failSaga(paymentId, "Bank declined: " + authResponse.declineCode());

            String result = "DECLINED:" + authResponse.declineCode();
            idempotencyRegistry.complete(request.idempotencyKey(), paymentId, result);
            updateReadModel(payment);
            return PaymentResult.bankDeclined(paymentId, authResponse.declineCode(),
                    authResponse.declineReason());
        }
        persistEvents(payment);

        // ── Step 5: Ledger Entry ────────────────────────────────────────────
        sagaOrchestrator.transitionTo(paymentId, SagaStep.LEDGER_ENTRY_REQUESTED);

        LedgerService.LedgerEntryResult ledgerResult;
        try {
            ledgerResult = bulkheads.executeSync(
                    "LEDGER_WRITES",
                    () -> ledgerService.recordDoubleEntry(
                            paymentId, request.sourceAccountId(),
                            request.destinationAccountId(), request.amount(),
                            authResponse.authorizationCode()
                    ),
                    Duration.ofSeconds(5)
            );
        } catch (Exception e) {
            // Ledger failed after bank approved → must reverse
            reverseAtBank(payment, authResponse.authorizationCode(), request.bankCode());
            return compensateAndFail(payment, saga, "LEDGER_FAILURE",
                    "Ledger entry failed: " + e.getMessage(), request);
        }

        payment.recordLedgerEntry(ledgerResult.ledgerEntryId(),
                request.amount(), request.amount());
        sagaOrchestrator.transitionTo(paymentId, SagaStep.LEDGER_RECORDED);
        persistEvents(payment);

        // ── Step 6: Complete ────────────────────────────────────────────────
        String settlementRef = "SETL-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
        payment.complete(request.amount(), settlementRef);
        persistEvents(payment);

        sagaOrchestrator.transitionTo(paymentId, SagaStep.COMPLETED);
        sagaOrchestrator.completeSaga(paymentId);

        updateReadModel(payment);
        String result = "COMPLETED:" + settlementRef;
        idempotencyRegistry.complete(request.idempotencyKey(), paymentId, result);

        System.out.printf("[PaymentService] ✅ Payment COMPLETED paymentId=%s amount=%s %s%n",
                paymentId, request.amount(), request.currency());

        return PaymentResult.success(paymentId, settlementRef, request.amount());
    }

    // ─── Saga Compensation ────────────────────────────────────────────────────

    private PaymentResult compensateAndFail(PaymentAggregate payment,
                                            PaymentSagaOrchestrator.SagaInstance saga,
                                            String code, String reason,
                                            PaymentRequest request) {
        System.out.printf("[PaymentService] ⚠️  Compensating payment=%s reason=%s%n",
                payment.getPaymentId(), reason);

        sagaOrchestrator.beginCompensation(payment.getPaymentId(), reason);

        try {
            payment.fail(reason, code);
            persistEvents(payment);
        } catch (IllegalStateException e) {
            // Already in terminal state — compensation recorded externally
        }

        updateReadModel(payment);
        sagaOrchestrator.compensationComplete(payment.getPaymentId());
        idempotencyRegistry.fail(request.idempotencyKey(), payment.getPaymentId(), reason);

        return PaymentResult.failed(payment.getPaymentId(), code, reason);
    }

    private void reverseAtBank(PaymentAggregate payment, String authCode, String bankCode) {
        try {
            bankClient.reverse(new BankApiClient.ReversalRequest(
                    payment.getPaymentId(), bankCode, authCode, payment.getAmount()));
            System.out.printf("[PaymentService] Bank reversal requested for payment=%s%n",
                    payment.getPaymentId());
        } catch (Exception e) {
            // Log to dead-letter queue for manual reconciliation
            System.err.printf("[PaymentService] ❌ BANK REVERSAL FAILED payment=%s — " +
                    "needs manual reconciliation: %s%n", payment.getPaymentId(), e.getMessage());
        }
    }

    // ─── Event Persistence ────────────────────────────────────────────────────

    private void persistEvents(PaymentAggregate payment) {
        if (payment.getPendingEvents().isEmpty()) return;

        long currentVersion = eventStore.getCurrentVersion(payment.getPaymentId());
        eventStore.appendEvents(
                payment.getPaymentId(),
                payment.getPendingEvents(),
                currentVersion
        );
        payment.clearPendingEvents();
    }

    // ─── CQRS Read Model Update ───────────────────────────────────────────────

    private void updateReadModel(PaymentAggregate payment) {
        readModel.upsert(new PaymentReadModel.PaymentView(
                payment.getPaymentId(),
                payment.getCustomerId(),
                payment.getSourceAccountId(),
                payment.getDestinationAccountId(),
                payment.getAmount(),
                payment.getCurrency().getCurrencyCode(),
                payment.getStatus(),
                payment.getRiskScore(),
                payment.getAuthorizationCode(),
                payment.getSettlementReference(),
                null,
                payment.getCreatedAt(),
                null,
                0L
        ));
    }

    // ─── QUERY: Get Payment ───────────────────────────────────────────────────

    public Optional<PaymentReadModel.PaymentView> getPayment(String paymentId) {
        return readModel.findByPaymentId(paymentId);
    }

    public java.util.List<PaymentReadModel.PaymentView> getCustomerPayments(String customerId) {
        return readModel.findByCustomer(customerId);
    }

    // ─── Request / Response ───────────────────────────────────────────────────

    public record PaymentRequest(
            String     customerId,
            String     sourceAccountId,
            String     destinationAccountId,
            BigDecimal amount,
            String     currency,
            String     bankCode,
            String     idempotencyKey,
            String     ipAddress,
            String     deviceFingerprint,
            String     merchantCategory
    ) {}

    public record PaymentResult(
            String        paymentId,
            ResultStatus  status,
            String        settlementReference,
            BigDecimal    amount,
            String        declineCode,
            String        message,
            boolean       idempotentReplay
    ) {
        public static PaymentResult success(String pid, String ref, BigDecimal amt) {
            return new PaymentResult(pid, ResultStatus.COMPLETED, ref, amt, null, "Payment completed", false);
        }
        public static PaymentResult fraudRejected(String pid, String reason, double score) {
            return new PaymentResult(pid, ResultStatus.FRAUD_REJECTED, null, null,
                    "FRAUD_REJECTED", String.format("Risk score %.2f: %s", score, reason), false);
        }
        public static PaymentResult bankDeclined(String pid, String code, String reason) {
            return new PaymentResult(pid, ResultStatus.BANK_DECLINED, null, null, code, reason, false);
        }
        public static PaymentResult failed(String pid, String code, String reason) {
            return new PaymentResult(pid, ResultStatus.FAILED, null, null, code, reason, false);
        }
        public static PaymentResult conflict(String message) {
            return new PaymentResult(null, ResultStatus.CONFLICT, null, null, "CONFLICT", message, false);
        }
        public static PaymentResult idempotentReplay(String pid, String payload) {
            return new PaymentResult(pid, ResultStatus.COMPLETED, null, null, null,
                    "Idempotent replay: " + payload, true);
        }
    }

    public enum ResultStatus { COMPLETED, FRAUD_REJECTED, BANK_DECLINED, FAILED, CONFLICT }
}
