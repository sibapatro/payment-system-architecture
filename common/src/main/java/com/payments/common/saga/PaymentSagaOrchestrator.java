package com.payments.common.saga;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SAGA ORCHESTRATOR — Distributed Transaction Manager
 *
 * Problem: A payment spans multiple services (Fraud, Bank, Ledger).
 * No single database transaction can span all of them.
 *
 * Solution: The Saga pattern breaks the transaction into a sequence
 * of local transactions, each publishing an event or message.
 * If any step fails → compensating transactions run in reverse.
 *
 *  Happy Path:
 *   INIT → FRAUD_CHECK → BANK_AUTH → LEDGER_ENTRY → COMPLETE
 *
 *  Fraud fails:
 *   INIT → FRAUD_CHECK → [FRAUD_REJECT] → COMPENSATE → FAILED
 *
 *  Bank declines after fraud cleared:
 *   INIT → FRAUD_CHECK → BANK_AUTH → [BANK_DECLINE]
 *        → REVERSE_FRAUD_HOLD → FAILED
 */
public class PaymentSagaOrchestrator {

    private final Map<String, SagaInstance> activeSagas = new ConcurrentHashMap<>();
    private final List<SagaEventListener>   listeners   = new ArrayList<>();

    // ─── Saga Lifecycle ───────────────────────────────────────────────────────

    public SagaInstance startSaga(String paymentId, SagaContext context) {
        SagaInstance saga = new SagaInstance(paymentId, context);
        activeSagas.put(paymentId, saga);
        notifyListeners(saga, SagaEvent.STARTED);

        System.out.printf("[SAGA] Started for payment=%s%n", paymentId);
        return saga;
    }

    public SagaInstance getSaga(String paymentId) {
        return activeSagas.get(paymentId);
    }

    public void transitionTo(String paymentId, SagaStep step) {
        SagaInstance saga = requireSaga(paymentId);
        saga.transitionTo(step);
        notifyListeners(saga, SagaEvent.STEP_COMPLETED);
        System.out.printf("[SAGA] payment=%s → %s%n", paymentId, step);
    }

    public void completeSaga(String paymentId) {
        SagaInstance saga = requireSaga(paymentId);
        saga.complete();
        notifyListeners(saga, SagaEvent.COMPLETED);
        activeSagas.remove(paymentId);
        System.out.printf("[SAGA] Completed for payment=%s%n", paymentId);
    }

    public void failSaga(String paymentId, String reason) {
        SagaInstance saga = requireSaga(paymentId);
        saga.fail(reason);
        notifyListeners(saga, SagaEvent.FAILED);
        System.out.printf("[SAGA] Failed for payment=%s reason=%s%n", paymentId, reason);
    }

    public void beginCompensation(String paymentId, String reason) {
        SagaInstance saga = requireSaga(paymentId);
        saga.beginCompensation(reason);
        notifyListeners(saga, SagaEvent.COMPENSATING);
        System.out.printf("[SAGA] Compensation started for payment=%s%n", paymentId);
    }

    public void compensationComplete(String paymentId) {
        SagaInstance saga = requireSaga(paymentId);
        saga.compensationComplete();
        notifyListeners(saga, SagaEvent.COMPENSATION_COMPLETE);
        activeSagas.remove(paymentId);
        System.out.printf("[SAGA] Compensation complete for payment=%s%n", paymentId);
    }

    private SagaInstance requireSaga(String paymentId) {
        SagaInstance saga = activeSagas.get(paymentId);
        if (saga == null) {
            throw new IllegalStateException("No active saga for payment: " + paymentId);
        }
        return saga;
    }

    public void addListener(SagaEventListener listener) {
        listeners.add(listener);
    }

    private void notifyListeners(SagaInstance saga, SagaEvent event) {
        listeners.forEach(l -> l.onSagaEvent(saga, event));
    }

    // ─── Saga Instance ────────────────────────────────────────────────────────

    public static class SagaInstance {
        private final String        sagaId;
        private final String        paymentId;
        private final SagaContext   context;
        private final List<SagaStepRecord> history = new ArrayList<>();
        private       SagaStep      currentStep;
        private       SagaState     state;
        private       String        failureReason;
        private final Instant       startedAt;
        private       Instant       endedAt;

        public SagaInstance(String paymentId, SagaContext context) {
            this.sagaId    = UUID.randomUUID().toString();
            this.paymentId = paymentId;
            this.context   = context;
            this.currentStep = SagaStep.INITIATED;
            this.state     = SagaState.RUNNING;
            this.startedAt = Instant.now();
            recordStep(SagaStep.INITIATED, "Saga initiated");
        }

        public void transitionTo(SagaStep step) {
            validateTransition(currentStep, step);
            recordStep(step, "Step completed");
            this.currentStep = step;
        }

        public void complete() {
            this.state   = SagaState.COMPLETED;
            this.endedAt = Instant.now();
            recordStep(SagaStep.COMPLETED, "Saga completed successfully");
        }

        public void fail(String reason) {
            this.state         = SagaState.FAILED;
            this.failureReason = reason;
            this.endedAt       = Instant.now();
            recordStep(SagaStep.FAILED, "Saga failed: " + reason);
        }

        public void beginCompensation(String reason) {
            this.state         = SagaState.COMPENSATING;
            this.failureReason = reason;
            recordStep(SagaStep.COMPENSATING, "Compensation started: " + reason);
        }

        public void compensationComplete() {
            this.state   = SagaState.COMPENSATED;
            this.endedAt = Instant.now();
            recordStep(SagaStep.COMPENSATED, "Compensation complete");
        }

        private void recordStep(SagaStep step, String description) {
            history.add(new SagaStepRecord(step, description, Instant.now()));
        }

        private void validateTransition(SagaStep from, SagaStep to) {
            // Define legal transitions
            Map<SagaStep, Set<SagaStep>> legal = Map.of(
                    SagaStep.INITIATED,      Set.of(SagaStep.FRAUD_CHECK_REQUESTED),
                    SagaStep.FRAUD_CHECK_REQUESTED, Set.of(SagaStep.FRAUD_CLEARED, SagaStep.FRAUD_REJECTED),
                    SagaStep.FRAUD_CLEARED,  Set.of(SagaStep.BANK_AUTH_REQUESTED),
                    SagaStep.BANK_AUTH_REQUESTED, Set.of(SagaStep.BANK_AUTH_APPROVED, SagaStep.BANK_AUTH_DECLINED),
                    SagaStep.BANK_AUTH_APPROVED, Set.of(SagaStep.LEDGER_ENTRY_REQUESTED),
                    SagaStep.LEDGER_ENTRY_REQUESTED, Set.of(SagaStep.LEDGER_RECORDED),
                    SagaStep.LEDGER_RECORDED, Set.of(SagaStep.COMPLETED),
                    SagaStep.COMPENSATING,   Set.of(SagaStep.COMPENSATED)
            );

            Set<SagaStep> allowed = legal.getOrDefault(from, Set.of());
            if (!allowed.contains(to)) {
                throw new IllegalStateException(
                        String.format("Invalid saga transition: %s → %s", from, to));
            }
        }

        // Getters
        public String getSagaId()                       { return sagaId; }
        public String getPaymentId()                    { return paymentId; }
        public SagaContext getContext()                 { return context; }
        public SagaStep getCurrentStep()                { return currentStep; }
        public SagaState getState()                     { return state; }
        public String getFailureReason()                { return failureReason; }
        public Instant getStartedAt()                   { return startedAt; }
        public Instant getEndedAt()                     { return endedAt; }
        public List<SagaStepRecord> getHistory()        { return Collections.unmodifiableList(history); }
        public boolean isRunning()                      { return state == SagaState.RUNNING; }
        public boolean isCompensating()                 { return state == SagaState.COMPENSATING; }
        public boolean isTerminal() {
            return state == SagaState.COMPLETED
                    || state == SagaState.FAILED
                    || state == SagaState.COMPENSATED;
        }
    }

    // ─── Supporting Types ─────────────────────────────────────────────────────

    public record SagaContext(
            String  paymentId,
            String  customerId,
            String  sourceAccountId,
            String  destinationAccountId,
            java.math.BigDecimal amount,
            String  currency,
            String  idempotencyKey,
            Map<String, Object> metadata
    ) {}

    public record SagaStepRecord(
            SagaStep step,
            String   description,
            Instant  timestamp
    ) {}

    public enum SagaStep {
        INITIATED,
        FRAUD_CHECK_REQUESTED,
        FRAUD_CLEARED,
        FRAUD_REJECTED,
        BANK_AUTH_REQUESTED,
        BANK_AUTH_APPROVED,
        BANK_AUTH_DECLINED,
        LEDGER_ENTRY_REQUESTED,
        LEDGER_RECORDED,
        COMPLETED,
        FAILED,
        COMPENSATING,
        COMPENSATED
    }

    public enum SagaState {
        RUNNING,
        COMPLETED,
        FAILED,
        COMPENSATING,
        COMPENSATED
    }

    public enum SagaEvent {
        STARTED, STEP_COMPLETED, COMPLETED,
        FAILED, COMPENSATING, COMPENSATION_COMPLETE
    }

    @FunctionalInterface
    public interface SagaEventListener {
        void onSagaEvent(SagaInstance saga, SagaEvent event);
    }
}
