package com.payments.payment.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LEDGER SERVICE — Double-Entry Bookkeeping
 *
 * Every financial transaction produces TWO ledger entries:
 *   DEBIT  → source account      (money leaving)
 *   CREDIT → destination account (money arriving)
 *
 * This is the foundation of accounting integrity.
 * DEBIT total must always equal CREDIT total — if it doesn't,
 * we have a bug that could result in money creation or destruction.
 *
 * In production: backed by a dedicated ledger database
 * with strict ACID guarantees and append-only tables.
 */
public class LedgerService {

    private final Map<String, List<LedgerEntry>> entriesByAccount  = new ConcurrentHashMap<>();
    private final Map<String, LedgerEntry>       entriesById       = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal>         balances          = new ConcurrentHashMap<>();
    private final AtomicLong                      entrySequence     = new AtomicLong(0);

    // Integrity check: total debits must equal total credits
    private volatile BigDecimal totalDebits  = BigDecimal.ZERO;
    private volatile BigDecimal totalCredits = BigDecimal.ZERO;

    // ─── Double-Entry Recording ───────────────────────────────────────────────

    public synchronized LedgerEntryResult recordDoubleEntry(String paymentId,
                                                            String sourceAccountId,
                                                            String destinationAccountId,
                                                            BigDecimal amount,
                                                            String authorizationCode) {
        validateAccounts(sourceAccountId, destinationAccountId, amount);

        String entryId = "LDG-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
        Instant now    = Instant.now();

        // DEBIT: source account (money leaving)
        LedgerEntry debitEntry = new LedgerEntry(
                entryId + "-D", paymentId, sourceAccountId,
                EntryType.DEBIT, amount, authorizationCode, now,
                entrySequence.incrementAndGet());

        // CREDIT: destination account (money arriving)
        LedgerEntry creditEntry = new LedgerEntry(
                entryId + "-C", paymentId, destinationAccountId,
                EntryType.CREDIT, amount, authorizationCode, now,
                entrySequence.incrementAndGet());

        // Persist entries
        persist(debitEntry);
        persist(creditEntry);

        // Update running balances
        balances.merge(sourceAccountId, amount.negate(), BigDecimal::add);
        balances.merge(destinationAccountId, amount, BigDecimal::add);

        // Integrity tracking
        totalDebits  = totalDebits.add(amount);
        totalCredits = totalCredits.add(amount);

        verifyIntegrity();

        System.out.printf(
                "[Ledger] Recorded DEBIT %s from %s | CREDIT %s to %s | paymentId=%s%n",
                amount, sourceAccountId, amount, destinationAccountId, paymentId);

        return new LedgerEntryResult(entryId, paymentId, debitEntry, creditEntry,
                balances.get(sourceAccountId), balances.get(destinationAccountId));
    }

    /**
     * Reverse a ledger entry (compensating transaction in Saga).
     * Creates a new pair of entries — NEVER modifies existing ones.
     */
    public synchronized LedgerEntryResult reverseEntry(String originalPaymentId,
                                                       String reversalPaymentId,
                                                       String sourceAccountId,
                                                       String destinationAccountId,
                                                       BigDecimal amount) {
        String reversalId = "REV-LDG-" + UUID.randomUUID().toString().substring(0, 10).toUpperCase();
        Instant now       = Instant.now();

        // Reversal: CREDIT back to source (refund), DEBIT from destination
        LedgerEntry creditBack = new LedgerEntry(
                reversalId + "-CR", reversalPaymentId, sourceAccountId,
                EntryType.CREDIT, amount,
                "REVERSAL-OF-" + originalPaymentId, now,
                entrySequence.incrementAndGet());

        LedgerEntry debitBack = new LedgerEntry(
                reversalId + "-DB", reversalPaymentId, destinationAccountId,
                EntryType.DEBIT, amount,
                "REVERSAL-OF-" + originalPaymentId, now,
                entrySequence.incrementAndGet());

        persist(creditBack);
        persist(debitBack);

        balances.merge(sourceAccountId, amount, BigDecimal::add);
        balances.merge(destinationAccountId, amount.negate(), BigDecimal::add);

        totalDebits  = totalDebits.add(amount);
        totalCredits = totalCredits.add(amount);

        verifyIntegrity();

        System.out.printf("[Ledger] Reversal recorded for original paymentId=%s%n",
                originalPaymentId);

        return new LedgerEntryResult(reversalId, reversalPaymentId, debitBack, creditBack,
                balances.get(sourceAccountId), balances.get(destinationAccountId));
    }

    // ─── Queries ──────────────────────────────────────────────────────────────

    public Optional<BigDecimal> getBalance(String accountId) {
        return Optional.ofNullable(balances.get(accountId));
    }

    public List<LedgerEntry> getEntriesForAccount(String accountId) {
        return List.copyOf(
                entriesByAccount.getOrDefault(accountId, Collections.emptyList()));
    }

    public List<LedgerEntry> getEntriesForPayment(String paymentId) {
        return entriesById.values().stream()
                .filter(e -> e.paymentId().equals(paymentId))
                .sorted(Comparator.comparingLong(LedgerEntry::sequence))
                .toList();
    }

    // ─── Integrity ────────────────────────────────────────────────────────────

    public LedgerIntegritySummary getIntegritySummary() {
        return new LedgerIntegritySummary(
                totalDebits, totalCredits,
                totalDebits.compareTo(totalCredits) == 0,
                entriesById.size(), balances.size());
    }

    private void verifyIntegrity() {
        if (totalDebits.compareTo(totalCredits) != 0) {
            throw new LedgerIntegrityViolationException(
                    String.format("INTEGRITY VIOLATION: totalDebits=%s ≠ totalCredits=%s. " +
                            "IMMEDIATE INVESTIGATION REQUIRED.", totalDebits, totalCredits));
        }
    }

    private void validateAccounts(String source, String dest, BigDecimal amount) {
        if (source.equals(dest)) {
            throw new IllegalArgumentException("Source and destination accounts must differ");
        }
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Ledger amount must be positive");
        }
    }

    private void persist(LedgerEntry entry) {
        entriesById.put(entry.entryId(), entry);
        entriesByAccount.computeIfAbsent(entry.accountId(),
                k -> Collections.synchronizedList(new ArrayList<>())).add(entry);
    }

    // ─── Types ────────────────────────────────────────────────────────────────

    public record LedgerEntry(
            String     entryId,
            String     paymentId,
            String     accountId,
            EntryType  entryType,
            BigDecimal amount,
            String     reference,
            Instant    recordedAt,
            long       sequence
    ) {}

    public record LedgerEntryResult(
            String      ledgerEntryId,
            String      paymentId,
            LedgerEntry debitEntry,
            LedgerEntry creditEntry,
            BigDecimal  sourceBalance,
            BigDecimal  destinationBalance
    ) {}

    public record LedgerIntegritySummary(
            BigDecimal totalDebits,
            BigDecimal totalCredits,
            boolean    balanced,
            int        totalEntries,
            int        totalAccounts
    ) {}

    public enum EntryType { DEBIT, CREDIT }

    public static class LedgerIntegrityViolationException extends RuntimeException {
        public LedgerIntegrityViolationException(String message) { super(message); }
    }
}
