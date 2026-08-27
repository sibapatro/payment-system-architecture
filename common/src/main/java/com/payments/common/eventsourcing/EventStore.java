package com.payments.common.eventsourcing;

import com.payments.common.events.DomainEvent;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * EVENT STORE — The Immutable Audit Trail
 *
 * Core principle: We NEVER update or delete events.
 * Every state change is appended as a new event.
 * To get current state → replay all events for that aggregate.
 *
 * In production: backed by PostgreSQL (JSONB) or EventStoreDB.
 * This in-memory implementation preserves the exact same contract.
 *
 * Guarantees:
 *  - Optimistic concurrency via expectedVersion (prevents lost updates)
 *  - Strict ordering via global sequence number
 *  - Snapshotting to avoid full replay for high-event aggregates
 */
public class EventStore {

    private final Map<String, List<StoredEvent>>  eventsByAggregate = new ConcurrentHashMap<>();
    private final List<StoredEvent>               globalLog         = new CopyOnWriteArrayList<>();
    private final Map<String, AggregateSnapshot> snapshots         = new ConcurrentHashMap<>();
    private final AtomicLong                      globalSequence    = new AtomicLong(0);

    private static final int SNAPSHOT_THRESHOLD = 50; // snapshot every 50 events

    // ─── Append ──────────────────────────────────────────────────────────────

    /**
     * Append events for an aggregate with optimistic concurrency control.
     *
     * @param aggregateId     the aggregate root ID
     * @param events          the new events to append
     * @param expectedVersion the version the caller believes the aggregate is at
     *                        (-1 = new aggregate, must not exist yet)
     */
    public synchronized void appendEvents(String aggregateId,
                                          List<DomainEvent> events,
                                          long expectedVersion) {
        List<StoredEvent> existing = eventsByAggregate
                .getOrDefault(aggregateId, Collections.emptyList());

        long currentVersion = existing.isEmpty() ? -1
                : existing.get(existing.size() - 1).getSequenceNumber();

        // Optimistic concurrency check — prevents double-charge scenarios
        if (currentVersion != expectedVersion) {
            throw new OptimisticConcurrencyException(
                    String.format("Concurrency conflict for aggregate %s: " +
                            "expected version %d but found %d. " +
                            "Another transaction may have modified this record.",
                            aggregateId, expectedVersion, currentVersion));
        }

        List<StoredEvent> aggregateEvents = eventsByAggregate
                .computeIfAbsent(aggregateId, k -> new CopyOnWriteArrayList<>());

        for (DomainEvent event : events) {
            StoredEvent stored = new StoredEvent(
                    event,
                    globalSequence.incrementAndGet(),
                    Instant.now()
            );
            aggregateEvents.add(stored);
            globalLog.add(stored);
        }

        // Auto-snapshot if event count crosses threshold
        if (aggregateEvents.size() % SNAPSHOT_THRESHOLD == 0) {
            triggerSnapshot(aggregateId, aggregateEvents);
        }
    }

    // ─── Load ────────────────────────────────────────────────────────────────

    /**
     * Load all events for an aggregate (full replay from beginning or snapshot).
     */
    public List<StoredEvent> loadEvents(String aggregateId) {
        return Collections.unmodifiableList(
                eventsByAggregate.getOrDefault(aggregateId, Collections.emptyList()));
    }

    /**
     * Load events after a specific sequence number (for catch-up projections).
     */
    public List<StoredEvent> loadEventsSince(String aggregateId, long afterSequence) {
        return eventsByAggregate
                .getOrDefault(aggregateId, Collections.emptyList())
                .stream()
                .filter(e -> e.getSequenceNumber() > afterSequence)
                .collect(Collectors.toList());
    }

    /**
     * Load all events across all aggregates in global order (for read-model rebuild).
     */
    public List<StoredEvent> loadAllEventsSince(long afterGlobalSequence) {
        return globalLog.stream()
                .filter(e -> e.getGlobalSequence() > afterGlobalSequence)
                .sorted(Comparator.comparingLong(StoredEvent::getGlobalSequence))
                .collect(Collectors.toList());
    }

    /**
     * Get the current version (last sequence number) of an aggregate.
     */
    public long getCurrentVersion(String aggregateId) {
        List<StoredEvent> events = eventsByAggregate
                .getOrDefault(aggregateId, Collections.emptyList());
        if (events.isEmpty()) return -1;
        return events.get(events.size() - 1).getSequenceNumber();
    }

    // ─── Snapshots ───────────────────────────────────────────────────────────

    public Optional<AggregateSnapshot> loadSnapshot(String aggregateId) {
        return Optional.ofNullable(snapshots.get(aggregateId));
    }

    public void saveSnapshot(AggregateSnapshot snapshot) {
        snapshots.put(snapshot.getAggregateId(), snapshot);
    }

    private void triggerSnapshot(String aggregateId, List<StoredEvent> events) {
        // Signal that a snapshot should be taken — actual state captured by aggregate
        long lastSeq = events.get(events.size() - 1).getSequenceNumber();
        System.out.printf("[EventStore] Snapshot threshold reached for %s at seq=%d%n",
                aggregateId, lastSeq);
    }

    // ─── Diagnostics ─────────────────────────────────────────────────────────

    public long getTotalEventCount()              { return globalLog.size(); }
    public long getGlobalSequence()               { return globalSequence.get(); }
    public Set<String> getAggregateIds()          { return eventsByAggregate.keySet(); }
    public int getEventCountFor(String aggId)     {
        return eventsByAggregate.getOrDefault(aggId, Collections.emptyList()).size();
    }

    // ─── Inner Types ─────────────────────────────────────────────────────────

    public static class StoredEvent {
        private final DomainEvent event;
        private final long        globalSequence;
        private final Instant     storedAt;

        public StoredEvent(DomainEvent event, long globalSequence, Instant storedAt) {
            this.event          = event;
            this.globalSequence = globalSequence;
            this.storedAt       = storedAt;
        }

        public DomainEvent getEvent()         { return event; }
        public long getGlobalSequence()       { return globalSequence; }
        public long getSequenceNumber()       { return event.getSequenceNumber(); }
        public String getAggregateId()        { return event.getAggregateId(); }
        public String getEventType()          { return event.getEventType(); }
        public Instant getStoredAt()          { return storedAt; }
    }

    public static class AggregateSnapshot {
        private final String  aggregateId;
        private final long    version;
        private final Object  state;
        private final Instant takenAt;

        public AggregateSnapshot(String aggregateId, long version,
                                 Object state, Instant takenAt) {
            this.aggregateId = aggregateId;
            this.version     = version;
            this.state       = state;
            this.takenAt     = takenAt;
        }

        public String getAggregateId()  { return aggregateId; }
        public long getVersion()        { return version; }
        public Object getState()        { return state; }
        public Instant getTakenAt()     { return takenAt; }
    }

    public static class OptimisticConcurrencyException extends RuntimeException {
        public OptimisticConcurrencyException(String message) {
            super(message);
        }
    }
}
