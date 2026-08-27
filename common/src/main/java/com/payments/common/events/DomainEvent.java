package com.payments.common.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Base class for all domain events in the Event Sourcing pattern.
 * Every state change in the system is captured as an immutable event.
 * The current state of any aggregate is derived by replaying its events.
 */
public abstract class DomainEvent {

    private final String eventId;
    private final String aggregateId;
    private final String aggregateType;
    private final long sequenceNumber;
    private final Instant occurredAt;
    private final String eventType;
    private final int version;

    protected DomainEvent(String aggregateId, String aggregateType,
                          long sequenceNumber, String eventType) {
        this.eventId      = UUID.randomUUID().toString();
        this.aggregateId  = aggregateId;
        this.aggregateType = aggregateType;
        this.sequenceNumber = sequenceNumber;
        this.occurredAt   = Instant.now();
        this.eventType    = eventType;
        this.version      = 1;
    }

    public String getEventId()       { return eventId; }
    public String getAggregateId()   { return aggregateId; }
    public String getAggregateType() { return aggregateType; }
    public long getSequenceNumber()  { return sequenceNumber; }
    public Instant getOccurredAt()   { return occurredAt; }
    public String getEventType()     { return eventType; }
    public int getVersion()          { return version; }

    @Override
    public String toString() {
        return String.format("[%s] eventId=%s aggregateId=%s seq=%d at=%s",
                eventType, eventId, aggregateId, sequenceNumber, occurredAt);
    }
}
