package eventsourcing.postgresql.service.event;

import eventsourcing.postgresql.domain.Aggregate;
import eventsourcing.postgresql.domain.event.Event;
import eventsourcing.postgresql.domain.event.EventWithId;
import jakarta.annotation.Nonnull;
import reactor.core.publisher.Mono;

import java.util.List;

public interface SyncEventHandler {

    Mono<Void> handleEvents(List<EventWithId<Event>> events,
                      Aggregate aggregate);

    @Nonnull
    String getAggregateType();
}
