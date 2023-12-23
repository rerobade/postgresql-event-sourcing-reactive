package eventsourcing.postgresql.service.event;

import eventsourcing.postgresql.domain.event.Event;
import eventsourcing.postgresql.domain.event.EventWithId;
import jakarta.annotation.Nonnull;
import reactor.core.publisher.Mono;

public interface AsyncEventHandler {

    Mono<Void> handleEvent(EventWithId<Event> event);

    @Nonnull
    String getAggregateType();

    default String getSubscriptionName() {
        return getClass().getName();
    }
}
