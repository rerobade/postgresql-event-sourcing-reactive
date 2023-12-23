package eventsourcing.postgresql.service;

import eventsourcing.postgresql.repository.EventRepository;
import eventsourcing.postgresql.repository.EventSubscriptionRepository;
import eventsourcing.postgresql.service.event.AsyncEventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

@Transactional(propagation = Propagation.REQUIRES_NEW)
@Component
@RequiredArgsConstructor
@Slf4j
public class EventSubscriptionProcessor {

    private final EventSubscriptionRepository subscriptionRepository;
    private final EventRepository eventRepository;

    public Mono<Void> processNewEvents(AsyncEventHandler eventHandler) {
        String subscriptionName = eventHandler.getSubscriptionName();
        log.debug("Handling new events for subscription {}", subscriptionName);

        return subscriptionRepository.createSubscriptionIfAbsent(subscriptionName)
                .then(subscriptionRepository.readCheckpointAndLockSubscription(subscriptionName))
                .flatMap(checkpoint -> {
                    log.debug("Acquired lock on subscription {}, checkpoint = {}", subscriptionName, checkpoint);
                    return eventRepository.readEventsAfterCheckpoint(
                            eventHandler.getAggregateType(),
                            checkpoint.lastProcessedTransactionId(),
                            checkpoint.lastProcessedEventId()
                            )
                            .flatMap(event -> {
                                log.debug("Fetched new event for subscription {}", subscriptionName);
                                return eventHandler.handleEvent(event)
                                        .then(subscriptionRepository.updateEventSubscription(
                                                    subscriptionName,
                                                        event.transactionId(),
                                                event.id())
                                        );
                            }).count().then();

                })
                .doOnError(e -> log.error("error while accessing db", e));
    }
}
