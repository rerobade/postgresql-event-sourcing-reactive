package eventsourcing.postgresql.service;

import eventsourcing.postgresql.config.EventSourcingProperties;
import eventsourcing.postgresql.config.EventSourcingProperties.SnapshottingProperties;
import eventsourcing.postgresql.domain.Aggregate;
import eventsourcing.postgresql.domain.event.Event;
import eventsourcing.postgresql.domain.event.EventWithId;
import eventsourcing.postgresql.error.OptimisticConcurrencyControlException;
import eventsourcing.postgresql.repository.AggregateRepository;
import eventsourcing.postgresql.repository.EventRepository;
import jakarta.annotation.Nullable;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Transactional
@Component
@RequiredArgsConstructor
@Slf4j
public class AggregateStore {

    private final AggregateRepository aggregateRepository;
    private final EventRepository eventRepository;
    private final AggregateFactory aggregateFactory;
    private final EventSourcingProperties properties;

    public Flux<EventWithId<Event>> saveAggregate(Aggregate aggregate) {
        log.debug("Saving aggregate {}", aggregate);

        String aggregateType = aggregate.getAggregateType();
        UUID aggregateId = aggregate.getAggregateId();
        int expectedVersion = aggregate.getBaseVersion();
        int newVersion = aggregate.getVersion();
        return aggregateRepository.createAggregateIfAbsent(aggregateType, aggregateId)
                .then(aggregateRepository.checkAndUpdateAggregateVersion(aggregateId, expectedVersion, newVersion))
                .flatMap(updated -> {
                    if (!updated) {
                        log.warn("Optimistic concurrency control error in aggregate {} {}: " +
                                        "actual version doesn't match expected version {}",
                                aggregateType, aggregateId, expectedVersion);
                        return Mono.error(new OptimisticConcurrencyControlException(expectedVersion));
                    } else {
                        SnapshottingProperties snapshotting = properties.getSnapshotting(aggregateType);
                        return createAggregateSnapshot(snapshotting, aggregate);
                    }
                })
                .then(Mono.just(aggregate.getChanges()))
                .flatMapMany(changes -> {
                    List<Mono<EventWithId<Event>>> storedChanges = new ArrayList<>();
                    changes.forEach(event -> {
                        log.info("Appending {} event: {}", aggregateType, event);
                        storedChanges.add(eventRepository.appendEvent(event));
                    });
                    return Flux.merge(storedChanges);
                });
    }

    private Mono<Void> createAggregateSnapshot(SnapshottingProperties snapshotting, Aggregate aggregate) {
        if (snapshotting.enabled() && snapshotting.nthEvent() > 1 &&
                aggregate.getVersion() % snapshotting.nthEvent() == 0) {
            log.info("Creating {} aggregate {} version {} snapshot",
                    aggregate.getAggregateType(), aggregate.getAggregateId(), aggregate.getVersion());
            return aggregateRepository.createAggregateSnapshot(aggregate);
        } else {
            return Mono.empty();
        }
    }

    public Mono<Aggregate> readAggregate(String aggregateType, UUID aggregateId) {
        return readAggregate(aggregateType, aggregateId, null);
    }

    public Mono<Aggregate> readAggregate(@NonNull String aggregateType,
                                   @NonNull UUID aggregateId,
                                   @Nullable Integer version) {
        log.debug("Reading {} aggregate {}", aggregateType, aggregateId);
        SnapshottingProperties snapshotting = properties.getSnapshotting(aggregateType);
        Mono<Aggregate> aggregate;
        if (snapshotting.enabled()) {
            aggregate = readAggregateFromSnapshot(aggregateId, version)
                    .switchIfEmpty(readAggregateFromEvents(aggregateType, aggregateId, version));

        } else {
            aggregate = readAggregateFromEvents(aggregateType, aggregateId, version);
        }
        return aggregate
                .doOnSuccess(a -> log.debug("Read aggregate {}", a))
                ;
    }

    private Mono<Aggregate> readAggregateFromSnapshot(UUID aggregateId, @Nullable Integer aggregateVersion) {
        return aggregateRepository.readAggregateSnapshot(aggregateId, aggregateVersion)
                .flatMap(aggregate -> {
                    int snapshotVersion = aggregate.getVersion();
                    log.debug("Read aggregate {} snapshot version {}", aggregateId, snapshotVersion);
                    if (aggregateVersion == null || snapshotVersion < aggregateVersion) {
                        return eventRepository.readEvents(aggregateId, snapshotVersion, aggregateVersion)
                                .map(EventWithId::event)
                                .collectList()
                                .map(events -> {
                                    log.debug("Read {} events after version {} for aggregate {}", events.size(), snapshotVersion, aggregateId);
                                    aggregate.loadFromHistory(events);
                                    return aggregate;
                                });

                    }
                    return Mono.just(aggregate);
                });
    }

    private Mono<Aggregate> readAggregateFromEvents(String aggregateType,
                                              UUID aggregateId,
                                              @Nullable Integer aggregateVersion) {
        return eventRepository.readEvents(aggregateId, null, aggregateVersion)
                .map(EventWithId::event)
                .collectList()
                .map(events -> {
                    log.debug("Read {} events for aggregate {}", events.size(), aggregateId);
                    Aggregate aggregate = aggregateFactory.newInstance(aggregateType, aggregateId);
                    aggregate.loadFromHistory(events);
                    return aggregate;
                });
    }
}
