package com.example.eventsourcing.service.event;

import com.example.eventsourcing.domain.AggregateType;
import com.example.eventsourcing.domain.OrderAggregate;
import com.example.eventsourcing.mapper.OrderMapper;
import com.example.eventsourcing.projection.OrderProjection;
import com.example.eventsourcing.projection.WaypointProjection;
import com.example.eventsourcing.repository.OrderProjectionRepository;
import com.example.eventsourcing.repository.WaypointProjectionRepository;
import eventsourcing.postgresql.domain.Aggregate;
import eventsourcing.postgresql.domain.event.Event;
import eventsourcing.postgresql.domain.event.EventWithId;
import eventsourcing.postgresql.service.event.SyncEventHandler;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderProjectionUpdater implements SyncEventHandler {

    private final OrderProjectionRepository orderProjectionRepository;
    private final WaypointProjectionRepository waypointProjectionRepository;
    private final OrderMapper mapper;

    @Transactional
    @Override
    public Mono<Void> handleEvents(List<EventWithId<Event>> events, Aggregate aggregate) {
        log.debug("Updating read model for order {}", aggregate);
        return updateOrderProjection((OrderAggregate) aggregate);
    }

    private Mono<Void> updateOrderProjection(OrderAggregate orderAggregate) {
        OrderProjection orderProjection = mapper.toProjection(orderAggregate);
        log.info("Saving order projection {}", orderProjection);
        return orderProjectionRepository.save(orderProjection)
                .doOnSuccess(o -> log.info("saved order {}", o))
                .flatMap(savedOrderProjection -> updateWaypointProjections(orderProjection.getId(), orderProjection.getRoute()))
                .then();
    }

    private Mono<Void> updateWaypointProjections(UUID orderId, List<WaypointProjection> route) {
        route.forEach(waypointProjection -> {
            waypointProjection.setOrderId(orderId);
        });
        log.info("Saving waypoint projection {}", route);
        return waypointProjectionRepository.deleteAllByOrderId(orderId)
                .doOnSuccess(o -> log.info("deleted waypoints {}", o))
                .then(waypointProjectionRepository.saveAll(route)
                        .doOnNext(w -> log.info("saved waypoint {}", w))
                        .then()
        );
    }

    @Nonnull
    @Override
    public String getAggregateType() {
        return AggregateType.ORDER.toString();
    }
}
