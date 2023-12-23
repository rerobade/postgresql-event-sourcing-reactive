package com.example.eventsourcing.repository;

import com.example.eventsourcing.projection.WaypointProjection;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface WaypointProjectionRepository extends R2dbcRepository<WaypointProjection, UUID> {
    Flux<WaypointProjection> findByOrderId(UUID orderId);

    Mono<Void> deleteAllByOrderId(UUID orderId);
}
