package com.example.eventsourcing.repository;

import com.example.eventsourcing.projection.OrderProjection;
import org.springframework.data.r2dbc.repository.R2dbcRepository;

import java.util.UUID;

public interface OrderProjectionRepository extends R2dbcRepository<OrderProjection, UUID> {
}
