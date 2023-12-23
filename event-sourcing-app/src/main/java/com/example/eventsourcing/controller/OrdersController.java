package com.example.eventsourcing.controller;

import com.example.eventsourcing.domain.command.*;
import com.example.eventsourcing.dto.OrderStatus;
import com.example.eventsourcing.projection.OrderProjection;
import com.example.eventsourcing.repository.OrderProjectionRepository;
import com.example.eventsourcing.repository.WaypointProjectionRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eventsourcing.postgresql.service.CommandProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrdersController {

    private final ObjectMapper objectMapper;
    private final CommandProcessor commandProcessor;
    private final OrderProjectionRepository orderProjectionRepository;
    private final WaypointProjectionRepository waypointProjectionRepository;

    @PostMapping
    public Mono<ResponseEntity<JsonNode>> placeOrder(@RequestBody JsonNode request) throws IOException {
        return commandProcessor.process(new PlaceOrderCommand(
                UUID.fromString(request.get("riderId").asText()),
                new BigDecimal(request.get("price").asText()),
                objectMapper.readValue(
                        objectMapper.treeAsTokens(request.get("route")), new TypeReference<>() {
                        }
                )))
                .map(order ->
        ResponseEntity.ok()
                .body(objectMapper.createObjectNode()
                        .put("orderId", order.getAggregateId().toString()))
                );
    }

    @PutMapping("/{orderId}")
    public Mono<ResponseEntity<Object>> modifyOrder(@PathVariable UUID orderId, @RequestBody JsonNode request) {
        OrderStatus newStatus = OrderStatus.valueOf(request.get("status").asText());
        switch (newStatus) {
            case ADJUSTED -> {
                return commandProcessor.process(new AdjustOrderPriceCommand(
                        orderId,
                        new BigDecimal(request.get("price").asText())
                )).map(order -> ResponseEntity.ok().build());
            }
            case ACCEPTED -> {
                return commandProcessor.process(new AcceptOrderCommand(
                        orderId,
                        UUID.fromString(request.get("driverId").asText())
                )).map(order -> ResponseEntity.ok().build());
            }
            case COMPLETED -> {
                return commandProcessor.process(new CompleteOrderCommand(orderId))
                        .map(order -> ResponseEntity.ok().build());
            }
            case CANCELLED -> {
                return commandProcessor.process(new CancelOrderCommand(orderId))
                        .map(order -> ResponseEntity.ok().build());
            }
            default -> {
                return Mono.just(ResponseEntity.badRequest().build());
            }
        }
    }

    @GetMapping("/")
    public ResponseEntity<Flux<OrderProjection>> getOrders() {
        return ResponseEntity.ok(orderProjectionRepository.findAll());
    }

    @GetMapping("/{orderId}")
    public Mono<ResponseEntity<OrderProjection>> getOrder(@PathVariable UUID orderId) {
        return orderProjectionRepository
                .findById(orderId)
                .flatMap(order -> waypointProjectionRepository.findByOrderId(orderId)
                        .collectList()
                        .map(waypoints -> {
                            order.setRoute(waypoints);
                            return order;
                        }))
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()));
    }
}
