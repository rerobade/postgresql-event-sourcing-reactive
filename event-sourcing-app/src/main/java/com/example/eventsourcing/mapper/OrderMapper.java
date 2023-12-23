package com.example.eventsourcing.mapper;

import com.example.eventsourcing.domain.OrderAggregate;
import com.example.eventsourcing.dto.OrderDto;
import com.example.eventsourcing.dto.WaypointDto;
import com.example.eventsourcing.projection.OrderProjection;
import com.example.eventsourcing.projection.WaypointProjection;
import eventsourcing.postgresql.domain.event.Event;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;

@Mapper(componentModel = "spring")
public interface OrderMapper {

    @Mapping(source = "aggregateId", target = "id")
    @Mapping(target = "route", qualifiedByName = "toWaypointProjection")
    OrderProjection toProjection(OrderAggregate order);

    @Named("toWaypointProjection")
    @Mapping(target = "orderId", ignore = true)
    @Mapping(target = "id", ignore = true)
    WaypointProjection toProjection(WaypointDto waypoint);

    @Mapping(source = "order.aggregateId", target = "orderId")
    @Mapping(source = "event.eventType", target = "eventType")
    @Mapping(source = "event.createdDate", target = "eventTimestamp")
    @Mapping(source = "order.baseVersion", target = "version")
    @Mapping(source = "order.riderId", target = "riderId")
    @Mapping(source = "order.price", target = "price")
    @Mapping(source = "order.route", target = "route")
    @Mapping(source = "order.driverId", target = "driverId")
    OrderDto toDto(Event event, OrderAggregate order);
}
