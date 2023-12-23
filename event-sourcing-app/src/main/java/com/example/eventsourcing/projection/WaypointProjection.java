package com.example.eventsourcing.projection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.io.Serializable;
import java.util.UUID;

@Data
@Table(name = "RM_ORDER_ROUTE")
public class WaypointProjection implements Serializable {

    @Id
    @JsonIgnore
    private UUID id;
    @JsonIgnore
    private UUID orderId;
    private String address;
    @JsonProperty("lat")
    private double latitude;
    @JsonProperty("lon")
    private double longitude;
}
