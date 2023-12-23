package com.example.eventsourcing.projection;

import com.example.eventsourcing.dto.OrderStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Table(name = "RM_ORDER")
@RequiredArgsConstructor
@Getter
@Setter
@ToString
public class OrderProjection implements Persistable<UUID>, Serializable {

    @Id
    private UUID id;
    private int version;
    private OrderStatus status;
    private UUID riderId;
    private BigDecimal price;
    @Transient // needs to be mapped manually
    @ToString.Exclude
    private List<WaypointProjection> route = new ArrayList<>();
    private UUID driverId;
    private OffsetDateTime placedDate;
    private OffsetDateTime acceptedDate;
    private OffsetDateTime completedDate;
    private OffsetDateTime cancelledDate;

    @JsonIgnore
    @Override
    public boolean isNew() {
        return version <= 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        OrderProjection other = (OrderProjection) o;
        return Objects.equals(id, other.getId());
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
