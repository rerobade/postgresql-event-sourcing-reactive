package eventsourcing.postgresql.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import eventsourcing.postgresql.domain.event.Event;
import eventsourcing.postgresql.domain.event.EventTypeMapper;
import eventsourcing.postgresql.domain.event.EventWithId;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import jakarta.annotation.Nullable;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigInteger;
import java.util.UUID;

@Transactional(propagation = Propagation.MANDATORY)
@Repository
@RequiredArgsConstructor
public class EventRepository {

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;
    private final EventTypeMapper eventTypeMapper;

    @SneakyThrows
    public Mono<EventWithId<Event>> appendEvent(@NonNull Event event) {
        return databaseClient.sql("""
                        INSERT INTO ES_EVENT (TRANSACTION_ID, AGGREGATE_ID, VERSION, EVENT_TYPE, JSON_DATA)
                        VALUES(pg_current_xact_id(), :aggregateId, :version, :eventType, :jsonObj::json)
                        RETURNING ID, TRANSACTION_ID::text, EVENT_TYPE, JSON_DATA
                        """)
                .bind("aggregateId", event.getAggregateId())
                .bind("version", event.getVersion())
                .bind("eventType", event.getEventType())
                .bind("jsonObj", Json.of(objectMapper.writeValueAsString(event)))
                .map(this::toEvent).first();
    }

    public Flux<EventWithId<Event>> readEvents(@NonNull UUID aggregateId,
                                               @Nullable Integer fromVersion,
                                               @Nullable Integer toVersion) {
        return databaseClient.sql("""
                        SELECT ID,
                               TRANSACTION_ID::text,
                               EVENT_TYPE,
                               JSON_DATA
                          FROM ES_EVENT
                         WHERE AGGREGATE_ID = :aggregateId
                           AND (:fromVersion IS NULL OR VERSION > :fromVersion)
                           AND (:toVersion IS NULL OR VERSION <= :toVersion)
                         ORDER BY VERSION ASC
                        """)
                .bind("aggregateId", aggregateId)
                .bind("fromVersion", Parameters.in(R2dbcType.INTEGER, fromVersion))
                .bind("toVersion", Parameters.in(R2dbcType.INTEGER, toVersion))
                .map(this::toEvent)
                .all();
    }

    public Flux<EventWithId<Event>> readEventsAfterCheckpoint(@NonNull String aggregateType,
                                                              @NonNull BigInteger lastProcessedTransactionId,
                                                              long lastProcessedEventId) {
        return databaseClient.sql("""
                        SELECT e.ID,
                               e.TRANSACTION_ID::text,
                               e.EVENT_TYPE,
                               e.JSON_DATA
                          FROM ES_EVENT e
                          JOIN ES_AGGREGATE a on a.ID = e.AGGREGATE_ID
                         WHERE a.AGGREGATE_TYPE = :aggregateType
                           AND (e.TRANSACTION_ID, e.ID) > (:lastProcessedTransactionId::xid8, :lastProcessedEventId)
                           AND e.TRANSACTION_ID < pg_snapshot_xmin(pg_current_snapshot())
                         ORDER BY e.TRANSACTION_ID ASC, e.ID ASC
                        """)
                .bind("aggregateType", aggregateType)
                .bind("lastProcessedTransactionId", Parameters.in(R2dbcType.VARCHAR, lastProcessedTransactionId.toString()))
                .bind("lastProcessedEventId", lastProcessedEventId)
                .map(this::toEvent)
                .all();
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private <T extends Event> EventWithId<T> toEvent(Row row, RowMetadata rowMetadata) {
        Long id = row.get("ID", Long.class);
        String transactionId = row.get("TRANSACTION_ID", String.class);
        String eventType = row.get("EVENT_TYPE", String.class);
        Json jsonObj = row.get("JSON_DATA", Json.class);
        String json = jsonObj.asString();
        Class<? extends Event> eventClass = eventTypeMapper.getClassByEventType(eventType);
        Event event = objectMapper.readValue(json, eventClass);
        return new EventWithId<>(id, new BigInteger(transactionId), (T) event);
    }
}
