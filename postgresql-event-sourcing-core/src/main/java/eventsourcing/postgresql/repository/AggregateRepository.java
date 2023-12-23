package eventsourcing.postgresql.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import eventsourcing.postgresql.domain.Aggregate;
import eventsourcing.postgresql.domain.AggregateTypeMapper;
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
import reactor.core.publisher.Mono;

import java.util.UUID;

@Transactional(propagation = Propagation.MANDATORY)
@Repository
@RequiredArgsConstructor
public class AggregateRepository {

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;
    private final AggregateTypeMapper aggregateTypeMapper;

    public Mono<Void> createAggregateIfAbsent(@NonNull String aggregateType,
                                              @NonNull UUID aggregateId) {
        return databaseClient.sql("""
                        INSERT INTO ES_AGGREGATE (ID, VERSION, AGGREGATE_TYPE)
                        VALUES (:aggregateId, 0, :aggregateType)
                        ON CONFLICT DO NOTHING
                        """)
                .bind("aggregateId", aggregateId)
                .bind("aggregateType", aggregateType)
                .then();
    }

    public Mono<Boolean> checkAndUpdateAggregateVersion(@NonNull UUID aggregateId,
                                                        int expectedVersion,
                                                        int newVersion) {
        return databaseClient.sql("""
                        UPDATE ES_AGGREGATE
                           SET VERSION = :newVersion
                         WHERE ID = :aggregateId
                           AND VERSION = :expectedVersion
                        """)
                .bind("newVersion", newVersion)
                .bind("aggregateId", aggregateId)
                .bind("expectedVersion", expectedVersion
                ).fetch().rowsUpdated().map(updatedRows -> updatedRows > 0L);
    }

    @SneakyThrows
    public Mono<Void> createAggregateSnapshot(@NonNull Aggregate aggregate) {
        return databaseClient.sql("""
                        INSERT INTO ES_AGGREGATE_SNAPSHOT (AGGREGATE_ID, VERSION, JSON_DATA)
                        VALUES (:aggregateId, :version, :jsonObj::json)
                        """)
                .bind("aggregateId", aggregate.getAggregateId())
                .bind("version", aggregate.getVersion())
                .bind("jsonObj", objectMapper.writeValueAsString(aggregate)
                ).then();
    }

    public Mono<Aggregate> readAggregateSnapshot(@NonNull UUID aggregateId,
                                                 @Nullable Integer version) {
        return databaseClient.sql("""
                        SELECT a.AGGREGATE_TYPE,
                               s.JSON_DATA
                          FROM ES_AGGREGATE_SNAPSHOT s
                          JOIN ES_AGGREGATE a ON a.ID = s.AGGREGATE_ID
                         WHERE s.AGGREGATE_ID = :aggregateId
                           AND (:version IS NULL OR s.VERSION <= :version)
                         ORDER BY s.VERSION DESC
                         LIMIT 1
                        """)
                .bind("aggregateId", aggregateId)
                .bind("version", Parameters.in(R2dbcType.INTEGER, version))
                .map(this::toAggregate).first();
    }

    @SneakyThrows
    private Aggregate toAggregate(Row row, RowMetadata rowMetadata) {
        String aggregateType = row.get("AGGREGATE_TYPE", String.class);
        Json jsonObj = row.get("JSON_DATA", Json.class);
        String json = jsonObj.asString();
        Class<? extends Aggregate> aggregateClass = aggregateTypeMapper.getClassByAggregateType(aggregateType);
        return objectMapper.readValue(json, aggregateClass);
    }
}
