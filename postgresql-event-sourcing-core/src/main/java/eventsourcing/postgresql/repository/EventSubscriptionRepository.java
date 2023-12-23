package eventsourcing.postgresql.repository;

import eventsourcing.postgresql.domain.event.EventSubscriptionCheckpoint;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.math.BigInteger;

@Transactional(propagation = Propagation.MANDATORY)
@Repository
@RequiredArgsConstructor
public class EventSubscriptionRepository {

    private final DatabaseClient databaseClient;

    public Mono<Void> createSubscriptionIfAbsent(String subscriptionName) {
        return databaseClient.sql("""
                        INSERT INTO ES_EVENT_SUBSCRIPTION (SUBSCRIPTION_NAME, LAST_TRANSACTION_ID, LAST_EVENT_ID)
                        VALUES (:subscriptionName, '0'::xid8, 0)
                        ON CONFLICT DO NOTHING
                        """)
                .bind("subscriptionName", subscriptionName)
                .then();
    }

    public Mono<EventSubscriptionCheckpoint> readCheckpointAndLockSubscription(String subscriptionName) {
        return databaseClient.sql("""
                        SELECT LAST_TRANSACTION_ID::text,
                               LAST_EVENT_ID
                          FROM ES_EVENT_SUBSCRIPTION
                         WHERE SUBSCRIPTION_NAME = :subscriptionName
                           FOR UPDATE SKIP LOCKED
                        """)
                .bind("subscriptionName", subscriptionName)
                .map(this::toEventSubscriptionCheckpoint)
                .first();
    }

    public Mono<Boolean> updateEventSubscription(String subscriptionName,
                                                 BigInteger lastProcessedTransactionId,
                                                 long lastProcessedEventId) {
        return databaseClient.sql("""
                        UPDATE ES_EVENT_SUBSCRIPTION
                           SET LAST_TRANSACTION_ID = :lastProcessedTransactionId::xid8,
                               LAST_EVENT_ID = :lastProcessedEventId
                         WHERE SUBSCRIPTION_NAME = :subscriptionName
                        """)
                .bind("subscriptionName", subscriptionName)
                .bind("lastProcessedTransactionId", lastProcessedTransactionId.toString())
                .bind("lastProcessedEventId", lastProcessedEventId
                ).fetch().rowsUpdated().map(updatedRows -> updatedRows > 0L);
    }

    private EventSubscriptionCheckpoint toEventSubscriptionCheckpoint(Row row, RowMetadata rowMetadata) {
        String lastProcessedTransactionId = row.get("LAST_TRANSACTION_ID", String.class);
        Long lastProcessedEventId = row.get("LAST_EVENT_ID", Long.class);
        return new EventSubscriptionCheckpoint(new BigInteger(lastProcessedTransactionId), lastProcessedEventId);
    }
}
