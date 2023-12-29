package eventsourcing.postgresql.service;

import eventsourcing.postgresql.service.event.AsyncEventHandler;
import io.netty.util.internal.StringUtil;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Wrapped;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

@Component
@ConditionalOnProperty(name = "event-sourcing.subscriptions", havingValue = "postgres-channel")
@RequiredArgsConstructor
@Slf4j
public class PostgresChannelEventSubscriptionProcessor {

    private final List<AsyncEventHandler> eventHandlers;
    private final EventSubscriptionProcessor eventSubscriptionProcessor;
    private final R2dbcProperties dataSourceProperties;
    private CountDownLatch latch = new CountDownLatch(0);

    @Async
    public void start() {
        if (this.latch.getCount() > 0) {
            return;
        }
        this.latch = new CountDownLatch(1);
        try {
            while (isActive()) {
                PostgresqlConnection conn = null;
                try {
                    conn = getPgConnection().block();
                    conn.createStatement("LISTEN channel_event_notify")
                            .execute()
                            .flatMap(PostgresqlResult::getRowsUpdated, 1)
                            .log("listen::")
                            // initially trigger processNewEvents manually to be up-to-date
                            .thenMany(Flux.merge(this.eventHandlers.stream().map(this::processNewEvents).toList()))
                            // read all incoming notifications
                            .thenMany(conn.getNotifications().onBackpressureLatest().log("notification::"))
                            .flatMap(notification -> {
                                String aggregateType = notification.getParameter();
                                return Flux.fromStream(eventHandlers.stream()
                                        .filter(eventHandler -> eventHandler.getAggregateType().equals(aggregateType))
                                );
                            }, 1)
                            .flatMap(this::processNewEvents)
                            .doOnError(e -> this.latch.countDown())
                            .doOnComplete(() -> this.latch.countDown())
                            .doFinally((x) -> {

                            })
                            .blockLast();
                    this.latch.await();
                } catch (Exception e) {
                    // The getNotifications method does not throw a meaningful message on interruption.
                    // Therefore, we do not log an error, unless it occurred while active.
                    if (isActive()) {
                        log.error("Failed to poll notifications from Postgres database", e);
                    }
                } finally {
                    log.info("close existing connection");
                    if (conn != null) {
                        conn.close().subscribe();
                    }
                }
            }

        } finally {
            this.latch.countDown();
        }
    }

    private Mono<PostgresqlConnection> getPgConnection() {
        ConnectionFactoryOptions baseOptions = ConnectionFactoryOptions.parse(dataSourceProperties.getUrl());
        ConnectionFactoryOptions.Builder ob = ConnectionFactoryOptions.builder().from(baseOptions);
        if (!StringUtil.isNullOrEmpty(dataSourceProperties.getUsername())) {
            ob.option(USER, dataSourceProperties.getUsername());
        }
        if (!StringUtil.isNullOrEmpty(dataSourceProperties.getPassword())) {
            ob.option(PASSWORD, dataSourceProperties.getPassword());
        }
        return Mono.from(ConnectionFactories.get(ob.build()).create())
                .mapNotNull(connection -> {
                    if (connection instanceof Wrapped<?> wrapped) {
                        return wrapped.unwrap();
                    } else {
                        return connection;
                    }
                })
                .cast(PostgresqlConnection.class);
    }

    private boolean isActive() {
        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    private Mono<Void> processNewEvents(AsyncEventHandler eventHandler) {
        return eventSubscriptionProcessor.processNewEvents(eventHandler)
                .doOnError(e -> log.warn("Failed to handle new events for subscription %s"
                        .formatted(eventHandler.getSubscriptionName()), e));
    }
}