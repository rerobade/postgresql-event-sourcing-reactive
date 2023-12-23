package eventsourcing.postgresql.service;

import eventsourcing.postgresql.service.event.AsyncEventHandler;
import io.netty.util.internal.StringUtil;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcProperties;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

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
    private final ConnectionFactory connectionFactory;
    private final DatabaseClient databaseClient;
    //private final ExecutorService executor = newExecutor();
    //private CountDownLatch latch = new CountDownLatch(0);
    //private Future<?> future = CompletableFuture.completedFuture(null);
    private volatile PostgresqlConnection connection;

    /*private static ExecutorService newExecutor() {
        CustomizableThreadFactory threadFactory =
                new CustomizableThreadFactory("postgres-channel-event-subscription-");
        threadFactory.setDaemon(true);
        return Executors.newSingleThreadExecutor(threadFactory);
    }*/

    @Async
    public void listenForNotifications() {
        while (true) {
        try {
            connection = getPgConnection().block();
            connection.createStatement("LISTEN channel_event_notify")
                    .execute()
                    .flatMap(PostgresqlResult::getRowsUpdated, 1)
                    .log("listen::")
                    // initially trigger processNewEvents manually to be up-to-date
                    .thenMany(Flux.merge(this.eventHandlers.stream().map(this::processNewEvents).toList()))
                    // read all incoming notifications
                    .thenMany(connection.getNotifications().delayElements(Duration.ofSeconds(1), Schedulers.single()).log("notification::"))
                    .delayElements(Duration.ofSeconds(1))
                    .flatMapSequential(notification -> {
                        String aggregateType = notification.getParameter();
                        return Flux.fromStream(eventHandlers.stream()
                                .filter(eventHandler -> eventHandler.getAggregateType().equals(aggregateType))
                                );
                    }, 1)
                    .map(this::processNewEvents)
                    /*.flatMap(notification -> {
                        String aggregateType = notification.getParameter();
                        return Flux.merge(eventHandlers.stream()
                                .filter(eventHandler -> eventHandler.getAggregateType().equals(aggregateType))
                                .map(this::processNewEvents)
                                .toList());
                    }, 1)*/
                    .blockLast();
        } catch (Exception e) {
            log.warn("Error occurred while listening for notifications, attempting to reconnect ...", e);

            //this.listenForNotifications();
        } finally {
            log.info("close existing connection");
            if (connection != null) {
                connection.close().subscribe();
            }
        }
        }
    }

    //@PostConstruct
    public void start() {
        /*
        if (this.latch.getCount() > 0) {
            return;
        }
        this.latch = new CountDownLatch(1);
        this.future = executor.submit(() -> {
            try {
                while (isActive()) {
         */
        try {
            connection = getPgConnection().block();
            connection.createStatement("LISTEN channel_event_notify")
                    .execute()
                    .flatMap(PostgresqlResult::getRowsUpdated)
                    .log("listen::")
                    .subscribe();
            // initially trigger processNewEvents manually to be up to date
            this.eventHandlers.forEach(this::processNewEvents);

            //try {
            //this.connection = conn;
            //while (isActive()) {
            Flux<Notification> notifications = connection.getNotifications()
                    .delayElements(Duration.ofSeconds(1));
            // Unfortunately, there is no good way of interrupting a notification
            // poll but by closing its connection.
            //if (!isActive()) {
            //    return;
            //}
            notifications.doOnNext(notification -> {
                String aggregateType = notification.getParameter();
                eventHandlers.stream()
                        .filter(eventHandler -> eventHandler.getAggregateType().equals(aggregateType))
                        .forEach(this::processNewEvents);
            }).subscribe();
            //}
            //} finally {
            //    conn.close().subscribe();
            //}
        } catch (Exception e) {
            // The getNotifications method does not throw a meaningful message on interruption.
            // Therefore, we do not log an error, unless it occurred while active.
            //if (isActive()) {
            //    log.error("Failed to poll notifications from Postgres database", e);
            //}
        }
/*                }
            } finally {
                this.latch.countDown();
            }
        });
 */
    }
/*
    private PostgresqlConnection getPgConnection() {
        return Mono.from(connectionFactory.create())
                .mapNotNull(connection -> {
                    if (connection instanceof Wrapped<?> wrapped) {
                        return wrapped.unwrap();
                    } else {
                        return connection;
                    }
                })
                .cast(PostgresqlConnection.class)
                .block();
    }
*/
    private Mono<PostgresqlConnection> getPgConnection() {
        R2dbcProperties properties = dataSourceProperties;
        ConnectionFactoryOptions baseOptions = ConnectionFactoryOptions.parse(properties.getUrl());
        ConnectionFactoryOptions.Builder ob = ConnectionFactoryOptions.builder().from(baseOptions);
        if (!StringUtil.isNullOrEmpty(properties.getUsername())) {
            ob.option(USER, properties.getUsername());
        }
        if (!StringUtil.isNullOrEmpty(properties.getPassword())) {
            ob.option(PASSWORD, properties.getPassword());
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

    private PostgresqlConnection getConnection(Connection connection) {
        if (connection instanceof Wrapped<?> wrapped) {
            return (PostgresqlConnection) wrapped.unwrap();
        } else {
            return (PostgresqlConnection) connection;
        }
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
/*
    @PreDestroy
    public synchronized void stop() {
        if (this.future.isDone()) {
            return;
        }
        this.future.cancel(true);
        PostgresqlConnection conn = this.connection;
        if (conn != null) {
            conn.close().subscribe();
        }
        try {
            if (!this.latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "Failed to stop %s".formatted(PostgresChannelEventSubscriptionProcessor.class.getName()));
            }
        } catch (InterruptedException ignored) {
        }
    }*/
}