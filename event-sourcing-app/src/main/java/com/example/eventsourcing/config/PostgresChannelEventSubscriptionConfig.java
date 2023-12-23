package com.example.eventsourcing.config;

import eventsourcing.postgresql.service.PostgresChannelEventSubscriptionProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "event-sourcing.subscriptions", havingValue = "postgres-channel")
@RequiredArgsConstructor
@Slf4j
public class PostgresChannelEventSubscriptionConfig {

    private final PostgresChannelEventSubscriptionProcessor processor;

    @Bean
    CommandLineRunner startListener() {
        return (args) -> {
            log.info("Starting to watch for new notifications in the queue...");
            processor.listenForNotifications();
        };
    }
}
