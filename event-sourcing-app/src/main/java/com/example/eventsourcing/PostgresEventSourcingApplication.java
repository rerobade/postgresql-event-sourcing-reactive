package com.example.eventsourcing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@ComponentScan("eventsourcing.postgresql")
@ComponentScan
@EntityScan
@EnableR2dbcRepositories
@EnableTransactionManagement
@EnableScheduling
@EnableAsync
public class PostgresEventSourcingApplication {

    public static void main(String[] args) {
        SpringApplication.run(PostgresEventSourcingApplication.class, args);
    }
}
