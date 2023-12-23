package com.example.eventsourcing;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
abstract class PostgresEventSourcingApplicationTests extends AbstractContainerBaseTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void orderTestScript() throws Exception {
        System.out.println("###");
        System.out.println("###");
        System.out.println("###");
        System.out.println("E2E_TESTING = " + System.getenv().get("E2E_TESTING"));
        System.out.println("###");
        System.out.println("###");
        System.out.println("###");

        new OrderTestScript(restTemplate, KAFKA.getBootstrapServers()).execute();
    }
}
