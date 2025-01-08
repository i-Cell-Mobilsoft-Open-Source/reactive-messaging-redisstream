package hu.icellmobilsoft.quarkus.sample;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class MyMessagingApplicationTest {

    @Inject
    MyMessagingApplication application;

    @Test
    void test() {
        // assertEquals("HELLO", application.toUpperCase(Message.of("Hello")).getPayload());
        // assertEquals("BONJOUR", application.toUpperCase(Message.of("bonjour")).getPayload());
    }
}
