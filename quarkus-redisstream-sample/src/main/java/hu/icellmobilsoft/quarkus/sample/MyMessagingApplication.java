package hu.icellmobilsoft.quarkus.sample;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.MDC;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;

@ApplicationScoped
public class MyMessagingApplication {

    @Inject
    @Channel("words-out")
    Emitter<String> emitter;

    private int errorsFound = 0;

    /**
     * Sends message to the "words-out" channel, can be used from a JAX-RS resource or any bean of your application. Messages are sent to the broker.
     **/
    void onStart(@Observes StartupEvent ev) {
        sendMessages();
    }

    public void sendMessages() {
        Stream.of("Hello", "Quarkus", "Messaging"
        ).forEach(this::sendMessage);
    }

    public void sendMessage(String message) {
        if (MDC.get("extSessionId") == null) {
            MDC.put("extSessionId", UUID.randomUUID().toString());
        }
        Log.infov("Sending message: {0}", message);
        emitter.send(Message.of(message));
        MDC.clear();
    }

    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel. Messages come from the broker.
     *
     * @return
     */
    @Incoming("words-in")
    @Outgoing("uppercase")
    @Blocking(ordered = false, value = "incoming-pool")
    @Retry(maxRetries = 2)
    public String toUpperCase(String message) {
        Log.infov("Message received: [{0}]", message);
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (message != null) {
            if (message.contains("error")) {
                errorsFound++;
                Log.errorv("Error: [{0}]", errorsFound);
                if (errorsFound % 5 != 0) {
                    throw new RuntimeException("Error");
                }
            }
        }
        return message.toUpperCase();
    }

    /**
     * Consume the uppercase channel (in-memory) and print the messages.
     **/
    @Incoming("uppercase")
    public void sink(String word) {
        Log.info(word);
    }
}
