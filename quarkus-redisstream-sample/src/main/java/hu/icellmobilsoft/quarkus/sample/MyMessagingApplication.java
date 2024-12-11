package hu.icellmobilsoft.quarkus.sample;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;

import org.eclipse.microprofile.reactive.messaging.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ApplicationScoped
public class MyMessagingApplication {

    @Inject
    @Channel("words-out")
    Emitter<String> emitter;

    /**
     * Sends message to the "words-out" channel, can be used from a JAX-RS resource or any bean of your application.
     * Messages are sent to the broker.
     **/
    void onStart(@Observes StartupEvent ev) {
        sendMessages();
    }

    public void sendMessages() {
        Stream.of("Hello", "with", "Quarkus", "Messaging", "message").forEach(string -> emitter.send(string));
    }

    public void sendMessage(String message) {
        emitter.send(message);
    }


    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel.
     * Messages come from the broker.
     **/
    @Incoming("words-in")
    @Outgoing("uppercase")
    @Blocking(ordered = false, value = "incoming-pool")
    public Message<String> toUpperCase(Message<Object> message) {
        Log.infov("Message received: [{0}]", message.getPayload());
        for (Object metadata : message.getMetadata()) {
            Log.infov("metadata: [{0}]", message.getPayload());
        }
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (message.getPayload() != null) {
            return message.withPayload(message.getPayload().toString().toUpperCase());
        }
        return message.withPayload("Hello");
    }

    /**
     * Consume the uppercase channel (in-memory) and print the messages.
     **/
    @Incoming("uppercase")
    public void sink(String word) {
        Log.info(word);
    }
}
