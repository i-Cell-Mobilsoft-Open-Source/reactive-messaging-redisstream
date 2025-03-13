/*-
 * #%L
 * reactive-redisstream-messaging
 * %%
 * Copyright (C) 2025 i-Cell Mobilsoft Zrt.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package hu.icellmobilsoft.quarkus.sample;

import hu.icellmobilsoft.reactive.messaging.redis.streams.IncomingRedisStreamMetadata;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.annotations.Blocking;
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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Class that handles messages
 */
@ApplicationScoped
public class MyMessagingApplication {

    @Inject
    @Channel("words-out")
    Emitter<String> emitter;

    private int errorsFound = 0;

    /**
     * Default constructor
     */
    public MyMessagingApplication() {
        // NOTE: For jdk 21.
    }
    /**
     * Sends message to the "words-out" channel, can be used from a JAX-RS resource or any bean of your application. Messages are sent to the broker.
     * @param ev startup event
     **/
    void onStart(@Observes StartupEvent ev) {
        sendMessages();
    }

    /**
     * Send message example messages
     */
    public void sendMessages() {
        Stream.of("Hello", "Quarkus", "Messaging"
        ).forEach(this::sendMessage);
    }

    /**
     * Send message
     *
     * @param message sendable message
     */
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
     * @param payload payLoad String
     *
     * @return uppercase payload
     */
    @Incoming("words-in")
    @Outgoing("uppercase")
    @Blocking(ordered = false, value = "incoming-pool")
    @Retry(maxRetries = 2)
    public String toUpperCase(String payload) {
        Log.infov("Message received: [{0}]", payload);
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (payload != null) {
            if (payload.contains("error")) {
                errorsFound++;
                Log.errorv("Error: [{0}]", errorsFound);
                if (errorsFound % 5 != 0) {
                    throw new RuntimeException("Error");
                }
            }
            return payload.toUpperCase();
        } else {
            return null;
        }
    }

    private static void logMetadata(Message<String> message) {
        Optional<IncomingRedisStreamMetadata> metadata = message.getMetadata().get(IncomingRedisStreamMetadata.class);
        Log.infov("Message metadata stream: [{0}]", metadata.map(IncomingRedisStreamMetadata::getStream));
        Log.infov("Message metadata id: [{0}]", metadata.map(IncomingRedisStreamMetadata::getId));
        Log.infov("Message metadata additional: [{0}]", metadata.map(IncomingRedisStreamMetadata::getAdditionalFields));
    }

    /**
     * Consume the uppercase channel (in-memory) and print the messages.
     *
     * @param word logged message
     **/
    @Incoming("uppercase")
    public void sink(String word) {
        Log.info(word);
    }
}
