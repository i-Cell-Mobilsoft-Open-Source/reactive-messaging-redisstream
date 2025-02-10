package hu.icellmobilsoft.quarkus.extension.redisstream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import hu.icellmobilsoft.reactive.messaging.redis.streams.RedisStreamMetadata;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

/**
 * Test bean for producing redis messages.
 *
 * @author mark.petrenyi
 * @since 1.0.0
 */
@ApplicationScoped
public class TestProducer {

    public static final String ADDITIONAL_FIELD_KEY = "TestProducer_additionalField";
    /**
     * The Emitter.
     */
    @Inject
    @Channel("out")
    Emitter<String> emitter;

    /**
     * The Emitter with metadata.
     */
    @Inject
    @Channel("out-with-metadata")
    Emitter<String> emitterWithMetadata;

    /**
     * Produce simple message.
     *
     * @param message
     *            the message
     */
    public void produce(final String message) {
        emitter.send(message);
    }

    /**
     * Produce message with additional field.
     *
     * @param message
     *            the message
     * @param additionalField
     *            the additional field
     */
    public void produceWithMetadata(final String message, String additionalField) {
        emitterWithMetadata.send(
                ContextAwareMessage.of(message)
                        .addMetadata(new RedisStreamMetadata().withAdditionalField(ADDITIONAL_FIELD_KEY, additionalField)));
    }
}
