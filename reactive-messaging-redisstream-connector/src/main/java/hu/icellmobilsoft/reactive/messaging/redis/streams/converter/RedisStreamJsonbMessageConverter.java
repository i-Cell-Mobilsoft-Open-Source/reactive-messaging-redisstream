/*-
 * #%L
 * reactive-messaging-redisstream
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
package hu.icellmobilsoft.reactive.messaging.redis.streams.converter;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import hu.icellmobilsoft.reactive.messaging.redis.streams.metadata.IncomingRedisStreamMetadata;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

/**
 * A {@link MessageConverter} that deserializes JSON string payloads from Redis Streams into target DTO types using Jakarta JSON-B.
 *
 * <p>
 * The converter is automatically discovered via CDI and used by the SmallRye Reactive Messaging framework when the target type of an
 * {@code @Incoming} method parameter does not match the raw {@code String} payload from the Redis Stream. No additional configuration is required —
 * the converter detects Redis Stream messages by the presence of {@link IncomingRedisStreamMetadata}.
 * </p>
 *
 * @since 1.4.0
 * @author kornel.danko
 */
@ApplicationScoped
public class RedisStreamJsonbMessageConverter implements MessageConverter {

    private static final Logger log = Logger.getLogger(RedisStreamJsonbMessageConverter.class);

    private final Jsonb jsonb;

    /**
     * Default constructor.
     */
    public RedisStreamJsonbMessageConverter() {
        this.jsonb = JsonbBuilder.create();
    }

    /**
     * Constructs the converter. If a CDI-managed {@link Jsonb} instance is available, it is used; otherwise a default instance is created.
     *
     * @param jsonbInstance
     *            optional CDI-managed Jsonb instance
     */
    @Inject
    public RedisStreamJsonbMessageConverter(Instance<Jsonb> jsonbInstance) {
        this.jsonb = jsonbInstance.isResolvable() ? jsonbInstance.get() : JsonbBuilder.create();
    }

    /**
     * Determines whether this converter can handle the conversion from a {@code String} payload to the specified target type.
     *
     * @param in
     *            the incoming message
     * @param target
     *            the desired target type
     * @return {@code true} if the payload is a non-null {@code String}, the target type is not {@code String} or {@code Object}, and the message
     *         originates from a Redis Stream (indicated by {@link IncomingRedisStreamMetadata})
     */
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return in.getPayload() instanceof String
                && target instanceof Class<?> targetClass
                && targetClass != String.class
                && targetClass != Object.class
                && in.getMetadata(IncomingRedisStreamMetadata.class).isPresent();
    }

    /**
     * Converts the message payload from a JSON {@code String} to the specified target type using JSON-B deserialization. The original message
     * metadata is preserved.
     *
     * @param in
     *            the incoming message with a JSON string payload
     * @param target
     *            the desired target type
     * @return a new message with the deserialized payload and preserved metadata
     * @throws jakarta.json.bind.JsonbException
     *             if the JSON string cannot be deserialized to the target type
     */
    @Override
    public Message<?> convert(Message<?> in, Type target) {
        String json = (String) in.getPayload();
        log.tracev("Converting JSON payload to type [{0}]", target);
        Object deserialized = jsonb.fromJson(json, target);
        return ContextAwareMessage.of(deserialized)
                .withAck(in::ack)
                .withNack(in::nack)
                .withMetadata(in.getMetadata());
    }

    @Override
    public int getPriority() {
        return CONVERTER_DEFAULT_PRIORITY + 1;
    }
}
