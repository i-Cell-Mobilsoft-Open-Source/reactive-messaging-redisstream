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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

/**
 * CDI bean for serializing objects to JSON strings using Jakarta JSON-B. Used by the outgoing connector to serialize DTO payloads before writing them
 * to Redis Streams.
 *
 * @since 1.4.0
 * @author kornel.danko
 */
@ApplicationScoped
public class RedisStreamJsonbSerializer {

    private final Jsonb jsonb;

    /**
     * Default constructor.
     */
    public RedisStreamJsonbSerializer() {
        this.jsonb = JsonbBuilder.create();
    }

    /**
     * Constructs the serializer. If a CDI-managed {@link Jsonb} instance is available, it is used; otherwise a default instance is created.
     *
     * @param jsonbInstance
     *            optional CDI-managed Jsonb instance
     */
    @Inject
    public RedisStreamJsonbSerializer(Instance<Jsonb> jsonbInstance) {
        this.jsonb = jsonbInstance.isResolvable() ? jsonbInstance.get() : JsonbBuilder.create();
    }

    /**
     * Serializes the given object to a JSON string.
     *
     * @param payload
     *            the object to serialize
     * @return the JSON string representation
     */
    public String serialize(Object payload) {
        if (payload == null) {
            return String.valueOf(payload);
        }
        if (payload instanceof String s) {
            return s;
        }
        return jsonb.toJson(payload);
    }
}
