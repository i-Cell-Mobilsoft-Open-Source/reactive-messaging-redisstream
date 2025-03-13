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
package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import java.lang.annotation.Annotation;
import java.util.function.Function;

import jakarta.enterprise.inject.Default;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.redis.client.RedisClientName;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.mutiny.redis.client.RedisAPI;

/**
 * Recorder class for Redis Streams. This class is responsible for creating and configuring the RedisStreamsProducer.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
@Recorder
public class RedisStreamsRecorder {

    /**
     * Default constructor
     */
    public RedisStreamsRecorder() {
        // NOTE: For jdk 21.
    }

    /**
     * Creates a function that produces a RedisStreamsProducer.
     *
     * @return a function that takes a SyntheticCreationalContext and returns a RedisStreamsProducer
     */
    public Function<SyntheticCreationalContext<RedisStreamsProducer>, RedisStreamsProducer> createWith() {
        return this::redisStreamsProducer;
    }

    /**
     * Produces a RedisStreamsProducer based on the given SyntheticCreationalContext. This method retrieves the RedisAPI instance using the
     * appropriate qualifier and creates a new QuarkusRedisStreamsAdapter.
     *
     * @param ctx
     *            the SyntheticCreationalContext used to get the injected reference of RedisAPI
     * @return a RedisStreamsProducer configured with the RedisAPI instance
     */
    private RedisStreamsProducer redisStreamsProducer(SyntheticCreationalContext<RedisStreamsProducer> ctx) {
        return connectionKey -> {
            Annotation qualifier = Default.Literal.INSTANCE;
            if (connectionKey != null && !RedisStreamsProducer.DEFAULT_CONNECTION_KEY.equals(connectionKey)) {
                qualifier = RedisClientName.Literal.of(connectionKey);
            }
            RedisAPI redisApi = ctx.getInjectedReference(RedisAPI.class, qualifier);
            return new QuarkusRedisStreamsAdapter(redisApi);
        };
    }
}
