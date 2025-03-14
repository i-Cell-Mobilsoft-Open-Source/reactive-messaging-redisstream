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
package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import hu.icellmobilsoft.reactive.messaging.redis.streams.RedisStreamsConnector;
import io.quarkus.runtime.Shutdown;

/**
 * Quarkus specific lifecycle manager for RedisStreamsConnector.
 * 
 * @author mark.petrenyi
 * @since 1.0.0
 */
@ApplicationScoped
public class RedisStreamsConnectorLifecycleManager {

    @Inject
    @Connector(RedisStreamsConnector.REACTIVE_MESSAGING_REDIS_STREAMS_CONNECTOR)
    RedisStreamsConnector redisStreamsConnector;

    /**
     * Default constructor
     */
    public RedisStreamsConnectorLifecycleManager() {
        // NOTE: For jdk 21.
    }
    /**
     * Terminate the connector upon quarkus shutdown event (before ApplicationScope is destroyed).
     */
    @Shutdown
    public void terminate() {
        // Close the RedisStreamsConnector upon quarkus shutdown event in order to close before redis client gets destroyed
        redisStreamsConnector.close();
    }
}
