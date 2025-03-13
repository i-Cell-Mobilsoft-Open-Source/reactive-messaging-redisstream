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
package hu.icellmobilsoft.reactive.messaging.redis.streams.metadata;

import java.util.Objects;

/**
 * Microprofile reactive streams metadata for an incoming Redis stream.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
public class IncomingRedisStreamMetadata extends RedisStreamMetadata {

    private final String stream;
    private final String id;

    /**
     * Constructs a new IncomingRedisStreamMetadata instance.
     *
     * @param stream
     *            the redis key of the stream, must not be null
     * @param id
     *            the ID of the message, must not be null
     */
    public IncomingRedisStreamMetadata(String stream, String id) {
        Objects.requireNonNull(stream, "stream is required");
        Objects.requireNonNull(id, "id is required");
        this.stream = stream;
        this.id = id;
    }

    /**
     * Gets the redis key of the stream.
     *
     * @return the redis key of the stream
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the ID of the message.
     *
     * @return the ID of the message
     */
    public String getId() {
        return id;
    }
}
