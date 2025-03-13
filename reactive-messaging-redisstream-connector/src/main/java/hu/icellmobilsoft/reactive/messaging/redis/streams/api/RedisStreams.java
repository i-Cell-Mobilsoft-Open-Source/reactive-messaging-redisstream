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
package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.Map;

/**
 * Interface for interacting with Redis Streams.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
public interface RedisStreams {

    /**
     * Checks if a group exists in the specified stream (i.e. using {@code XINFO GROUPS}).
     *
     * @param stream
     *            the redis key of the stream
     * @param group
     *            the name of the consumer group
     * @return true if the group exists, false otherwise
     * @see <a href="https://redis.io/docs/latest/commands/xinfo">XINFO</a>
     */
    Uni<Boolean> existGroup(String stream, String group);

    /**
     * Creates a new consumer group in the specified stream.
     *
     *
     * @param stream
     *            the redis key of the stream
     * @param group
     *            the name of the consumer group
     * @return redis operation result
     * @see <a href="https://redis.io/docs/latest/commands/xgroup-create/">XGROUP CREATE</a>
     */
    Uni<String> xGroupCreate(String stream, String group);

    /**
     * Acknowledges the processing of a message in the specified stream by the specified group.
     * 
     *
     * @param stream
     *            the redis key of the stream
     * @param group
     *            the name of the consumer group
     * @param id
     *            the ID of the message
     * @return a Uni containing the number of acknowledged messages
     * @see <a href="https://redis.io/docs/latest/commands/xack">XACK</a>
     */
    Uni<Long> xAck(String stream, String group, String id);

    /**
     * Adds a message to the specified stream.
     *
     * @param stream
     *            the redis key of the stream
     * @param id
     *            the ID of the message
     * @param fields
     *            the fields of the message
     * @return a Uni containing the ID of the added message
     * @see <a href="https://redis.io/docs/latest/commands/xadd">XADD</a>
     */
    Uni<String> xAdd(String stream, String id, Map<String, String> fields);

    /**
     * Adds a message to the specified stream with additional parameters.
     * 
     * @param stream
     *            the redis key of the stream
     * @param id
     *            the ID of the message
     * @param maxLen
     *            the maximum length of the stream, it takes precedence over {@code minId}
     * @param exact
     *            whether the maximum length should be exact or near exact
     * @param minId
     *            the minimum ID of entries allowed in stream
     * @param fields
     *            the fields of the message
     * @return a Uni containing the ID of the added message
     * @see <a href="https://redis.io/docs/latest/commands/xadd">XADD</a>
     */
    Uni<String> xAdd(String stream, String id, Integer maxLen, Boolean exact, String minId, Map<String, String> fields);

    /**
     * Reads messages from the specified stream and group.
     *
     * @param stream
     *            the redis key of the stream
     * @param group
     *            the name of the consumer group
     * @param consumer
     *            the unique ID of the consumer
     * @param count
     *            the number of messages to read
     * @param blockMs
     *            the maximum amount of time to block in milliseconds
     * @param noack
     *          do ack or don't
     * @return a Uni containing a list of stream entries
     * @see <a href="https://redis.io/docs/latest/commands/xreadgroup">XREADGROUP</a>
     */
    Uni<List<StreamEntry>> xReadGroup(String stream, String group, String consumer, Integer count, Integer blockMs, Boolean noack);

    /**
     * Close underlying connections.
     */
    default void close(){}

}
