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
package hu.icellmobilsoft.reactive.messaging.redis.streams.tracing;

import java.util.Map;

/**
 * Holds the data required to create OpenTelemetry spans for messages produced to or consumed from a redis stream.
 * <p>
 * The {@link #getFields()} map is the very same map instance which is used to build (outgoing) or which was read from (incoming) the redis stream
 * entry, so that the OpenTelemetry context can be injected into / extracted from it as a regular redis stream field (e.g. {@code traceparent}).
 *
 * @since 1.5.0
 * @author kornel.danko
 */
public class RedisStreamTrace {

    private final String streamKey;
    private final String consumer;
    private final String id;
    private final Map<String, String> fields;

    private RedisStreamTrace(Builder builder) {
        this.streamKey = builder.streamKey;
        this.consumer = builder.consumer;
        this.id = builder.id;
        this.fields = builder.fields;
    }

    /**
     * Gets the redis key of the stream.
     *
     * @return the redis key of the stream
     */
    public String getStreamKey() {
        return streamKey;
    }

    /**
     * Gets the unique ID of the consumer.
     *
     * @return the unique ID of the consumer, {@code null} for outgoing (produced) messages
     */
    public String getConsumer() {
        return consumer;
    }

    /**
     * Gets the ID of the redis stream entry.
     *
     * @return the ID of the redis stream entry
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the fields of the redis stream entry, used as the carrier for OpenTelemetry context propagation.
     *
     * @return the fields of the redis stream entry
     */
    public Map<String, String> getFields() {
        return fields;
    }

    /**
     * Creates a new builder for {@link RedisStreamTrace}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link RedisStreamTrace}.
     */
    public static class Builder {
        private String streamKey;
        private String consumer;
        private String id;
        private Map<String, String> fields;

        /**
         * Default constructor
         */
        public Builder() {
            // NOTE: For jdk 21.
        }

        /**
         * Sets the redis key of the stream.
         *
         * @param streamKey
         *            the redis key of the stream
         * @return this builder
         */
        public Builder withStreamKey(String streamKey) {
            this.streamKey = streamKey;
            return this;
        }

        /**
         * Sets the unique ID of the consumer.
         *
         * @param consumer
         *            the unique ID of the consumer
         * @return this builder
         */
        public Builder withConsumer(String consumer) {
            this.consumer = consumer;
            return this;
        }

        /**
         * Sets the ID of the redis stream entry.
         *
         * @param id
         *            the ID of the redis stream entry
         * @return this builder
         */
        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the fields of the redis stream entry, used as the carrier for OpenTelemetry context propagation.
         *
         * @param fields
         *            the fields of the redis stream entry
         * @return this builder
         */
        public Builder withFields(Map<String, String> fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Builds the {@link RedisStreamTrace} instance.
         *
         * @return the built instance
         */
        public RedisStreamTrace build() {
            return new RedisStreamTrace(this);
        }
    }
}
