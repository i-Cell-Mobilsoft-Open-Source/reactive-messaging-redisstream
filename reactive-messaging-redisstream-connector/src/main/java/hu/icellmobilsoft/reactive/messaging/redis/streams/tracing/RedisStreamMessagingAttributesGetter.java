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

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;

/**
 * Implementation of {@link MessagingAttributesGetter} which extracts OpenTelemetry messaging semantic convention attributes from a
 * {@link RedisStreamTrace}.
 *
 * @since 1.5.0
 * @author kornel.danko
 * @see <a href="https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/">OpenTelemetry Semantic Conventions for Messaging</a>
 */
public enum RedisStreamMessagingAttributesGetter implements MessagingAttributesGetter<RedisStreamTrace, Void> {

    /**
     * Singleton instance.
     */
    INSTANCE;

    /**
     * The messaging system identifier reported in the {@code messaging.system} attribute.
     */
    public static final String MESSAGING_SYSTEM = "redisstream";

    @Override
    public String getSystem(RedisStreamTrace redisStreamTrace) {
        return MESSAGING_SYSTEM;
    }

    @Override
    public String getDestination(RedisStreamTrace redisStreamTrace) {
        return redisStreamTrace.getStreamKey();
    }

    @Override
    public String getDestinationTemplate(RedisStreamTrace redisStreamTrace) {
        return null;
    }

    @Override
    public boolean isTemporaryDestination(RedisStreamTrace redisStreamTrace) {
        return false;
    }

    @Override
    public boolean isAnonymousDestination(RedisStreamTrace redisStreamTrace) {
        return false;
    }

    @Override
    public String getConversationId(RedisStreamTrace redisStreamTrace) {
        return null;
    }

    @Override
    public Long getMessageBodySize(RedisStreamTrace redisStreamTrace) {
        return null;
    }

    @Override
    public Long getMessageEnvelopeSize(RedisStreamTrace redisStreamTrace) {
        return null;
    }

    @Override
    public String getMessageId(RedisStreamTrace redisStreamTrace, Void unused) {
        return redisStreamTrace.getId();
    }

    @Override
    public String getClientId(RedisStreamTrace redisStreamTrace) {
        return redisStreamTrace.getConsumer();
    }

    @Override
    public Long getBatchMessageCount(RedisStreamTrace redisStreamTrace, Void unused) {
        return null;
    }
}
