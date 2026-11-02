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

import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * Injects the OpenTelemetry propagation headers into the fields of a {@link RedisStreamTrace}, so they are sent along the redis stream entry as
 * regular fields (e.g. {@code traceparent}).
 *
 * @since 1.5.0
 * @author kornel.danko
 */
public enum RedisStreamTraceTextMapSetter implements TextMapSetter<RedisStreamTrace> {

    /**
     * Singleton instance.
     */
    INSTANCE;

    @Override
    public void set(RedisStreamTrace carrier, String key, String value) {
        if (carrier == null || carrier.getFields() == null) {
            return;
        }
        carrier.getFields().put(key, value);
    }
}
