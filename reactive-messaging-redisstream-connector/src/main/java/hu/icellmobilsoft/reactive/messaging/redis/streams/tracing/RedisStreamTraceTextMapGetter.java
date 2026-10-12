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

import java.util.Collections;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

/**
 * Extracts the OpenTelemetry propagation headers from the fields of a {@link RedisStreamTrace}.
 *
 * @since 1.5.0
 * @author kornel.danko
 */
public enum RedisStreamTraceTextMapGetter implements TextMapGetter<RedisStreamTrace> {

    /**
     * Singleton instance.
     */
    INSTANCE;

    @Override
    public Iterable<String> keys(RedisStreamTrace carrier) {
        Map<String, String> fields = carrier.getFields();
        return fields == null ? Collections.emptyList() : fields.keySet();
    }

    @Override
    public String get(RedisStreamTrace carrier, String key) {
        if (carrier == null || carrier.getFields() == null) {
            return null;
        }
        return carrier.getFields().get(key);
    }
}
