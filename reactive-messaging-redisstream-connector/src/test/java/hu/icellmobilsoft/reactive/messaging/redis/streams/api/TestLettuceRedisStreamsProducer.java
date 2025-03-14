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
package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Test implementation of {@link RedisStreamsProducer}.
 * 
 * @author mark.petrenyi
 * @since 1.0.0
 */
@ApplicationScoped
public class TestLettuceRedisStreamsProducer implements RedisStreamsProducer {

    public static final String TEST_REDIS_PORT_KEY = "test.redis.port";

    @Override
    public RedisStreams produce(String connectionKey) {
        return new TestLettuceRedisStreams(
                ConfigProvider.getConfig().getValue(TestLettuceRedisStreamsProducer.TEST_REDIS_PORT_KEY, Integer.class));
    }

}
