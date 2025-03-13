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
package hu.icellmobilsoft.quarkus.sample.mdc;

import hu.icellmobilsoft.reactive.messaging.redis.streams.IncomingRedisStreamMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logmanager.MDC;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Put all addtitionalField from message metadata to MDC
 */
@ApplicationScoped
public class MdcConsumerDecorator implements PublisherDecorator {

    /**
     * Default constructor
     */
    public MdcConsumerDecorator() {
        // NOTE: For jdk 21.
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName, boolean isConnector) {
        return publisher.invoke(message -> {
            Optional<IncomingRedisStreamMetadata> incomingRedisStreamMetadata = message.getMetadata().get(IncomingRedisStreamMetadata.class);
            // don't have to map MDC in case of messages inside
            if (isConnector && incomingRedisStreamMetadata.isPresent()) {
                Map<String, String> additionalFields = incomingRedisStreamMetadata.get().getAdditionalFields();
                additionalFields.forEach(MDC::put);
            }
        });
    }
}
