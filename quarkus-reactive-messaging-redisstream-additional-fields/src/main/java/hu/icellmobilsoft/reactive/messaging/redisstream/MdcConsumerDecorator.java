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
package hu.icellmobilsoft.reactive.messaging.redisstream;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logmanager.MDC;

import hu.icellmobilsoft.reactive.messaging.redis.streams.metadata.IncomingRedisStreamMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;

/**
 * Put all additionalField from message metadata to MDC
 * 
 * @since 1.1.0
 * @author mark.petrenyi
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
                processAdditionalFields(additionalFields);
            }
        });
    }

    /**
     * Put additionalFields from message metadata to MDC
     *
     * @param additionalFields
     *            additional fields to add
     */
    protected void processAdditionalFields(Map<String, String> additionalFields) {
        MDC.put(LogConstants.LOG_SESSION_ID, additionalFields.get(LogConstants.LOG_SESSION_ID));
    }
}
