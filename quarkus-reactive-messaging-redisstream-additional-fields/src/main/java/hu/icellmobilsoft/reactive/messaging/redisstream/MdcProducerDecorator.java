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
import org.jboss.logging.MDC;

import hu.icellmobilsoft.reactive.messaging.redis.streams.metadata.RedisStreamMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.SubscriberDecorator;

/**
 * Put all MDC to message metadata
 * 
 * @since 1.1.0
 * @author mark.petrenyi
 */
@ApplicationScoped
public class MdcProducerDecorator implements SubscriberDecorator {

    /**
     * Default constructor
     */
    public MdcProducerDecorator() {
        // NOTE: For jdk 21.
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName, boolean isConnector) {
        return publisher.map(message -> {
            if (!isConnector) {
                // don't have to map MDC in case of messages inside
                return message;
            }
            Optional<RedisStreamMetadata> redisStreamMetadataPresent = message.getMetadata().get(RedisStreamMetadata.class);
            if (redisStreamMetadataPresent.isPresent()) {
                redisStreamMetadataPresent.get().withAdditionalFields(getAdditionalFields(redisStreamMetadataPresent.get()));
                return message;
            } else {
                return message.addMetadata(new RedisStreamMetadata().withAdditionalFields(getAdditionalFields(null)));
            }
        });
    }

    /**
     * Put redis message additionalFields from MDC
     * 
     * @param redisStreamMetadata
     *            metadata containing additionalFields and flowIdExtension
     * @return a map that should be added to redis stream additional fields
     */
    protected Map<String, String> getAdditionalFields(RedisStreamMetadata redisStreamMetadata) {
        return Map.of(LogConstants.LOG_SESSION_ID, getFlowIdMessage(redisStreamMetadata == null ? null : redisStreamMetadata.getFlowIdExtension()));
    }

    private String getFlowIdMessage(String flowIdExtension) {
        String flowIdMessage = (String) MDC.get(LogConstants.LOG_SESSION_ID);
        if (flowIdExtension == null) {
            return flowIdMessage;
        }
        return flowIdMessage + "_" + flowIdExtension;
    }
}
