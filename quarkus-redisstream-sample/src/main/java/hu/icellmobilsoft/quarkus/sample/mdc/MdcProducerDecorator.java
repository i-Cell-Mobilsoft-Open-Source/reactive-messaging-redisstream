package hu.icellmobilsoft.quarkus.sample.mdc;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logmanager.MDC;

import hu.icellmobilsoft.reactive.messaging.redis.streams.RedisStreamMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.SubscriberDecorator;

@ApplicationScoped
public class MdcProducerDecorator implements SubscriberDecorator {
    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName, boolean isConnector) {
        return publisher.map(message -> {
            // don't have to map MDC in case of messages inside
            if (isConnector) {
                Optional<RedisStreamMetadata> redisStreamMetadataPresent = message.getMetadata().get(RedisStreamMetadata.class);
                if (redisStreamMetadataPresent.isEmpty()) {
                    RedisStreamMetadata redisStreamMetadata = new RedisStreamMetadata();
                    redisStreamMetadata.getAdditionalFields().putAll(MDC.copy());
                    return message.addMetadata(redisStreamMetadata);
                }
            }
            return message;
        }
        );
    }
}
