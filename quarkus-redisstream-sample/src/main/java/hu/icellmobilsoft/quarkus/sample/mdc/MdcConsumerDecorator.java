package hu.icellmobilsoft.quarkus.sample.mdc;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logmanager.MDC;

import hu.icellmobilsoft.reactive.messaging.redis.streams.IncomingRedisStreamMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;

@ApplicationScoped
public class MdcConsumerDecorator implements PublisherDecorator {
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
