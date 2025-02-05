package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import hu.icellmobilsoft.reactive.messaging.redis.streams.RedisStreamsConnector;
import io.quarkus.runtime.Shutdown;

/**
 * Quarkus specific lifecycle manager for RedisStreamsConnector.
 * 
 * @author mark.petrenyi
 * @since 1.0.0
 */
@ApplicationScoped
public class RedisStreamsConnectorLifecycleManager {

    @Inject
    @Connector(RedisStreamsConnector.ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR)
    RedisStreamsConnector redisStreamsConnector;

    /**
     * Terminate the connector upon quarkus shutdown event (before ApplicationScope is destroyed).
     */
    @Shutdown
    public void terminate() {
        // Close the RedisStreamsConnector upon quarkus shutdown event in order to close before redis client gets destroyed
        redisStreamsConnector.close();
    }
}
