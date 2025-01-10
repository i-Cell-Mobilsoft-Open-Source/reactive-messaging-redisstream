package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

/**
 * CDI managed interface for producing Redis Streams.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 * @see RedisStreams
 */
public interface RedisStreamsProducer {

    /**
     * The default redis connection key.
     */
    String DEFAULT_CONNECTION_KEY = "<<default>>";

    /**
     * Produces a RedisStreams instance based on the provided connection key.
     *
     * @param connectionKey
     *            the key for the Redis connection
     * @return a RedisStreams instance
     */
    RedisStreams produce(String connectionKey);

}
