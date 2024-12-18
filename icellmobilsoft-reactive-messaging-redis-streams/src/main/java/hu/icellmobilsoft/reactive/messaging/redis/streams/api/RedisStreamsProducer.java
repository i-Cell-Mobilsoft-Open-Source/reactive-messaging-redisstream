package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

public interface RedisStreamsProducer {

    String DEFAULT_CONNECTION_KEY = "<<default>>";

    RedisStreams produce(String connectionKey);

}
