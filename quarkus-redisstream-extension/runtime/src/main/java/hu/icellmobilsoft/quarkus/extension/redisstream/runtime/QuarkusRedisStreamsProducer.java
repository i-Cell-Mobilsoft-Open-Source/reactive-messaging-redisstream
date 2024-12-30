package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import io.vertx.mutiny.redis.client.RedisAPI;

public class QuarkusRedisStreamsProducer implements RedisStreamsProducer {

    private final RedisAPI redisAPI;

    public QuarkusRedisStreamsProducer(RedisAPI redisAPI) {
        this.redisAPI = redisAPI;
    }

    @Override
    public RedisStreams produce(String connectionKey) {
        return new QuarkusRedisStreamsAdapter(redisAPI);
    }
}
