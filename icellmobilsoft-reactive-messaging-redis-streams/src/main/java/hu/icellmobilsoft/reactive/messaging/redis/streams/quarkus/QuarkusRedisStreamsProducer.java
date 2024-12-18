package hu.icellmobilsoft.reactive.messaging.redis.streams.quarkus;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import io.vertx.mutiny.redis.client.RedisAPI;

@ApplicationScoped
public class QuarkusRedisStreamsProducer implements RedisStreamsProducer {

    private final RedisAPI redisAPI;

    @Inject
    public QuarkusRedisStreamsProducer(RedisAPI redisAPI) {
        this.redisAPI = redisAPI;
    }

    @Override
    public RedisStreams produce(String connectionKey) {
        return new QuarkusRedisStreamsAdapter(redisAPI);
    }
}
