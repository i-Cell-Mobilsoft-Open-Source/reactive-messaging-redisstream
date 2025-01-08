package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import java.lang.annotation.Annotation;
import java.util.function.Function;

import jakarta.enterprise.inject.Default;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.redis.client.RedisClientName;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.mutiny.redis.client.RedisAPI;

@Recorder
public class RedisStreamsRecorder {

    public Function<SyntheticCreationalContext<RedisStreamsProducer>, RedisStreamsProducer> createWith() {
        return this::redisStreamsProducer;
    }

    private RedisStreamsProducer redisStreamsProducer(SyntheticCreationalContext<RedisStreamsProducer> ctx) {
        return connectionKey -> {
            Annotation qualifier = Default.Literal.INSTANCE;
            if (connectionKey != null && !RedisStreamsProducer.DEFAULT_CONNECTION_KEY.equals(connectionKey)) {
                qualifier = RedisClientName.Literal.of(connectionKey);
            }
            RedisAPI redisApi = ctx.getInjectedReference(RedisAPI.class, qualifier);
            return new QuarkusRedisStreamsAdapter(redisApi);
        };
    }
}
