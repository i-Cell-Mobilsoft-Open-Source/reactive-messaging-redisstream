package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import java.util.function.Function;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.mutiny.redis.client.RedisAPI;

@Recorder
public class RedisStreamsRecorder {

    public RedisStreamsRecorder() {
    }

    public Function<SyntheticCreationalContext<RedisStreamsProducer>, RedisStreamsProducer> createWith() {

        return ctx -> {
            RedisAPI redisApi = ctx.getInjectedReference(RedisAPI.class);
            return new QuarkusRedisStreamsProducer(redisApi);
        };
    }

}
