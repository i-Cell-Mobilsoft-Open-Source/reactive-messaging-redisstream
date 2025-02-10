package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Test implementation of {@link RedisStreamsProducer}.
 * 
 * @author mark.petrenyi
 * @since 1.0.0
 */
@ApplicationScoped
public class TestLettuceRedisStreamsProducer implements RedisStreamsProducer {

    public static final String TEST_REDIS_PORT_KEY = "test.redis.port";

    @Override
    public RedisStreams produce(String connectionKey) {
        return new TestLettuceRedisStreams(
                ConfigProvider.getConfig().getValue(TestLettuceRedisStreamsProducer.TEST_REDIS_PORT_KEY, Integer.class));
    }

}
