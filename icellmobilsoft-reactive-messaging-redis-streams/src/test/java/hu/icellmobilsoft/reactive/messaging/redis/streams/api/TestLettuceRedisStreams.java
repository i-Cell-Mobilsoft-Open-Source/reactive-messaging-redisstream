package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.jboss.logging.Logger;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.converters.uni.UniReactorConverters;
import reactor.core.publisher.Mono;

/**
 * Test implementation for {@link RedisStreams} using Lettuce.
 * 
 * @author mark.petrenyi
 * @since 1.0.0
 */
public class TestLettuceRedisStreams implements RedisStreams {
    private final RedisClient redisClient;

    public TestLettuceRedisStreams(int port) {
        redisClient = RedisClient.create(RedisURI.create("localhost", port));
        redisClient.setOptions(
                ClientOptions.builder()
                        .autoReconnect(false)
                        .suspendReconnectOnProtocolFailure(true)
                        .build());
    }

    @Override
    public void close() {
        try {
            redisClient.shutdownAsync(1, 1, TimeUnit.SECONDS).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Lettuce connection close failed!", e);
        }
    }

    @Override
    public boolean existGroup(String stream, String group) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            List<Object> groups = connection.sync().xinfoGroups(stream);
            return groups.stream()
                    .filter(o -> o instanceof List)
                    .map(o -> (List) o)
                    .filter(l -> l.size() > 2)
                    .map(l -> l.get(1))
                    .anyMatch(group::equals);
        } catch (Exception e) {
            Logger.getLogger(TestLettuceRedisStreams.class)
                    .errorv("Redis exception during checking group [{0}] on stream [{1}]: [{2}]", group, stream, e.getLocalizedMessage());
            return false;
        }
    }

    @Override
    public String xGroupCreate(String stream, String group) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            return connection.sync().xgroupCreate(XReadArgs.StreamOffset.from(stream, "0-0"), group, XGroupCreateArgs.Builder.mkstream());
        }
    }

    @Override
    public Uni<Long> xAck(String stream, String group, String id) {
        Mono<Long> xack = redisClient.connect().reactive().xack(stream, group, id);
        return UniReactorConverters.<Long> fromMono().from(xack);
    }

    @Override
    public Uni<String> xAdd(String stream, String id, Map<String, String> fields) {
        return xAdd(stream, id, null, null, null, fields);
    }

    @Override
    public Uni<String> xAdd(String stream, String id, Integer maxLen, Boolean exact, String minId, Map<String, String> fields) {
        XAddArgs args;
        if (maxLen != null) {
            args = XAddArgs.Builder.maxlen(maxLen).exactTrimming(exact);
        } else if (minId != null) {
            args = XAddArgs.Builder.minId(minId);
        } else {
            args = new XAddArgs();
        }
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        Mono<String> xadd = connection.reactive().xadd(stream, args.id(id), fields);
        return UniReactorConverters.<String> fromMono().from(xadd);

    }

    @Override
    public Uni<List<StreamEntry>> xReadGroup(String stream, String group, String consumer, Integer count, Integer blockMs) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        Mono<List<StreamEntry>> listMono = connection
                .reactive()
                .xreadgroup(
                        Consumer.from(group, consumer),
                        XReadArgs.Builder.count(count).block(blockMs),
                        XReadArgs.StreamOffset.lastConsumed(stream))
                .map(sm -> new StreamEntry(stream, sm.getId(), sm.getBody()))
                .collectList();
        return UniReactorConverters.<List<StreamEntry>> fromMono().from(listMono).onTermination().invoke(connection::close);
    }
}
