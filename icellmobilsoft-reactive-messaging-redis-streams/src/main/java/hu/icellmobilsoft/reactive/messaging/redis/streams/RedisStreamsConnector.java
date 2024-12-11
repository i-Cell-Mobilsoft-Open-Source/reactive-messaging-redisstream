package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.stream.StreamSupport;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;

@ApplicationScoped
@Connector("icellmobilsoft-redis-streams")
public class RedisStreamsConnector implements InboundConnector, OutboundConnector {


    @Inject
    protected RedisAPI redisAPI;
    private String group;
    private String streamKey;
    private String consumer;

    @PostConstruct
    void init() {
        this.group = "group";
        this.streamKey = "stream";
        this.consumer = "consumer";
        if (!existGroup(streamKey, group)) {
            Response create = redisAPI.xgroupAndAwait(List.of("CREATE", streamKey, group, "0", "MKSTREAM"));
            Log.info(create);
        }
    }


    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        return Multi.createBy().repeating().uni(this::xReadMessage).indefinitely().filter(Objects::nonNull)
                .invoke(r -> Log.infov("msg received: [{0}]", r)).map(Message::of);
    }

    private Uni<Response> xReadMessage() {
        List<String> xread = List.of("GROUP", group, consumer, "COUNT", "1", "STREAMS", streamKey, ">");

        return redisAPI.xreadgroup(xread);
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        return MultiUtils.via(multi -> multi.onItem().transformToUniAndConcatenate(message -> {
                            Log.infov("msg sent:[{0}]", message.getPayload());
                            return redisAPI.xadd(toXadd(message));
                        }
                )
        );
    }

    private List<String> toXadd(Message<?> message) {
        return List.of(streamKey, "*", "payload", message.getPayload().toString());
    }

    private boolean existGroup(String stream, String group) {
        try {
            Response groups = redisAPI.xinfoAndAwait(List.of("GROUPS", stream));
            return StreamSupport.stream(groups.spliterator(), false).map(r -> r.get("name")).map(Response::toString).anyMatch(group::equals);
        } catch (Exception e) {
            // ha nincs kulcs akkor a kovetkezo hiba jon:
            // redis.clients.jedis.exceptions.JedisDataException: ERR no such key
            Log.infov("Redis exception duringchecking group [{0}] on stream [{1}]: [{2}]", group, stream, e.getLocalizedMessage());
            return false;
        }
    }

}
