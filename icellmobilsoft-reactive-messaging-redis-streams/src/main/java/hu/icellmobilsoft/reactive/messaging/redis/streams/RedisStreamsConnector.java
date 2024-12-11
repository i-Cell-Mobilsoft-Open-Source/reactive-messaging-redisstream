package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.stream.StreamSupport;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.QuarkusRedisStreamsAdapter;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.StreamEntry;
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
    protected QuarkusRedisStreamsAdapter redisAPI;
    private String group;
    private String streamKey;
    private String consumer;

    @PostConstruct
    void init() {
        this.group = "group";
        this.streamKey = "stream";
        this.consumer = "consumer";
        if (!redisAPI.existGroup(streamKey, group)) {
            String create = redisAPI.xGroupCreate(streamKey, group);
            Log.info(create);
        }
    }


    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        return Multi.createBy().repeating().uni(this::xReadMessage).indefinitely().flatMap(l -> Multi.createFrom().iterable(l)).filter(Objects::nonNull)
                .invoke(r -> Log.infov("msg received: [{0}]", r)).map(Message::of);
    }

    private Uni<List<StreamEntry>> xReadMessage() {
        List<String> xread = List.of("GROUP", group, consumer, "COUNT", "1", "BLOCK", "30000", "STREAMS", streamKey, ">");

        return redisAPI.xReadGroup(streamKey, group, consumer, 1, 2000)
                .invoke(r -> Log.infov("XREADGROUP called with response: [{0}]", r))
                ;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        return MultiUtils.via(multi -> multi.onItem().transformToUniAndConcatenate(message -> {
                            Log.infov("msg sent:[{0}]", message.getPayload());
                            return redisAPI.xAdd(streamKey, Map.of("payload", message.getPayload().toString(), "timestamp", LocalDateTime.now().toString()));
                        }
                )
        );
    }

}
