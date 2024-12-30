package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.StreamEntry;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

@ApplicationScoped
@Connector(RedisStreamsConnector.ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR)
@ConnectorAttribute(name = RedisStreamsConnector.REDIS_STREAM_CONNECTION_KEY_CONFIG, description = "The redis connection key to use", defaultValue = RedisStreamsProducer.DEFAULT_CONNECTION_KEY, type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "stream-key", description = "The Redis key holding the stream items", mandatory = true, type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "group", description = "The consumer group of the Redis stream to read from", mandatory = true, type = "string", direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xread-count", description = "COUNT parameter of XREADGROUP command", type = "int", defaultValue = "1", direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xread-block-ms", description = "BLOCK parameter of XREADGROUP command", type = "int", defaultValue = "5000", direction = ConnectorAttribute.Direction.INCOMING)
public class RedisStreamsConnector implements InboundConnector, OutboundConnector {


    public static final String ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR = "icellmobilsoft-redis-streams";
    public static final String REDIS_STREAM_CONNECTION_KEY_CONFIG = "connection-key";
    @Inject
    protected RedisStreamsProducer redisStreamsProducer;
    private String consumer;

    @PostConstruct
    void init() {
        this.consumer = "consumer";
    }


    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        RedisStreamsConnectorIncomingConfiguration incomingConfig = new RedisStreamsConnectorIncomingConfiguration(config);
        String streamKey = incomingConfig.getStreamKey();
        String group = incomingConfig.getGroup();

        RedisStreams redisAPI = redisStreamsProducer.produce(incomingConfig.getConnectionKey());
        if (!redisAPI.existGroup(streamKey, group)) {
            String create = redisAPI.xGroupCreate(streamKey, group);
            Log.info(create);
        }
        return Multi.createBy().repeating().uni(() -> xReadMessage(redisAPI, incomingConfig)).indefinitely().flatMap(l -> Multi.createFrom().iterable(l)).filter(Objects::nonNull)
                .invoke(r -> Log.infov("msg received: [{0}]", r)).map(streamEntry ->
                        Message.of(streamEntry, m -> ack(streamEntry, redisAPI, incomingConfig)));
    }

    private CompletionStage<Void> ack(StreamEntry streamEntry, RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        Uni<Integer> integerUni = redisAPI.xAck(incomingConfig.getStreamKey(), incomingConfig.getGroup(), streamEntry.id());
        return integerUni.replaceWithVoid().subscribeAsCompletionStage();
    }

    private Uni<List<StreamEntry>> xReadMessage(RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        return redisAPI.xReadGroup(incomingConfig.getStreamKey(), incomingConfig.getGroup(), consumer, incomingConfig.getXreadCount(), incomingConfig.getXreadBlockMs())
                .invoke(r -> Log.infov("XREADGROUP called with response: [{0}]", r)).onCancellation().invoke(() -> Log.infov("XREADGROUP cancelled"));
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        RedisStreamsConnectorOutgoingConfiguration outgoingConfig = new RedisStreamsConnectorOutgoingConfiguration(config);
        RedisStreams redisAPI = redisStreamsProducer.produce(outgoingConfig.getConnectionKey());
        return MultiUtils.via(multi -> multi.onItem().transformToUniAndConcatenate(message -> {
                            Log.infov("msg sent:[{0}]", message.getPayload());
                            return redisAPI.xAdd(outgoingConfig.getStreamKey(), Map.of("payload", message.getPayload().toString(), "timestamp", LocalDateTime.now().toString()));
                        }
                )
        );
    }

}
