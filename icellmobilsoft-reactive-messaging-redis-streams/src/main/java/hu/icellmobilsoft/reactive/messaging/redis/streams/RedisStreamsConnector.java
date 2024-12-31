package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
@ConnectorAttribute(name = "xread-count", description = "The maximum number of entries to receive upon an XREADGROUP call", type = "int", defaultValue = "1", direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "retry", description = "The number of times the  consumer should retry", type = "int", defaultValue = "1", direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xread-block-ms", description = "The milliseconds to wait in an XREADGROUP call", type = "int", defaultValue = "5000", direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xadd-maxlen", description = "The maximum number of entries to keep in the stream", type = "int", direction = ConnectorAttribute.Direction.OUTGOING)
@ConnectorAttribute(name = "xadd-exact-maxlen", description = "Use exact trimming for MAXLEN parameter", type = "boolean", defaultValue = "false", direction = ConnectorAttribute.Direction.OUTGOING)
@ConnectorAttribute(name = "xadd-ttl-ms", description = "Milliseconds to keep an entry in the stream", type = "long", direction = ConnectorAttribute.Direction.OUTGOING)
public class RedisStreamsConnector implements InboundConnector, OutboundConnector {


    public static final String ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR = "icellmobilsoft-redis-streams";
    public static final String REDIS_STREAM_CONNECTION_KEY_CONFIG = "connection-key";
    @Inject
    protected RedisStreamsProducer redisStreamsProducer;
    private String consumer;

    @PostConstruct
    void init() {
        //TODO random
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
        return xreadMulti(redisAPI, incomingConfig);
    }

    private Multi<Message<StreamEntry>> xreadMulti(RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        return Multi.createBy().repeating().uni(() -> xReadMessage(redisAPI, incomingConfig)).indefinitely().flatMap(l -> Multi.createFrom().iterable(l))
                .filter(Objects::nonNull)
                .filter(this::notExpired)
                .invoke(r -> Log.infov("msg received: [{0}]", r)).map(streamEntry ->
                        Message.of(streamEntry, m -> ack(streamEntry, redisAPI, incomingConfig)))
                //TODO itt valami el van baszva
//                .onFailure().retry().atMost(incomingConfig.getRetry())
                .onFailure().recoverWithMulti(error -> {
                    Log.error("Uncaught exception while processing messages, trying to recover..", error);
                    return xreadMulti(redisAPI, incomingConfig);
                })
                ;
    }

    private boolean notExpired(StreamEntry streamEntry) {
        if (streamEntry.fields() != null && !streamEntry.fields().isEmpty() && streamEntry.fields().containsKey("ttl")) {
            String ttl = streamEntry.fields().get("ttl");
            try {
                Instant expirationTime = Instant.ofEpochMilli(Long.parseLong(ttl));
                return expirationTime.isAfter(Instant.now());
            } catch (NumberFormatException e) {
                Log.warnv(e, "Could not parse ttl:[{0}] as epoch millis", ttl);
                return true;
            }
        }
        return true;
    }

    private CompletionStage<Void> ack(StreamEntry streamEntry, RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        Uni<Integer> integerUni = redisAPI.xAck(incomingConfig.getStreamKey(), incomingConfig.getGroup(), streamEntry.id());
        return integerUni.replaceWithVoid().subscribeAsCompletionStage();
    }

    private Uni<List<StreamEntry>> xReadMessage(RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        return redisAPI.xReadGroup(incomingConfig.getStreamKey(), incomingConfig.getGroup(), consumer, incomingConfig.getXreadCount(), incomingConfig.getXreadBlockMs())
                .invoke(r -> Log.infov("XREADGROUP called with response: [{0}]", r)).onCancellation().invoke(() -> Log.infov("XREADGROUP cancelled"))
                ;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        RedisStreamsConnectorOutgoingConfiguration outgoingConfig = new RedisStreamsConnectorOutgoingConfiguration(config);
        RedisStreams redisAPI = redisStreamsProducer.produce(outgoingConfig.getConnectionKey());
        Optional<Long> ttlMsOpt = outgoingConfig.getXaddTtlMs();
        if (outgoingConfig.getXaddMaxlen().isPresent() && ttlMsOpt.isPresent()) {
            Log.warnv("When both xadd-maxlen and xadd-ttl-ms is set only maxlen will be used!");
        }
        return MultiUtils.via(multi -> multi.onItem().transformToUniAndConcatenate(message -> {
                            Log.infov("msg sent:[{0}]", message.getPayload());
                            String minId = null;
                            String fieldTtl = null;
                            if (ttlMsOpt.isPresent()) {
                                Long ttlMs = ttlMsOpt.get();
                                Long epochMilli = Instant.now().toEpochMilli();
                                // current message's business ttl (now + ttl)
                                fieldTtl = String.valueOf(epochMilli + ttlMs);
                                // last not expired id (now - ttl)
                                minId = String.valueOf(epochMilli - ttlMs);
                            }
                            Map<String, String> streamEntryFields = new HashMap<>();
                            //TODO json??
                            streamEntryFields.put("message", message.getPayload().toString());
                            if (fieldTtl != null) {
                                streamEntryFields.put("ttl", fieldTtl);
                            }
                            //TODO sid
                            // streamEntryFields.put("extSessionId", MDC.get());
                            return redisAPI.xAdd(outgoingConfig.getStreamKey(), "*", outgoingConfig.getXaddMaxlen().orElse(null), outgoingConfig.getXaddExactMaxlen(), minId,
                                    streamEntryFields);
                        }
                )
        );
    }

}
