package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.jboss.logging.Logger;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.StreamEntry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

/**
 * Microprofile Reactive Streams connector for Redis Streams integration.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
@ApplicationScoped
@Connector(RedisStreamsConnector.ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR)
@ConnectorAttribute(name = RedisStreamsConnector.REDIS_STREAM_CONNECTION_KEY_CONFIG, description = "The redis connection key to use",
        defaultValue = RedisStreamsProducer.DEFAULT_CONNECTION_KEY, type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "stream-key", description = "The Redis key holding the stream items", mandatory = true, type = "string",
        direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "payload-field", description = "The stream entry field name containing the message payload", type = "string",
        defaultValue = "message", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "group", description = "The consumer group of the Redis stream to read from", mandatory = true, type = "string",
        direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xread-count", description = "The maximum number of entries to receive upon an XREADGROUP call", type = "int",
        defaultValue = "1", direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xread-block-ms", description = "The milliseconds to wait in an XREADGROUP call", type = "int", defaultValue = "5000",
        direction = ConnectorAttribute.Direction.INCOMING)
@ConnectorAttribute(name = "xadd-maxlen", description = "The maximum number of entries to keep in the stream", type = "int",
        direction = ConnectorAttribute.Direction.OUTGOING)
@ConnectorAttribute(name = "xadd-exact-maxlen", description = "Use exact trimming for MAXLEN parameter", type = "boolean", defaultValue = "false",
        direction = ConnectorAttribute.Direction.OUTGOING)
@ConnectorAttribute(name = "xadd-ttl-ms", description = "Milliseconds to keep an entry in the stream", type = "long",
        direction = ConnectorAttribute.Direction.OUTGOING)
public class RedisStreamsConnector implements InboundConnector, OutboundConnector {

    /**
     * The name of the Redis Streams connector.
     */
    public static final String ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR = "icellmobilsoft-redis-streams";

    /**
     * The microprofile config key used to specify the Redis connection key.
     */
    public static final String REDIS_STREAM_CONNECTION_KEY_CONFIG = "connection-key";

    @Inject
    private Logger log;

    private final RedisStreamsProducer redisStreamsProducer;
    private String consumer;
    private volatile boolean consumerCancelled = false;
    private volatile boolean logSubscription = true;
    private final List<Flow.Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final Set<String> underProcessing = Collections.synchronizedSet(new HashSet<>());
    private final ReducableSemaphore shutdownPermit = new ReducableSemaphore(1);
    private final Integer gracefulShutdownTimeout;

    /**
     * Constructs a RedisStreamsConnector with the specified CDI RedisStreamsProducer.
     *
     * @param redisStreamsProducer
     *            the RedisStreamsProducer to be injected
     */
    @Inject
    public RedisStreamsConnector(RedisStreamsProducer redisStreamsProducer,
            @ConfigProperty(name = ConnectorFactory.CONNECTOR_PREFIX + ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR + ".graceful-timeout-ms",
                    defaultValue = "60000") Integer gracefulShutdownTimeout) {
        this.redisStreamsProducer = redisStreamsProducer;
        this.gracefulShutdownTimeout = gracefulShutdownTimeout;
    }

    /**
     * Initializes the connector, setting a unique consumer ID.
     */
    @PostConstruct
    public void init() {
        this.consumer = UUID.randomUUID().toString();
    }

    /**
     * Closes the connector, cancelling all subscriptions.
     *
     * @param ignored
     *            the shutdown event
     */
    public void terminate(@Observes(notifyObserver = Reception.IF_EXISTS) @Priority(10000) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        close();
    }

    public void close() {
        if (consumerCancelled) {
            return;
        }
        consumerCancelled = true;
        // cancel all subscriptions, don't read new messages from redis
        subscriptions.forEach(Flow.Subscription::cancel);
        // wait for all messages to be processed
        try {
            if (!shutdownPermit.tryAcquire(gracefulShutdownTimeout, TimeUnit.MILLISECONDS)) {
                log.warnv("There are still messages under processing: [{0}] after graceful timeout", underProcessing);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Gets the {@link Flow.Publisher} for the specified configuration.
     * <p>
     * This method is responsible for creating a publisher that reads messages from a Redis stream based on the provided configuration. It initializes
     * the necessary Redis connection and consumer group, and sets up the message reading process.
     * </p>
     *
     * @param config
     *            the configuration for the Redis stream, must not be {@code null}. The configuration should include:
     *            <ul>
     *            <li>{@code stream-key}: The Redis key holding the stream items.</li>
     *            <li>{@code group}: The consumer group of the Redis stream to read from.</li>
     *            <li>{@code connection-key}: The Redis connection key to use.</li>
     *            </ul>
     * @return the publisher that reads messages from the Redis stream, will not be {@code null}.
     */
    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        RedisStreamsConnectorIncomingConfiguration incomingConfig = new RedisStreamsConnectorIncomingConfiguration(config);
        String streamKey = incomingConfig.getStreamKey();
        String group = incomingConfig.getGroup();

        RedisStreams redisAPI = redisStreamsProducer.produce(incomingConfig.getConnectionKey());
        if (!redisAPI.existGroup(streamKey, group)) {
            redisAPI.xGroupCreate(streamKey, group);
            log.infov("Created consumer group [{0}] on redis stream [{1}]", group, streamKey);
        }
        return xreadMulti(redisAPI, incomingConfig);
    }

    /**
     * Creates a Multi for reading messages from the Redis stream. This method sets up a repeating Uni that reads messages from the Redis stream
     * indefinitely. It processes the messages, filters out expired ones, converts them to Message objects, and handles failures with retries.
     *
     * @param redisAPI
     *            the RedisStreams instance used to interact with the Redis stream
     * @param incomingConfig
     *            the configuration for the incoming Redis stream, including stream key, group, and other settings
     * @return the Multi for reading messages from the Redis stream
     */
    private Multi<Message<Object>> xreadMulti(RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        return Multi.createBy()
                // Multi creation
                .repeating()
                .uni(() -> xReadMessage(redisAPI, incomingConfig))
                .indefinitely()
                .flatMap(l -> Multi.createFrom().iterable(l))
                // Log first or recovered subscription to multi
                .onSubscription()
                .invoke(() -> {
                    if (logSubscription) {
                        log.infov(
                                "Subscribing channel [{0}] to redis stream [{1}] consuming group [{2}] as consumer [{3}] using redis connection key [{4}]",
                                incomingConfig.getChannel(),
                                incomingConfig.getStreamKey(),
                                incomingConfig.getGroup(),
                                consumer,
                                incomingConfig.getConnectionKey());
                        // don't log subscription again
                        logSubscription = false;
                    }
                })
                // Collect subscriptions in order to cancel them on shutdown
                .onSubscription()
                .invoke(subscriptions::add)
                // On item
                .filter(Objects::nonNull)
                .invoke(r -> log.tracev("Message received from redis-stream: [{0}]", r))
                // reduce shutdown permits to ensure graceful shutdown
                .invoke(streamEntry -> shutdownPermit.reducePermits(1))
                // keep the consumed message ids for logging
                .invoke(streamEntry -> underProcessing.add(streamEntry.id()))
                .flatMap(entity -> {
                    if (notExpired(entity)) {
                        return Multi.createFrom().item(entity);
                    } else {
                        // if it is expired, ack it immediately
                        return ack(entity, redisAPI, incomingConfig).map(i -> entity).toMulti();
                    }
                })
                // skip expired messages after they have been acked
                .filter(this::notExpired)
                // Convert to MP message
                .map(streamEntry -> toMessage(redisAPI, incomingConfig, streamEntry))
                // on failure log and retry
                .onFailure(t -> {
                    if (consumerCancelled) {
                        log.infov(t, "Exception occurred on already cancelled channel:[{0}], skipping retry", incomingConfig.getChannel());
                    } else {
                        log.errorv(
                                t,
                                "Uncaught exception while processing messages from channel [{0}], trying to recover..",
                                incomingConfig.getChannel());
                        // ensure that the subscription is logged again to have information on recovery
                        logSubscription = true;
                    }
                    // try to recover only if the consumer is not cancelled
                    return !consumerCancelled;
                })
                .retry()
                .withBackOff(Duration.of(1, ChronoUnit.SECONDS), Duration.of(30, ChronoUnit.SECONDS))
                .indefinitely()
                // Mark the connector as cancelled
                .onCancellation()
                .invoke(() -> {
                    consumerCancelled = true;
                    log.tracev("Subscription for channel [{0}] has been cancelled", incomingConfig.getChannel());
                });
    }

    /**
     * Converts a StreamEntry to a microprofile recitve streams message and metadata.
     *
     * @param redisAPI
     *            the RedisStreams instance
     * @param incomingConfig
     *            the incoming configuration
     * @param streamEntry
     *            the stream entry
     * @return the message
     */
    private Message<Object> toMessage(RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig, StreamEntry streamEntry) {
        String payloadField = incomingConfig.getPayloadField();
        Object payload = null;
        IncomingRedisStreamMetadata incomingRedisStreamMetadata = new IncomingRedisStreamMetadata(streamEntry.stream(), streamEntry.id());
        if (streamEntry.fields() != null) {
            for (Map.Entry<String, String> field : streamEntry.fields().entrySet()) {
                String key = field.getKey();
                String value = field.getValue();
                if (incomingConfig.getPayloadField().equals(key)) {
                    payload = value;
                } else {
                    incomingRedisStreamMetadata.getAdditionalFields().put(key, value);
                }
            }
        }
        if (payload == null) {
            log.warnv("Could not extract message payload from field {0} on entry [{1}]", payloadField, streamEntry);
        }
        return ContextAwareMessage.of(payload)
                .withAck(() -> ack(streamEntry, redisAPI, incomingConfig).subscribeAsCompletionStage())
                .addMetadata(incomingRedisStreamMetadata);
    }

    /**
     * Checks if a stream entry has not expired.
     *
     * @param streamEntry
     *            the stream entry
     * @return true if the entry has not expired, false otherwise
     */
    private boolean notExpired(StreamEntry streamEntry) {
        if (streamEntry.fields() != null && !streamEntry.fields().isEmpty() && streamEntry.fields().containsKey("ttl")) {
            String ttl = streamEntry.fields().get("ttl");
            try {
                Instant expirationTime = Instant.ofEpochMilli(Long.parseLong(ttl));
                return expirationTime.isAfter(Instant.now());
            } catch (NumberFormatException e) {
                log.warnv(e, "Could not parse ttl:[{0}] as epoch millis", ttl);
                return true;
            }
        }
        return true;
    }

    /**
     * Send ACK after successful processing of a stream entry.
     *
     * @param streamEntry
     *            the stream entry
     * @param redisAPI
     *            the RedisStreams instance
     * @param incomingConfig
     *            the incoming configuration
     * @return a CompletionStage representing the acknowledgment
     */
    private Uni<Void> ack(StreamEntry streamEntry, RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        Uni<Integer> integerUni = redisAPI.xAck(incomingConfig.getStreamKey(), incomingConfig.getGroup(), streamEntry.id());
        return integerUni
                .invoke(result -> log.tracev("ACK completed for id [{0}] with result [{1}]", streamEntry.id(), result)
                )
                .onFailure()
                .recoverWithItem(throwable -> {
                    log.errorv(
                            throwable,
                            "ACK failed for entry:[{0}] on channel: [{1}] by consumer group:[{2}]",
                            streamEntry.id(),
                            incomingConfig.getChannel(),
                            incomingConfig.getGroup());
                    return null;
                })
                .replaceWithVoid()
                // return permit after item or failure
                .invoke(() -> {
                    underProcessing.remove(streamEntry.id());
                    shutdownPermit.release();
                });
    }

    /**
     * Reads messages from the Redis stream using the XREADGROUP command. This method sets up a Uni that reads messages from the Redis stream for the
     * specified consumer group. It handles Redis connection errors and logs any issues that occur during the read operation.
     *
     * @param redisAPI
     *            the RedisStreams instance used to interact with the Redis stream
     * @param incomingConfig
     *            the configuration for the incoming Redis stream, including stream key, group, and other settings
     * @return a Uni containing a list of stream entries read from the Redis stream
     */
    private Uni<List<StreamEntry>> xReadMessage(RedisStreams redisAPI, RedisStreamsConnectorIncomingConfiguration incomingConfig) {
        return redisAPI
                .xReadGroup(
                        incomingConfig.getStreamKey(),
                        incomingConfig.getGroup(),
                        consumer,
                        incomingConfig.getXreadCount(),
                        incomingConfig.getXreadBlockMs())
                // Redis connection error while waiting for XREADGROUP response
                .onFailure()
                .invoke(
                        e -> log.errorv(
                                e,
                                "Redis error occured while waiting for XREADGROUP on channel [{0}], error: [{1}]",
                                incomingConfig.getChannel(),
                                e.getMessage())
                )
                .onTermination()
                .invoke(
                        (items, throwable, isCancelled) -> log.tracev(
                                "Terminating XREADGROUP call on channel [{0}], items: [{1}], throwable:[{2}], isCancelled:[{3}]",
                                incomingConfig.getChannel(),
                                items,
                                throwable,
                                isCancelled)
                );
    }

    /**
     * Gets the subscriber for the specified configuration.
     * <p>
     * This method is responsible for creating a {@link Flow.Subscriber} that will handle outgoing messages to a Redis stream. The configuration
     * provided is used to set up the connection and behavior of the subscriber.
     * </p>
     *
     * @param config
     *            the configuration for the subscriber, must not be {@code null}.
     * @return the subscriber that will handle outgoing messages to the Redis stream.
     * @throws IllegalArgumentException
     *             if the configuration is invalid.
     */
    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        RedisStreamsConnectorOutgoingConfiguration outgoingConfig = new RedisStreamsConnectorOutgoingConfiguration(config);
        RedisStreams redisAPI = redisStreamsProducer.produce(outgoingConfig.getConnectionKey());
        Optional<Long> ttlMsOpt = outgoingConfig.getXaddTtlMs();
        if (outgoingConfig.getXaddMaxlen().isPresent() && ttlMsOpt.isPresent()) {
            log.warnv("When both xadd-maxlen and xadd-ttl-ms is set only maxlen will be used!");
        }
        return MultiUtils.via(multi -> multi.onItem().transformToUniAndMerge(message -> {
            log.tracev("Sending message payload:[{0}] to redis stream:[{1}]", message.getPayload(), outgoingConfig.getStreamKey());
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
            streamEntryFields.put(outgoingConfig.getPayloadField(), message.getPayload().toString());
            if (fieldTtl != null) {
                streamEntryFields.put("ttl", fieldTtl);
            }
            Optional<RedisStreamMetadata> redisStreamMetadata = message.getMetadata().get(RedisStreamMetadata.class);
            if (redisStreamMetadata.isPresent()) {
                Map<String, String> additionalFields = redisStreamMetadata.get().getAdditionalFields();
                for (Map.Entry<String, String> entry : additionalFields.entrySet()) {
                    String valuePresent = streamEntryFields.putIfAbsent(entry.getKey(), entry.getValue());
                    if (valuePresent != null) {
                        log.warnv(
                                "Ignoring RedisStreamMetadata.additionalFields entry key:[{0}] with value: [{1}] since key is already present with value:[{2}]",
                                entry.getKey(),
                                entry.getValue(),
                                valuePresent);
                    }
                }
            }
            return redisAPI.xAdd(
                    outgoingConfig.getStreamKey(),
                    "*",
                    outgoingConfig.getXaddMaxlen().orElse(null),
                    outgoingConfig.getXaddExactMaxlen(),
                    minId,
                    streamEntryFields);
        }));
    }

}
