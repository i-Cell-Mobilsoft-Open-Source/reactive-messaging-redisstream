package hu.icellmobilsoft.reactive.messaging.redis.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.inject.Inject;

import org.jboss.logging.JBossLogManagerProvider;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldJunit5Extension;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.ShellStrategy;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.core.ConditionFactory;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.TestLettuceRedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.TestLettuceRedisStreamsProducer;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.models.stream.PendingMessages;

/**
 * Test class for {@link RedisStreamsConnector}
 */
@EnableWeld
@Tag("weld")
@ExtendWith(WeldJunit5Extension.class)
public class RedisStreamsConnectorTest {
    private static final int REDIS_PORT = 6379;
    public static final String TEST__CONSUMER_GROUP = "test-group";
    public static final String TEST_CONSUMER_ID = "test-consumer";
    public static final String DEFAULT_MESSAGE_KEY = "message";
    public static final String ZERO_OFFSET = "0-0";
    /**
     * Redis docker container.
     */
    static GenericContainer<?> REDIS_CONTAINER;

    @Inject
    TestConsumer testConsumer;
    @Inject
    TestProducer testProducer;
    @WeldSetup
    public WeldInitiator weld = weldSetup();

    private WeldInitiator weldSetup() {
        return WeldInitiator.from(
                WeldInitiator.createWeld()
                        .addBeanClass(TestConsumer.class)
                        .addBeanClass(TestProducer.class)
                        .addBeanClass(TestLettuceRedisStreams.class)
                        .addBeanClass(TestLettuceRedisStreamsProducer.class)
                        .addBeanClass(RedisStreamsConnector.class)
                        .addBeanClass(JBossLogManagerProvider.class)
                        // beans.xml scan
                        .enableDiscovery())
                .build();
    }

    /**
     * Start redis docker container.
     */
    @BeforeAll
    public static void startRedisContainer() {
        REDIS_CONTAINER = new GenericContainer<>("redis:7.0.1-alpine")
                .withExposedPorts(REDIS_PORT)
                .waitingFor(new ShellStrategy().withCommand("redis-cli --raw incr ping"));

        REDIS_CONTAINER.start();
        // set mp config redis port
        System.setProperty(TestLettuceRedisStreamsProducer.TEST_REDIS_PORT_KEY, String.valueOf(REDIS_CONTAINER.getMappedPort(REDIS_PORT)));
    }

    /**
     * Stop redis docker container.
     */
    @AfterAll
    public static void stopRedisContainer() {
        if (REDIS_CONTAINER != null) {
            REDIS_CONTAINER.stop();
        }
    }

    /**
     * Test consumer.
     */
    @Test
    void testConsumer() {
        String streamKey = "in-stream";
        // given we have redis
        try (RedisClient redisClient = connectToRedisContainer()) {
            // when we add a message to the stream
            String payload = "Hello";
            String messageId = redisClient.connect().sync().xadd(streamKey, Map.of(DEFAULT_MESSAGE_KEY, payload));
            // then the consumer should receive the message eventually
            ConditionFactory await = Awaitility.await();
            await.atMost(1, TimeUnit.SECONDS)
                    .until(() -> testConsumer.getMessages().size() > 0);
            Assertions.assertEquals(payload, testConsumer.getMessages().get(0));

            // And the message should be removed from the stream
            List<StreamMessage<String, String>> xreadgroup = redisClient.connect()
                    .sync()
                    .xreadgroup(Consumer.from(TEST__CONSUMER_GROUP, TEST_CONSUMER_ID), XReadArgs.StreamOffset.from(streamKey, ZERO_OFFSET));
            assertThat(xreadgroup).map(StreamMessage::getId).doesNotContain(messageId);

            // And the message should be acknowledged
            PendingMessages pending = redisClient.connect().sync().xpending(streamKey, TEST__CONSUMER_GROUP);
            assertThat(pending.getCount()).isZero();
        }
    }

    private RedisClient connectToRedisContainer() {
        return RedisClient.create(RedisURI.create("localhost", REDIS_CONTAINER.getMappedPort(REDIS_PORT)));
    }

    @Test
    void testConsumerReactive() {
        String streamKey = "in-stream-reactive";
        // given we have redis
        try (RedisClient redisClient = connectToRedisContainer()) {
            // when we add a message to the stream
            String payload = "Hello";
            String additionalFieldKey = "consumer.additionalFieldKey";
            String additionalFieldValue = "consumer.additionalFieldValue";

            String messageId = redisClient.connect()
                    .sync()
                    .xadd(streamKey, Map.of(DEFAULT_MESSAGE_KEY, payload, additionalFieldKey, additionalFieldValue));
            // then the consumer should receive the message eventually and have a metadata class
            ConditionFactory await = Awaitility.await();
            await.atMost(1, TimeUnit.SECONDS)
                    .until(() -> testConsumer.getMetadataMessages().size() > 0);

            assertThat(testConsumer.getMetadataMessages()).anySatisfy(sm -> {
                assertThat(sm).extracting(TestConsumer.MessageWithMetadata::getMessage).isEqualTo(payload);
                assertThat(sm).extracting(TestConsumer.MessageWithMetadata::getId).isEqualTo(messageId);
                assertThat(sm).extracting(TestConsumer.MessageWithMetadata::getMetadata)
                        .extracting(m -> m.get(additionalFieldKey))
                        .isEqualTo(additionalFieldValue);
            });
            // And the message should be removed from the stream
            List<StreamMessage<String, String>> xreadgroup = redisClient.connect()
                    .sync()
                    .xreadgroup(
                            Consumer.from(TEST__CONSUMER_GROUP, TEST_CONSUMER_ID),
                            XReadArgs.Builder.block(1000),
                            XReadArgs.StreamOffset.from(streamKey, ZERO_OFFSET));
            assertThat(xreadgroup).map(StreamMessage::getId).doesNotContain(messageId);
            // And the message should be acknowledged
            PendingMessages pending = redisClient.connect().sync().xpending(streamKey, TEST__CONSUMER_GROUP);
            assertThat(pending.getCount()).isZero();
        }
    }

    /**
     * Test producer.
     */
    @Test
    void testProducer() {
        // given we have redis
        String streamKey = "out-stream";
        try (RedisClient redisClient = connectToRedisContainer()) {
            RedisFuture<List<StreamMessage<String, String>>> readResult = redisClient.connect()
                    .async()
                    .xread(
                            XReadArgs.Builder.block(Duration.of(1, TimeUnit.MINUTES.toChronoUnit())),
                            XReadArgs.StreamOffset.from(streamKey, ZERO_OFFSET));
            // when we produce a message
            String message = "Test-Produce";
            testProducer.produce(message);
            // then the message should be in the stream
            List<StreamMessage<String, String>> streamMessages = readResult.get(2, TimeUnit.SECONDS);
            assertThat(streamMessages).hasSizeGreaterThanOrEqualTo(1);
            assertThat(streamMessages).anySatisfy(sm -> {
                assertThat(sm).extracting(StreamMessage::getStream).isEqualTo(streamKey);
                assertThat(sm).extracting(StreamMessage::getBody).extracting(m -> m.get(DEFAULT_MESSAGE_KEY)).isEqualTo(message);
                assertThat(sm).extracting(StreamMessage::getBody).extracting(m -> m.get("ttl")).isNotNull();
            }
            );
        } catch (ExecutionException | TimeoutException | InterruptedException e) {
            fail("Error occurred during producer test", e);
        }
    }

    /**
     * Test producer with metadata.
     */
    @Test
    void testProducerWithMetadata() {
        // given we have redis
        String streamKey = "out-with-metadata-stream";
        try (RedisClient redisClient = connectToRedisContainer()) {
            RedisFuture<List<StreamMessage<String, String>>> readResult = redisClient.connect()
                    .async()
                    .xread(
                            XReadArgs.Builder.block(Duration.of(1, TimeUnit.MINUTES.toChronoUnit())),
                            XReadArgs.StreamOffset.from(streamKey, ZERO_OFFSET));
            String message = "Test-Produce";
            String additionalField = "Test-additionalField";
            // when we produce a message
            testProducer.produceWithMetadata(message, additionalField);
            // then the message should be in the stream
            List<StreamMessage<String, String>> streamMessages = readResult.get(2, TimeUnit.SECONDS);
            assertThat(streamMessages).hasSizeGreaterThanOrEqualTo(1);
            assertThat(streamMessages).anySatisfy(sm -> {
                assertThat(sm).extracting(StreamMessage::getStream).isEqualTo(streamKey);
                assertThat(sm).extracting(StreamMessage::getBody).extracting(m -> m.get(DEFAULT_MESSAGE_KEY)).isEqualTo(message);
                assertThat(sm).extracting(StreamMessage::getBody)
                        .extracting(m -> m.get(TestProducer.ADDITIONAL_FIELD_KEY))
                        .isEqualTo(additionalField);
            }
            );
        } catch (ExecutionException | TimeoutException | InterruptedException e) {
            fail("Error occurred during producer with metadata test", e);
        }
    }

}
