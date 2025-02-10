package hu.icellmobilsoft.quarkus.extension.redisstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.core.ConditionFactory;

import hu.icellmobilsoft.reactive.messaging.redis.streams.RedisStreamsConnector;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.stream.ReactiveStreamCommands;
import io.quarkus.redis.datasource.stream.StreamMessage;
import io.quarkus.redis.datasource.stream.XReadArgs;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;

/**
 * Test class for {@link RedisStreamsConnector} quarkus extension.
 * 
 * @author mark.petrenyi
 * @since 1.0.0
 */
class RedisStreamsConnectorTest {

    public static final String TEST__CONSUMER_GROUP = "test-group";
    public static final String TEST_CONSUMER_ID = "test-consumer";
    public static final String DEFAULT_MESSAGE_KEY = "message";
    public static final String ZERO_OFFSET = "0-0";

    @RegisterExtension
    static final QuarkusUnitTest config = new QuarkusUnitTest().setArchiveProducer(
            () -> ShrinkWrap.create(JavaArchive.class)
                    .addClasses(TestConsumer.class, TestProducer.class))
            .withConfigurationResource("microprofile-config.properties");

    @Inject
    TestConsumer testConsumer;
    @Inject
    TestProducer testProducer;
    @Inject
    ReactiveRedisDataSource redisDataSource;
    @Inject
    RedisAPI redisAPI;

    /**
     * Test consumer.
     */
    @Test
    void testConsumer() {
        String streamKey = "in-stream";
        // when we add a message to the stream
        String payload = "Hello";
        ReactiveStreamCommands<String, String, String> streamCommand = redisDataSource.stream(String.class);
        String messageId = streamCommand.xadd(streamKey, Map.of(DEFAULT_MESSAGE_KEY, payload)).await().atMost(Duration.ofSeconds(1));
        // then the consumer should receive the message eventually
        ConditionFactory await = Awaitility.await();
        await.atMost(1, TimeUnit.SECONDS)
                .until(() -> testConsumer.getMessages().size() > 0);
        Assertions.assertEquals(payload, testConsumer.getMessages().get(0));

        // And the message should be removed from the stream and the message should be acknowledged
        assertThatMessageIsAckedOnRedis(
                messageId,
                streamCommand,
                streamKey);
    }

    @Test
    void testConsumerReactive() {
        String streamKey = "in-stream-reactive";
        // when we add a message to the stream
        String payload = "Hello";
        String additionalFieldKey = "consumer.additionalFieldKey";
        String additionalFieldValue = "consumer.additionalFieldValue";
        ReactiveStreamCommands<String, String, String> streamCommand = redisDataSource.stream(String.class);

        String messageId = streamCommand
                .xadd(streamKey, Map.of(DEFAULT_MESSAGE_KEY, payload, additionalFieldKey, additionalFieldValue))
                .await()
                .atMost(Duration.ofSeconds(1));
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
        // And the message should be removed from the stream and the message should be acknowledged
        assertThatMessageIsAckedOnRedis(
                messageId,
                streamCommand,
                streamKey);
    }

    /**
     * Test producer.
     */
    @Test
    void testProducer() {
        // given we have redis
        String streamKey = "out-stream";
        ReactiveStreamCommands<String, String, String> streamCommand = redisDataSource.stream(String.class);

        Uni<List<StreamMessage<String, String, String>>> readResult = streamCommand
                .xread(streamKey, ZERO_OFFSET, new XReadArgs().block(Duration.ofSeconds(1)));
        // when we produce a message
        String message = "Test-Produce";
        testProducer.produce(message);
        // then the message should be in the stream
        List<StreamMessage<String, String, String>> streamMessages = readResult.await().atMost(Duration.ofSeconds(2));
        assertThat(streamMessages).hasSizeGreaterThanOrEqualTo(1);
        assertThat(streamMessages).anySatisfy(sm -> {
            assertThat(sm).extracting(StreamMessage::key).isEqualTo(streamKey);
            assertThat(sm).extracting(StreamMessage::payload).extracting(m -> m.get(DEFAULT_MESSAGE_KEY)).isEqualTo(message);
            assertThat(sm).extracting(StreamMessage::payload).extracting(m -> m.get("ttl")).isNotNull();
        }
        );
    }

    /**
     * Test producer with metadata.
     */
    @Test
    void testProducerWithMetadata() {
        // given we have redis
        String streamKey = "out-with-metadata-stream";

        ReactiveStreamCommands<String, String, String> streamCommand = redisDataSource.stream(String.class);

        Uni<List<StreamMessage<String, String, String>>> readResult = streamCommand
                .xread(streamKey, ZERO_OFFSET, new XReadArgs().block(Duration.ofSeconds(1)));
        String message = "Test-Produce";
        String additionalField = "Test-additionalField";
        // when we produce a message
        testProducer.produceWithMetadata(message, additionalField);
        // then the message should be in the stream
        List<StreamMessage<String, String, String>> streamMessages = readResult.await().atMost(Duration.ofSeconds(2));
        assertThat(streamMessages).hasSizeGreaterThanOrEqualTo(1);
        assertThat(streamMessages).anySatisfy(sm -> {
            assertThat(sm).extracting(StreamMessage::key).isEqualTo(streamKey);
            assertThat(sm).extracting(StreamMessage::payload).extracting(m -> m.get(DEFAULT_MESSAGE_KEY)).isEqualTo(message);
            assertThat(sm).extracting(StreamMessage::payload)
                    .extracting(m -> m.get(TestProducer.ADDITIONAL_FIELD_KEY))
                    .isEqualTo(additionalField);
        }
        );
    }

    private void assertThatMessageIsAckedOnRedis(String messageId, ReactiveStreamCommands<String, String, String> streamCommand,
            String streamKey) {
        List<StreamMessage<String, String, String>> xreadgroup = streamCommand
                .xreadgroup(TEST__CONSUMER_GROUP, TEST_CONSUMER_ID, streamKey, ZERO_OFFSET)
                .await()
                .atMost(Duration.ofSeconds(1));
        assertThat(xreadgroup).map(StreamMessage::id).doesNotContain(messageId);

        // And the message should be acknowledged
        Response pending = redisAPI.xpending(List.of(streamKey, TEST__CONSUMER_GROUP)).await().atMost(Duration.ofSeconds(1));
        assertThat(pending).isNotNull();
        assertThat(pending.size()).isGreaterThanOrEqualTo(1);
        assertThat(pending.get(0)).isNotNull();
        assertThat(pending.get(0).toInteger()).isZero();
    }

}
