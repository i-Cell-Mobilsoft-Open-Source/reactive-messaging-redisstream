/*-
 * #%L
 * reactive-messaging-redisstream
 * %%
 * Copyright (C) 2025 i-Cell Mobilsoft Zrt.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package hu.icellmobilsoft.reactive.messaging.redis.streams;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory;
import org.junit.jupiter.api.Test;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.StreamEntry;
import hu.icellmobilsoft.reactive.messaging.redis.streams.converter.RedisStreamJsonbSerializer;
import hu.icellmobilsoft.reactive.messaging.redis.streams.metadata.RedisStreamMetadata;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

/**
 * Unit tests for the outgoing {@link RedisStreamsConnector#xAdd} decision logic.
 *
 * @since 1.4.0
 * @author kornel.danko
 */
class RedisStreamsConnectorXAddTest {

    private static final String STREAM_KEY = "test-stream";
    private static final String PAYLOAD_FIELD = "message";
    private static final String ADDITIONAL_FIELD_KEY = "otherKey";
    private static final String ADDITIONAL_FIELD_VALUE = "Other value";

    /**
     * Verifies that pipelined batch payloads use the batch {@code xAdd(List<StreamEntry>, ...)} overload instead of single-message writes.
     */
    @Test
    void shouldUseBatchXAddOverloadForPipelinedListPayload() {
        CapturingRedisStreams redisStreams = new CapturingRedisStreams();

        invokeXAdd(redisStreams, pipelinedBatchMessage(List.of("batch-1", "batch-2")), Optional.of(1000L));

        assertThat(redisStreams.batchXAddInvocationCount).isEqualTo(1);
        assertThat(redisStreams.singleXAddInvocationCount).isZero();
        assertThat(redisStreams.capturedBatchEntries)
                .extracting(StreamEntry::stream)
                .containsOnly(STREAM_KEY);
        assertThat(redisStreams.capturedBatchEntries)
                .extracting(entry -> entry.fields().get(PAYLOAD_FIELD))
                .containsExactly("batch-1", "batch-2");
    }

    /**
     * Verifies that connector-managed metadata fields are applied to each Redis entry created from a pipelined batch payload.
     */
    @Test
    void shouldApplyMetadataAndTtlToEveryPipelinedEntry() {
        CapturingRedisStreams redisStreams = new CapturingRedisStreams();

        invokeXAdd(redisStreams, pipelinedBatchMessage(List.of("batch-1", "batch-2", "batch-3")), Optional.of(1000L));

        assertThat(redisStreams.capturedBatchEntries).hasSize(3);
        assertThat(redisStreams.capturedBatchEntries)
                .extracting(entry -> entry.fields().get(ADDITIONAL_FIELD_KEY))
                .containsOnly(ADDITIONAL_FIELD_VALUE);
        assertThat(redisStreams.capturedBatchEntries)
                .extracting(entry -> entry.fields().get("ttl"))
                .allSatisfy(ttl -> assertThat(ttl).isNotBlank());
        assertThat(redisStreams.capturedBatchEntries)
                .extracting(entry -> entry.fields().get("ttl"))
                .doesNotContainNull()
                .hasSize(3);
        assertThat(redisStreams.capturedBatchEntries)
                .extracting(entry -> entry.fields().get("ttl"))
                .containsOnly(redisStreams.capturedBatchEntries.get(0).fields().get("ttl"));
    }

    private void invokeXAdd(CapturingRedisStreams redisStreams, Message<?> message, Optional<Long> ttlMsOpt) {
        TestableRedisStreamsConnector connector = new TestableRedisStreamsConnector();
        RedisStreamsConnectorOutgoingConfiguration outgoingConfig = new RedisStreamsConnectorOutgoingConfiguration(createConfig());

        List<String> ids = connector.invokeXAdd(redisStreams, outgoingConfig, ttlMsOpt, message)
                .await()
                .indefinitely();

        assertThat(ids).isNotEmpty();
    }

    private Message<List<String>> pipelinedBatchMessage(List<String> payload) {
        return ContextAwareMessage.of(payload)
                .addMetadata(new RedisStreamMetadata()
                        .withPipelined(true)
                        .withAdditionalField(ADDITIONAL_FIELD_KEY, ADDITIONAL_FIELD_VALUE));
    }

    private Config createConfig() {
        return new SmallRyeConfigBuilder()
                .withSources(new MapConfigSource(Map.of(
                        ConnectorFactory.CHANNEL_NAME_ATTRIBUTE, "test-channel",
                        "stream-key", STREAM_KEY,
                        "payload-field", PAYLOAD_FIELD,
                        "retries", "0")))
                .build();
    }

    private static class TestableRedisStreamsConnector extends RedisStreamsConnector {

        TestableRedisStreamsConnector() {
            super(connectionKey -> null, new RedisStreamJsonbSerializer(), 60000, null);
        }

        Uni<List<String>> invokeXAdd(RedisStreams redisStreams, RedisStreamsConnectorOutgoingConfiguration outgoingConfig, Optional<Long> ttlMsOpt,
                Message<?> message) {
            return super.xAdd(redisStreams, outgoingConfig, ttlMsOpt, message);
        }
    }

    private static class CapturingRedisStreams implements RedisStreams {

        private int singleXAddInvocationCount;
        private int batchXAddInvocationCount;
        private List<StreamEntry> capturedBatchEntries = List.of();

        @Override
        public Uni<Boolean> existGroup(String stream, String group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<String> xGroupCreate(String stream, String group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<Long> xAck(String stream, String group, String id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<String> xAdd(String stream, String id, Map<String, String> fields) {
            singleXAddInvocationCount++;
            return Uni.createFrom().item("1-0");
        }

        @Override
        public Uni<String> xAdd(String stream, String id, Integer maxLen, Boolean exact, String minId, Map<String, String> fields) {
            singleXAddInvocationCount++;
            return Uni.createFrom().item("1-0");
        }

        @Override
        public Uni<List<String>> xAdd(List<StreamEntry> entries, Integer maxLen, Boolean exact, String minId) {
            batchXAddInvocationCount++;
            capturedBatchEntries = List.copyOf(entries);
            return Uni.createFrom().item(entries.stream().map(entry -> "1-0").toList());
        }

        @Override
        public Uni<List<StreamEntry>> xReadGroup(String stream, String group, String consumer, Integer count, Integer blockMs, Boolean noack) {
            throw new UnsupportedOperationException();
        }
    }

    private record MapConfigSource(Map<String, String> values) implements ConfigSource {

        @Override
        public Map<String, String> getProperties() {
            return values;
        }

        @Override
        public Set<String> getPropertyNames() {
            return values.keySet();
        }

        @Override
        public int getOrdinal() {
            return 100;
        }

        @Override
        public String getValue(String propertyName) {
            return values.get(propertyName);
        }

        @Override
        public String getName() {
            return "test-config";
        }
    }
}
