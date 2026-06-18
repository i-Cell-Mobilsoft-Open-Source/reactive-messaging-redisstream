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

import java.util.List;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import hu.icellmobilsoft.reactive.messaging.redis.streams.dto.TestDto;
import hu.icellmobilsoft.reactive.messaging.redis.streams.metadata.RedisStreamMetadata;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

/**
 * Test bean for producing redis messages.
 *
 * @author mark.petrenyi
 * @since 1.0.0
 */
@ApplicationScoped
public class TestProducer {

    public static final String ADDITIONAL_FIELD_KEY = "TestProducer_additionalField";
    /**
     * The Emitter.
     */
    @Inject
    @Channel("out")
    Emitter<String> emitter;

    /**
     * The Emitter with metadata.
     */
    @Inject
    @Channel("out-with-metadata")
    Emitter<String> emitterWithMetadata;

    /**
     * The DTO Emitter.
     */
    @Inject
    @Channel("out-dto")
    Emitter<TestDto> dtoEmitter;

    /**
     * The reactive DTO Emitter.
     */
    @Inject
    @Channel("out-dto-reactive")
    Emitter<TestDto> dtoReactiveEmitter;

    @Inject
    @Channel("out-batch-list")
    Emitter<List<String>> batchListEmitter;

    @Inject
    @Channel("out-batch-stream")
    Emitter<Stream<String>> batchStreamEmitter;

    /**
     * Produce simple message.
     *
     * @param message
     *            the message
     */
    public void produce(final String message) {
        emitter.send(message);
    }

    /**
     * Produce message with additional field.
     *
     * @param message
     *            the message
     * @param additionalField
     *            the additional field
     */
    public void produceWithMetadata(final String message, String additionalField) {
        emitterWithMetadata.send(
                ContextAwareMessage.of(message)
                        .addMetadata(new RedisStreamMetadata().withAdditionalField(ADDITIONAL_FIELD_KEY, additionalField)));
    }

    /**
     * Produce DTO message.
     *
     * @param dto
     *            the DTO to send
     */
    public void produceDto(final TestDto dto) {
        dtoEmitter.send(dto);
    }

    /**
     * Produce DTO message through the reactive channel.
     *
     * @param dto
     *            the DTO to send
     */
    public void produceReactiveDto(final TestDto dto) {
        dtoReactiveEmitter.send(dto);
    }

    public void produceBatchList(final List<String> messages) {
        batchListEmitter.send(
                ContextAwareMessage.of(messages)
                        .addMetadata(new RedisStreamMetadata().withPipelined(true)));
    }

    public void produceBatchStream(final Stream<String> messages) {
        batchStreamEmitter.send(
                ContextAwareMessage.of(messages)
                        .addMetadata(new RedisStreamMetadata().withPipelined(true)));
    }
}
