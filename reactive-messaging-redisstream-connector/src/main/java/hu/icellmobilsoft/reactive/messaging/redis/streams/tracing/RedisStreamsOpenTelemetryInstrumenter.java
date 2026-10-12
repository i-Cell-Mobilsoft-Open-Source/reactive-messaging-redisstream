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
package hu.icellmobilsoft.reactive.messaging.redis.streams.tracing;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

/**
 * Creates and holds the OpenTelemetry {@link Instrumenter} used to instrument produced and consumed redis stream messages with tracing spans and
 * messaging semantic convention attributes.
 *
 * @since 1.5.0
 * @author kornel.danko
 */
public class RedisStreamsOpenTelemetryInstrumenter {

    /**
     * Instrumentation library name reported as {@code otel.library.name} on the produced spans.
     */
    public static final String INSTRUMENTATION_NAME = "io.smallrye.reactive.messaging.redis.streams";

    private final Instrumenter<RedisStreamTrace, Void> instrumenter;

    private RedisStreamsOpenTelemetryInstrumenter(Instrumenter<RedisStreamTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    /**
     * Creates the {@link RedisStreamsOpenTelemetryInstrumenter} instance used for incoming (consumed) redis stream messages.
     *
     * @param openTelemetryInstance
     *            the CDI instance of {@link OpenTelemetry}, falls back to {@link io.opentelemetry.api.GlobalOpenTelemetry} when unresolvable or
     *            {@code null}
     * @return the created instrumenter
     */
    public static RedisStreamsOpenTelemetryInstrumenter createForIncoming(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(openTelemetryInstance, MessageOperation.PROCESS, true);
    }

    /**
     * Creates the {@link RedisStreamsOpenTelemetryInstrumenter} instance used for outgoing (produced) redis stream messages.
     *
     * @param openTelemetryInstance
     *            the CDI instance of {@link OpenTelemetry}, falls back to {@link io.opentelemetry.api.GlobalOpenTelemetry} when unresolvable or
     *            {@code null}
     * @return the created instrumenter
     */
    public static RedisStreamsOpenTelemetryInstrumenter createForOutgoing(Instance<OpenTelemetry> openTelemetryInstance) {
        return create(openTelemetryInstance, MessageOperation.PUBLISH, false);
    }

    private static RedisStreamsOpenTelemetryInstrumenter create(Instance<OpenTelemetry> openTelemetryInstance, MessageOperation operation,
            boolean consumer) {
        OpenTelemetry openTelemetry = openTelemetryInstance == null
                ? GlobalOpenTelemetry.get()
                : TracingUtils.getOpenTelemetry(openTelemetryInstance);
        RedisStreamMessagingAttributesGetter attributesGetter = RedisStreamMessagingAttributesGetter.INSTANCE;
        SpanNameExtractor<RedisStreamTrace> spanNameExtractor = MessagingSpanNameExtractor.create(attributesGetter, operation);
        InstrumenterBuilder<RedisStreamTrace, Void> builder = Instrumenter.<RedisStreamTrace, Void> builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                spanNameExtractor)
                .addAttributesExtractor(MessagingAttributesExtractor.create(attributesGetter, operation));
        Instrumenter<RedisStreamTrace, Void> instrumenter;
        if (consumer) {
            builder.addSpanLinksExtractor((spanLinksBuilder, parentContext, request) -> {
                if (request != null && request.getFields() != null) {
                    Context extractedContext = openTelemetry.getPropagators()
                            .getTextMapPropagator()
                            .extract(parentContext, request, RedisStreamTraceTextMapGetter.INSTANCE);
                    SpanContext spanContext = Span.fromContext(extractedContext).getSpanContext();
                    if (spanContext.isValid()) {
                        spanLinksBuilder.addLink(spanContext);
                    }
                }
            });
            instrumenter = builder.buildInstrumenter(SpanKindExtractor.alwaysConsumer());
        } else {
            instrumenter = builder.buildProducerInstrumenter(RedisStreamTraceTextMapSetter.INSTANCE);
        }
        return new RedisStreamsOpenTelemetryInstrumenter(instrumenter);
    }

    /**
     * Traces an incoming (consumed) message: starts and immediately ends a {@code CONSUMER} span, linking to the propagated OpenTelemetry context
     * from {@link RedisStreamTrace#getFields()} via a span link, and attaches the resulting tracing context to the returned message, so that
     * further processing of the message happens within that consumer trace.
     *
     * @param message
     *            the message to trace
     * @param redisStreamTrace
     *            the redis stream trace data of the message
     * @return the (possibly new) message, enriched with the tracing metadata
     */
    public Message<?> traceIncoming(Message<?> message, RedisStreamTrace redisStreamTrace) {
        return TracingUtils.traceIncoming(instrumenter, message, redisStreamTrace);
    }

    /**
     * Traces an outgoing (produced) message: starts and immediately ends a {@code PRODUCER} span, injecting the current OpenTelemetry context into
     * {@link RedisStreamTrace#getFields()} so it is sent along the redis stream entry (e.g. as a {@code traceparent} field).
     *
     * @param message
     *            the message being sent
     * @param redisStreamTrace
     *            the redis stream trace data of the message
     */
    public void traceOutgoing(Message<?> message, RedisStreamTrace redisStreamTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, redisStreamTrace);
    }
}
