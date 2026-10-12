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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import jakarta.enterprise.inject.Instance;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;

/**
 * Test class for {@link RedisStreamsOpenTelemetryInstrumenter}.
 *
 * @since 1.5.0
 * @author kornel.danko
 */
public class RedisStreamsOpenTelemetryInstrumenterTest {

    @Test
    void traceOutgoingPropagatesContextAndIncomingLinksToIt() {
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        SpanContext parentContext = SpanContext.create(
                "0123456789abcdef0123456789abcdef",
                "0123456789abcdef",
                TraceFlags.getSampled(),
                TraceState.getDefault());

        try {
            Instance<OpenTelemetry> openTelemetryInstance = openTelemetryInstance(openTelemetry);
            RedisStreamsOpenTelemetryInstrumenter incomingInstrumenter =
                    RedisStreamsOpenTelemetryInstrumenter.createForIncoming(openTelemetryInstance);
            RedisStreamsOpenTelemetryInstrumenter outgoingInstrumenter =
                    RedisStreamsOpenTelemetryInstrumenter.createForOutgoing(openTelemetryInstance);
            Map<String, String> fields = new HashMap<>();

            try (var scope = Span.wrap(parentContext).makeCurrent()) {
                RedisStreamTrace outgoingTrace = RedisStreamTrace.builder()
                        .withStreamKey("test-stream")
                        .withFields(fields)
                        .build();
                outgoingInstrumenter.traceOutgoing(Message.of("test-payload"), outgoingTrace);
            }

            assertThat(fields.get("traceparent"))
                    .matches("00-0123456789abcdef0123456789abcdef-[0-9a-f]{16}-01");

            RedisStreamTrace incomingTrace = RedisStreamTrace.builder()
                    .withStreamKey("test-stream")
                    .withConsumer("test-consumer")
                    .withId("12345-0")
                    .withFields(fields)
                    .build();
            Message<?> tracedMessage = incomingInstrumenter.traceIncoming(Message.of("test-payload"), incomingTrace);
            List<SpanData> spans = spanExporter.getFinishedSpanItems();

            assertThat(tracedMessage.getPayload()).isEqualTo("test-payload");
            assertThat(spans).hasSize(2);
            assertThat(spans).extracting(SpanData::getKind).containsExactly(SpanKind.PRODUCER, SpanKind.CONSUMER);
            assertThat(spans.get(0).getParentSpanContext()).isEqualTo(parentContext);
            assertThat(fields.get("traceparent"))
                    .endsWith("-" + spans.get(0).getSpanContext().getSpanId() + "-01");
            assertThat(spans.get(1).getLinks())
                    .singleElement()
                    .satisfies(link -> {
                        assertThat(link.getSpanContext().getTraceId()).isEqualTo(spans.get(0).getSpanContext().getTraceId());
                        assertThat(link.getSpanContext().getSpanId()).isEqualTo(spans.get(0).getSpanContext().getSpanId());
                    });
        } finally {
            tracerProvider.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static Instance<OpenTelemetry> openTelemetryInstance(OpenTelemetry openTelemetry) {
        return (Instance<OpenTelemetry>) Proxy.newProxyInstance(
                Instance.class.getClassLoader(),
                new Class<?>[] { Instance.class },
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isResolvable" -> true;
                    case "get" -> openTelemetry;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
