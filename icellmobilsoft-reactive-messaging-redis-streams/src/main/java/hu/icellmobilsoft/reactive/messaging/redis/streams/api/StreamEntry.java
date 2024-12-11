package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import java.util.Map;

public record StreamEntry(
        String stream,
        String id,
        Map<String, String> fields) {
}
