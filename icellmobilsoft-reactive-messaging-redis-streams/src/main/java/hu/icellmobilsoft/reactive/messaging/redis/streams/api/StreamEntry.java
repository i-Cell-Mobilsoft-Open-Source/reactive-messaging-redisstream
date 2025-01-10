package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import java.util.Map;

/**
 * Represents a stream entry.
 * 
 * @param stream
 *            the stream key
 * @param id
 *            the entry id
 * @param fields
 *            the entry fields
 * @since 1.0.0
 * @author mark.petrenyi
 */
public record StreamEntry(
        String stream,
        String id,
        Map<String, String> fields) {
}
