package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.util.HashMap;
import java.util.Map;

/**
 * General microprofile reactive streams metadata for Redis stream connector.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
public class RedisStreamMetadata {

    private final Map<String, String> additionalFields = new HashMap<>();

    /**
     * Gets the additional fields beside message payload.
     *
     * @return a map containing the additional fields
     */
    public Map<String, String> getAdditionalFields() {
        return additionalFields;
    }
}
