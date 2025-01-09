package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.util.HashMap;
import java.util.Map;

public class RedisStreamMetadata {

    private final Map<String, String> additionalFields = new HashMap<>();

    public Map<String, String> getAdditionalFields() {
        return additionalFields;
    }
}
