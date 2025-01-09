package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.util.Objects;

public class IncomingRedisStreamMetadata extends RedisStreamMetadata {

    private final String stream;
    private final String id;

    public IncomingRedisStreamMetadata(String stream, String id) {
        Objects.requireNonNull(stream, "stream is required");
        Objects.requireNonNull(id, "id is required");
        this.stream = stream;
        this.id = id;
    }

    public String getStream() {
        return stream;
    }

    public String getId() {
        return id;
    }
}
