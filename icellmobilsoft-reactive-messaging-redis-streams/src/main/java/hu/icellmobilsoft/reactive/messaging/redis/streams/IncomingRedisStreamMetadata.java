package hu.icellmobilsoft.reactive.messaging.redis.streams;

import java.util.Objects;

/**
 * Microprofile reactive streams metadata for an incoming Redis stream.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
public class IncomingRedisStreamMetadata extends RedisStreamMetadata {

    private final String stream;
    private final String id;

    /**
     * Constructs a new IncomingRedisStreamMetadata instance.
     *
     * @param stream
     *            the redis key of the stream, must not be null
     * @param id
     *            the ID of the message, must not be null
     */
    public IncomingRedisStreamMetadata(String stream, String id) {
        Objects.requireNonNull(stream, "stream is required");
        Objects.requireNonNull(id, "id is required");
        this.stream = stream;
        this.id = id;
    }

    /**
     * Gets the redis key of the stream.
     *
     * @return the redis key of the stream
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the ID of the message.
     *
     * @return the ID of the message
     */
    public String getId() {
        return id;
    }
}
