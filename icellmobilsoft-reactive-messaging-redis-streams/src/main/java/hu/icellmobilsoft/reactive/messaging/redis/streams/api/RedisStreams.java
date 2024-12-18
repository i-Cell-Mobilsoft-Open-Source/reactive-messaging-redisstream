package hu.icellmobilsoft.reactive.messaging.redis.streams.api;

import java.util.List;
import java.util.Map;

import io.smallrye.mutiny.Uni;

public interface RedisStreams {

    boolean existGroup(String stream, String group);

    String xGroupCreate(String stream, String group);

    Uni<String> xAdd(String stream, String id, Map<String, String> fields);

    default Uni<String> xAdd(String stream, Map<String, String> fields) {
        return xAdd(stream, "*", fields);
    }

    Uni<List<StreamEntry>> xReadGroup(String stream, String group, String consumer, Integer count, Integer blockMs);

}
