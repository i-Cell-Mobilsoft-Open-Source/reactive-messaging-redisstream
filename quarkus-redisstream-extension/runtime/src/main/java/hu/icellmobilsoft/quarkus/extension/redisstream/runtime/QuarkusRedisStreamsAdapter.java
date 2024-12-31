package hu.icellmobilsoft.quarkus.extension.redisstream.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreams;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.StreamEntry;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;

public class QuarkusRedisStreamsAdapter implements RedisStreams {

    private final RedisAPI redisAPI;

    public QuarkusRedisStreamsAdapter(RedisAPI redisAPI) {
        this.redisAPI = redisAPI;
    }

    @Override
    public boolean existGroup(String stream, String group) {
        try {
            Response groups = redisAPI.xinfoAndAwait(List.of("GROUPS", stream));
            return StreamSupport.stream(groups.spliterator(), false).map(r -> r.get("name")).map(Response::toString).anyMatch(group::equals);
        } catch (Exception e) {
            // ha nincs kulcs akkor a kovetkezo hiba jon:
            // redis.clients.jedis.exceptions.JedisDataException: ERR no such key
            Log.infov("Redis exception duringchecking group [{0}] on stream [{1}]: [{2}]", group, stream, e.getLocalizedMessage());
            return false;
        }
    }

    @Override
    public String xGroupCreate(String stream, String group) {
        Response create = redisAPI.xgroupAndAwait(List.of("CREATE", stream, group, "0", "MKSTREAM"));
        return create.toString();
    }

    @Override
    public Uni<Integer> xAck(String stream, String group, String id) {
        return redisAPI.xack(List.of(stream, group, id)).map(Response::toInteger);
    }

    @Override
    public Uni<String> xAdd(String stream, String id, Map<String, String> fields) {
        return xAdd(stream, id, null, null, null, fields);
    }

    @Override
    public Uni<String> xAdd(String stream, String id, Integer maxLen, Boolean exact, String minId, Map<String, String> fields) {
        List<String> xAddArgs = new ArrayList<>();
        xAddArgs.add(stream);

        if (maxLen != null) {
            xAddArgs.add("MAXLEN");
            if (!Boolean.TRUE.equals(exact)) {
                xAddArgs.add("~");
            }
            xAddArgs.add(maxLen.toString());
        } else if (minId != null) {
            xAddArgs.add("MINID");
            xAddArgs.add(minId);
        }

        xAddArgs.add(id);
        fields.forEach((key, value) -> {
            xAddArgs.add(key);
            xAddArgs.add(value);
        });
        return redisAPI.xadd(xAddArgs).map(Response::toString);
    }

    @Override
    public Uni<List<StreamEntry>> xReadGroup(String stream, String group, String consumer, Integer count, Integer blockMs) {
        List<String> xReadArgs = new ArrayList<>();
        xReadArgs.add("GROUP");
        xReadArgs.add(group);
        xReadArgs.add(consumer);
        if (count != null) {
            xReadArgs.add("COUNT");
            xReadArgs.add(String.valueOf(count));
        }
        if (blockMs != null) {
            xReadArgs.add("BLOCK");
            xReadArgs.add(String.valueOf(blockMs));
        }
        xReadArgs.add("STREAMS");
        xReadArgs.add(stream);
        xReadArgs.add(">");

        return redisAPI.xreadgroup(xReadArgs).map(this::parseXReadResponse);

    }

    private Multi<StreamEntry> processXReadResponse(Response response) {
        return Multi.createFrom().iterable(parseXReadResponse(response));
    }

    private List<StreamEntry> parseXReadResponse(Response response) {
        if (response == null) {
            return Collections.emptyList();
        }
        List<StreamEntry> streamEntries = new ArrayList<>();
        for (String streamKey : response.getKeys()) {
            for (Response entry : response.get(streamKey)) {
                streamEntries.add(createStreamEntry(streamKey, entry));
            }
        }
        return streamEntries;
    }

    private StreamEntry createStreamEntry(String streamKey, Response entry) {
        if (entry == null) {
            return null;
        }
        if (entry.size() != 2) {
            throw new IllegalArgumentException("Entry size must be 2 but got " + entry.size());
        }

        String entryId = entry.get(0).toString();
        Response fieldResponse = entry.get(1);
        Map<String, String> fields = new HashMap<>();
        if (fieldResponse != null && fieldResponse.size() > 0 && fieldResponse.size() % 2 == 0) {
            for (int i = 1; i < fieldResponse.size(); i = i + 2) {
                String key = fieldResponse.get(i - 1).toString();
                String value = fieldResponse.get(i).toString();
                fields.put(key, value);
            }
        }
        return new StreamEntry(streamKey, entryId, fields);
    }


}
