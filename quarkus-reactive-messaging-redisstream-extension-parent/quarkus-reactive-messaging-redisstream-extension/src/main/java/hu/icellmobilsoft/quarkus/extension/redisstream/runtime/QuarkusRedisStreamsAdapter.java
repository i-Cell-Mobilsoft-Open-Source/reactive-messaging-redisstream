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
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;

/**
 * Adapter class for Redis Streams using Quarkus. This class provides methods to interact with Redis Streams using the RedisAPI.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 * 
 * @see RedisAPI
 * @see RedisStreams
 */
public class QuarkusRedisStreamsAdapter implements RedisStreams {

    private final RedisAPI redisAPI;

    /**
     * Constructs a new QuarkusRedisStreamsAdapter with the given RedisAPI.
     *
     * @param redisAPI
     *            the RedisAPI instance to use for Redis operations
     */
    public QuarkusRedisStreamsAdapter(RedisAPI redisAPI) {
        this.redisAPI = redisAPI;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uni<Boolean> existGroup(String stream, String group) {
        return redisAPI.xinfo(List.of("GROUPS", stream))
                .onSubscription()
                .invoke(() -> Log.tracev("Checking consumer group [{0}] on stream [{1}]", group, stream))
                .onItemOrFailure()
                .transform((response, throwable) -> {
                    if (throwable != null) {
                        Log.debugv(
                                "Redis exception during checking group [{0}] on stream [{1}]: [{2}]",
                                group,
                                stream,
                                throwable.getLocalizedMessage());
                        return false;
                    }
                    if (response == null) {
                        return false;
                    }
                    return StreamSupport.stream(response.spliterator(), false)
                            .map(r -> r.get("name"))
                            .map(Response::toString)
                            .anyMatch(group::equals);
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uni<String> xGroupCreate(String stream, String group) {
        return redisAPI.xgroup(List.of("CREATE", stream, group, "0", "MKSTREAM")).map(Response::toString);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uni<Long> xAck(String stream, String group, String id) {
        return redisAPI.xack(List.of(stream, group, id)).map(Response::toLong);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uni<String> xAdd(String stream, String id, Map<String, String> fields) {
        return xAdd(stream, id, null, null, null, fields);
    }

    /**
     * {@inheritDoc}
     */
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
        Log.tracev("Calling redis command XADD with args:[{0}]", xAddArgs);
        return redisAPI.xadd(xAddArgs).map(Response::toString);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Uni<List<StreamEntry>> xReadGroup(String stream, String group, String consumer, Integer count, Integer blockMs, Boolean noack) {
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
        if (Boolean.TRUE.equals(noack)) {
            xReadArgs.add("NOACK");
        }
        xReadArgs.add("STREAMS");
        xReadArgs.add(stream);
        xReadArgs.add(">");

        Log.tracev("Calling redis command XREADGROUP with args:[{0}]", xReadArgs);
        return redisAPI.xreadgroup(xReadArgs).map(this::parseXReadResponse);
    }

    /**
     * Parses the raw response from the {@code XREADGROUP} command.
     *
     * @param response
     *            the response from the Redis server
     * @return a list of StreamEntry objects
     */
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

    @Override
    public void close() {
        // do nothing, quarkus redis extension will close the redisAPIs upon shutdown
    }
}
