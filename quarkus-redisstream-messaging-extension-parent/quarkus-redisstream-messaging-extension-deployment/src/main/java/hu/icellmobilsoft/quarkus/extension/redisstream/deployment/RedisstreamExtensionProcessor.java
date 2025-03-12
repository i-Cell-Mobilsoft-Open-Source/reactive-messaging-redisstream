/*-
 * #%L
 * reactive-redisstream-messaging
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
package hu.icellmobilsoft.quarkus.extension.redisstream.deployment;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassType;
import org.jboss.jandex.DotName;

import hu.icellmobilsoft.quarkus.extension.redisstream.runtime.RedisStreamsRecorder;
import hu.icellmobilsoft.reactive.messaging.redis.streams.RedisStreamsConnector;
import hu.icellmobilsoft.reactive.messaging.redis.streams.api.RedisStreamsProducer;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.redis.client.RedisClientName;
import io.quarkus.redis.deployment.client.RequestedRedisClientBuildItem;
import io.quarkus.redis.runtime.client.config.RedisConfig;
import io.quarkus.smallrye.reactivemessaging.deployment.items.ChannelDirection;
import io.quarkus.smallrye.reactivemessaging.deployment.items.ConnectorManagedChannelBuildItem;
import io.vertx.mutiny.redis.client.RedisAPI;

/**
 * Quarkus Extension processor for reactive-streams Redis Stream connector.
 * 
 * @since 1.0.0
 * @author mark.petrenyi
 */
class RedisstreamExtensionProcessor {

    private static final String FEATURE = "redisstream-extension";

    private static final String CHANNEL_PROPERTY_FORMAT = "mp.messaging.%s.%s.%s";

    /**
     * Registers the Redis Stream extension feature.
     *
     * @return a FeatureBuildItem representing the Redis Stream extension feature
     */
    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * Produces a list of requested Redis clients based on the channels managed by connectors. This method iterates over the channels managed by
     * connectors and extracts the Redis client keys from the configuration. It then creates a list of RequestedRedisClientBuildItem for each unique
     * Redis client key.
     *
     * @param channelsManagedByConnectors
     *            the list of channels managed by reactive streams connectors
     * @return a list of RequestedRedisClientBuildItem representing the Redis clients requested by used redis stream connectors
     */
    @BuildStep
    List<RequestedRedisClientBuildItem> produceRedisClients(List<ConnectorManagedChannelBuildItem> channelsManagedByConnectors) {
        Set<String> redisCLientKeys = new HashSet<>();
        for (ConnectorManagedChannelBuildItem channelManagedByConnector : channelsManagedByConnectors) {
            if (RedisStreamsConnector.ICELLMOBILSOFT_REDIS_STREAMS_CONNECTOR.equals(channelManagedByConnector.getConnector())) {
                String channelPropertyKey = getChannelPropertyKey(
                        channelManagedByConnector.getName(),
                        RedisStreamsConnector.REDIS_STREAM_CONNECTION_KEY_CONFIG,
                        channelManagedByConnector.getDirection() == ChannelDirection.INCOMING);
                Config config = ConfigProvider.getConfig();
                String redisClientKey = config.getOptionalValue(channelPropertyKey, String.class).orElse(RedisConfig.DEFAULT_CLIENT_NAME);
                redisCLientKeys.add(redisClientKey);
            }
        }
        return redisCLientKeys.stream().map(RequestedRedisClientBuildItem::new).toList();
    }

    /**
     * Creates a synthetic bean for RedisStreamsProducer. This method configures a synthetic bean for RedisStreamsProducer and adds injection points
     * for each requested Redis client. The synthetic bean is marked as unremovable to ensure neither the bean nor its injected redis-clients are
     * removed during the build process.
     *
     * @param redisClients
     *            the list of requested Redis clients
     * @param recorder
     *            the RedisStreamsRecorder instance
     * @return a SyntheticBeanBuildItem representing the synthetic bean for RedisStreamsProducer
     */
    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    SyntheticBeanBuildItem syntheticBean(List<RequestedRedisClientBuildItem> redisClients, RedisStreamsRecorder recorder) {
        SyntheticBeanBuildItem.ExtendedBeanConfigurator redisStreamsProducer = SyntheticBeanBuildItem.configure(RedisStreamsProducer.class)
                .scope(ApplicationScoped.class);
        for (RequestedRedisClientBuildItem redisClient : redisClients) {
            AnnotationInstance jandexQualifier;
            String name = redisClient.name;
            if (RedisConfig.DEFAULT_CLIENT_NAME.equals(name)) {
                jandexQualifier = AnnotationInstance.builder(Default.class).build();
            } else {
                jandexQualifier = AnnotationInstance.builder(RedisClientName.class).value(name).build();
            }
            // Add redisclients as injectionPoints to prevent removal
            redisStreamsProducer.addInjectionPoint(ClassType.create(DotName.createSimple(RedisAPI.class)), jandexQualifier);
        }

        return redisStreamsProducer.unremovable().createWith(recorder.createWith()).done();
    }

    /**
     * Constructs the channel property key for the given parameters. This method formats the channel property key based on the channel name, property
     * name, and direction (incoming or outgoing).
     *
     * @param channelName
     *            the name of the channel
     * @param propertyName
     *            the name of the property
     * @param incoming
     *            true if the channel is incoming, false if outgoing
     * @return the constructed channel property key
     */
    private String getChannelPropertyKey(String channelName, String propertyName, boolean incoming) {
        return String.format(
                CHANNEL_PROPERTY_FORMAT,
                incoming ? "incoming" : "outgoing",
                channelName.contains(".") ? "\"" + channelName + "\"" : channelName,
                propertyName);
    }
}
