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

class RedisstreamExtensionProcessor {

    private static final String FEATURE = "redisstream-extension";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

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

    static String channelPropertyFormat = "mp.messaging.%s.%s.%s";

    static String getChannelPropertyKey(String channelName, String propertyName, boolean incoming) {
        return String.format(
                channelPropertyFormat,
                incoming ? "incoming" : "outgoing",
                channelName.contains(".") ? "\"" + channelName + "\"" : channelName,
                propertyName);
    }
}
