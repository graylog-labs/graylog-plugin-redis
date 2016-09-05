/**
 * This file is part of Graylog Redis Plugin.
 *
 * Graylog Redis Plugin is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog Redis Plugin is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog Redis Plugin.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog.plugins.redis.transports;

import com.codahale.metrics.MetricSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.ThrottleableTransport;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RedisTransport extends ThrottleableTransport {
    private static final Logger LOG = LoggerFactory.getLogger(RedisTransport.class);

    private static final String CK_REDIS_URI = "redis_uri";
    private static final String CK_CHANNELS = "channels";
    private static final String CK_PATTERNS = "patterns";
    private static final Function<String, byte[]> UTF_8_STRING_TO_BYTE_ARRAY_FUNCTION =
            input -> input == null ? null : input.getBytes(StandardCharsets.UTF_8);

    private final Configuration configuration;
    private final LocalMetricRegistry localRegistry;

    private RedisClient client;
    private StatefulRedisPubSubConnection<byte[], byte[]> connection;

    @AssistedInject
    public RedisTransport(@Assisted final Configuration configuration,
                          final EventBus serverEventBus,
                          final LocalMetricRegistry localRegistry) {
        super(serverEventBus, configuration);
        this.configuration = configuration;
        this.localRegistry = localRegistry;
    }

    @Override
    protected void doLaunch(final MessageInput input) throws MisfireException {
        final RedisURI redisURI = RedisURI.create(configuration.getString(CK_REDIS_URI));

        client = RedisClient.create(redisURI);
        connection = client.connectPubSub(new ByteArrayCodec());

        connection.addListener(new RedisPubSubListener<byte[], byte[]>() {
            private void processMessage(byte[] message) {
                final RawMessage rawMessage = new RawMessage(message);
                input.processRawMessage(rawMessage);
            }

            @Override
            public void message(byte[] channel, byte[] message) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received message on channel \"{}\"", new String(channel, StandardCharsets.UTF_8));
                }

                processMessage(message);
            }

            @Override
            public void message(byte[] pattern, byte[] channel, byte[] message) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received message for pattern \"{}\" on channel \"{}\"",
                            new String(pattern, StandardCharsets.UTF_8), new String(channel, StandardCharsets.UTF_8));
                }

                processMessage(message);
            }

            @Override
            public void subscribed(byte[] channel, long count) {
                LOG.debug("Subscribed to channel \"{}\"", new String(channel, StandardCharsets.UTF_8));
            }

            @Override
            public void psubscribed(byte[] pattern, long count) {
                LOG.debug("Subscribed to pattern \"{}\"", new String(pattern, StandardCharsets.UTF_8));
            }

            @Override
            public void unsubscribed(byte[] channel, long count) {
                LOG.debug("Unsubscribed from channel \"{}\"", new String(channel, StandardCharsets.UTF_8));
            }

            @Override
            public void punsubscribed(byte[] pattern, long count) {
                LOG.debug("Unsubscribed from pattern \"{}\"", new String(pattern, StandardCharsets.UTF_8));
            }
        });

        final String channelsString = configuration.getString(CK_CHANNELS, "");
        byte[][] channels = StreamSupport.stream(Arrays.spliterator(channelsString.split(",")), false)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(UTF_8_STRING_TO_BYTE_ARRAY_FUNCTION)
                .collect(Collectors.toList())
                .toArray(new byte[0][0]);
        if (channels.length > 0) {
            connection.sync().subscribe(channels);
        }

        final String patternsString = configuration.getString(CK_PATTERNS, "");
        byte[][] patterns = StreamSupport.stream(Arrays.spliterator(patternsString.split(",")), false)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(UTF_8_STRING_TO_BYTE_ARRAY_FUNCTION)
                .collect(Collectors.toList())
                .toArray(new byte[0][0]);
        if (patterns.length > 0) {
            connection.sync().psubscribe(patterns);
        }
    }

    @Override
    protected void doStop() {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }

        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public void setMessageAggregator(CodecAggregator aggregator) {

    }

    @Override
    public MetricSet getMetricSet() {
        return localRegistry;
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<RedisTransport> {
        @Override
        RedisTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends ThrottleableTransport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest r = super.getRequestedConfiguration();

            r.addField(new TextField(
                    CK_REDIS_URI,
                    "Redis URI",
                    "redis://localhost",
                    "URI of the Redis server: redis://[password@]host[:port][/databaseNumber]",
                    ConfigurationField.Optional.NOT_OPTIONAL));
            r.addField(new TextField(
                    CK_CHANNELS,
                    "Channels",
                    "",
                    "Comma-separated list of channels to subscribe to",
                    ConfigurationField.Optional.OPTIONAL));
            r.addField(new TextField(
                    CK_PATTERNS,
                    "Channel patterns",
                    "",
                    "Comma-separated list of channel patterns to subscribe to",
                    ConfigurationField.Optional.OPTIONAL));

            return r;
        }
    }
}
