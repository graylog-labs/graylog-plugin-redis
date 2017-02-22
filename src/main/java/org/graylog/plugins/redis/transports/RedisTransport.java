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
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import org.graylog.plugins.redis.internal.RedisClientBuilder;
import org.graylog.plugins.redis.internal.RedisClientConfiguration;
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

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.StreamSupport;

public class RedisTransport extends ThrottleableTransport {
    private static final Logger LOG = LoggerFactory.getLogger(RedisTransport.class);

    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final String CK_CHANNELS = "channels";
    private static final String CK_PATTERNS = "patterns";

    private final Configuration configuration;
    private final LocalMetricRegistry localRegistry;

    private RedisClient client;
    private StatefulRedisPubSubConnection<byte[], byte[]> connection;

    @Inject
    public RedisTransport(@Assisted final Configuration configuration,
                          final EventBus serverEventBus,
                          final LocalMetricRegistry localRegistry) {
        super(serverEventBus, configuration);
        this.configuration = configuration;
        this.localRegistry = localRegistry;
    }

    @Override
    protected void doLaunch(final MessageInput input) throws MisfireException {
        client = new RedisClientBuilder(configuration).buildClient();
        connection = client.connectPubSub(new ByteArrayCodec());
        connection.addListener(new RedisPubSubListener<byte[], byte[]>() {
            private void processMessage(byte[] message) {
                final RawMessage rawMessage = new RawMessage(message);
                input.processRawMessage(rawMessage);
            }

            @Override
            public void message(byte[] channel, byte[] message) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received message on channel \"{}\"", new String(channel, UTF_8));
                }

                processMessage(message);
            }

            @Override
            public void message(byte[] pattern, byte[] channel, byte[] message) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received message for pattern \"{}\" on channel \"{}\"",
                            new String(pattern, UTF_8), new String(channel, UTF_8));
                }

                processMessage(message);
            }

            @Override
            public void subscribed(byte[] channel, long count) {
                LOG.debug("Subscribed to channel \"{}\"", new String(channel, UTF_8));
            }

            @Override
            public void psubscribed(byte[] pattern, long count) {
                LOG.debug("Subscribed to pattern \"{}\"", new String(pattern, UTF_8));
            }

            @Override
            public void unsubscribed(byte[] channel, long count) {
                LOG.debug("Unsubscribed from channel \"{}\"", new String(channel, UTF_8));
            }

            @Override
            public void punsubscribed(byte[] pattern, long count) {
                LOG.debug("Unsubscribed from pattern \"{}\"", new String(pattern, UTF_8));
            }
        });

        final String channelsString = configuration.getString(CK_CHANNELS, "");
        byte[][] channels = splitToByteArray(channelsString);
        if (channels.length > 0) {
            connection.sync().subscribe(channels);
        }

        final String patternsString = configuration.getString(CK_PATTERNS, "");
        byte[][] patterns = splitToByteArray(patternsString);
        if (patterns.length > 0) {
            connection.sync().psubscribe(patterns);
        }
    }

    private byte[][] splitToByteArray(String str) {
        return StreamSupport.stream(Arrays.spliterator(str.split(",")), false)
                .map(String::trim)
                .filter(s -> s != null && !s.isEmpty())
                .map(s -> s.getBytes(UTF_8))
                .toArray(byte[][]::new);
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
            final RedisClientConfiguration r = new RedisClientConfiguration(super.getRequestedConfiguration());
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
