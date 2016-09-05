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

import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RedisTransportIT {
    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    private static final String REDIS_HOST = System.getProperty("redis.host", "127.0.0.1");
    private static final int REDIS_PORT = Integer.getInteger("redis.port", RedisURI.DEFAULT_REDIS_PORT);

    private EventBus eventBus;
    private LocalMetricRegistry localMetricRegistry;

    @Before
    public void setUp() throws MessageOutputConfigurationException {
        eventBus = new EventBus();
        localMetricRegistry = new LocalMetricRegistry();
    }

    @Test
    public void subscribeChannel() throws Exception {
        final Configuration configuration = new Configuration(
                ImmutableMap.of(
                        "redis_uri", "redis://" + REDIS_HOST + ":" + REDIS_PORT,
                        "channels", "graylog"
                )
        );

        final RedisTransport redisTransport = new RedisTransport(configuration, eventBus, localMetricRegistry);
        final RedisURI redisURI = RedisURI.create(REDIS_HOST, REDIS_PORT);
        final RedisClient client = RedisClient.create(redisURI);

        final StatefulRedisPubSubConnection<String, String> pubSub = client.connectPubSub();

        final MessageInput messageInput = mock(MessageInput.class);
        redisTransport.launch(messageInput);

        pubSub.sync().publish("graylog", "TEST");

        Thread.sleep(100L);

        verify(messageInput, times(1)).processRawMessage(any(RawMessage.class));

        redisTransport.stop();
    }

    @Test
    public void subscribePattern() throws Exception {
        final Configuration configuration = new Configuration(
                ImmutableMap.of(
                        "redis_uri", "redis://" + REDIS_HOST + ":" + REDIS_PORT,
                        "patterns", "g*log"
                )
        );

        final RedisTransport redisTransport = new RedisTransport(configuration, eventBus, localMetricRegistry);
        final RedisURI redisURI = RedisURI.create(REDIS_HOST, REDIS_PORT);
        final RedisClient client = RedisClient.create(redisURI);

        final StatefulRedisPubSubConnection<String, String> pubSub = client.connectPubSub();

        final MessageInput messageInput = mock(MessageInput.class);
        redisTransport.launch(messageInput);

        pubSub.sync().publish("graylog", "TEST");

        Thread.sleep(100L);

        verify(messageInput, times(1)).processRawMessage(any(RawMessage.class));

        redisTransport.stop();
    }
}
