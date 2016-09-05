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
package org.graylog.plugins.redis.outputs;

import com.google.common.collect.ImmutableMap;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.pubsub.RedisPubSubAdapter;
import com.lambdaworks.redis.pubsub.StatefulRedisPubSubConnection;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.plugin.system.NodeId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.when;

public class GELFRedisOutputIT {
    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    private static final String REDIS_HOST = System.getProperty("redis.host", "127.0.0.1");
    private static final int REDIS_PORT = Integer.getInteger("redis.port", RedisURI.DEFAULT_REDIS_PORT);

    private static String CLUSTER_ID = "GRAYLOG-CLUSTER-ID";
    private static String NODE_ID = "GRAYLOG-NODE-ID";

    @Mock
    private NodeId nodeId;
    @Mock
    private ServerStatus serverStatus;

    private GELFRedisOutput output;

    @Before
    public void setUp() throws MessageOutputConfigurationException {
        final Configuration configuration = new Configuration(
                ImmutableMap.of(
                        "redis_uri", "redis://" + REDIS_HOST + ":" + REDIS_PORT,
                        "redis_channel", "graylog"
                )

        );
        when(serverStatus.getClusterId()).thenReturn(CLUSTER_ID);
        when(nodeId.toString()).thenReturn(NODE_ID);
        when(serverStatus.getNodeId()).thenReturn(nodeId);

        output = new GELFRedisOutput(configuration, serverStatus);

        assumeTrue(output.isRunning());
    }

    @Test
    public void publishMessage() throws Exception {
        final List<String> receivedMessages = new CopyOnWriteArrayList<>();
        final RedisURI redisURI = RedisURI.create(REDIS_HOST, REDIS_PORT);
        final RedisClient client = RedisClient.create(redisURI);

        final StatefulRedisPubSubConnection<String, String> pubSub = client.connectPubSub();
        pubSub.addListener(new RedisPubSubAdapter<String, String>() {
            @Override
            public void message(String channel, String message) {
                assertThat(channel).isEqualTo("graylog");
                receivedMessages.add(message);
            }

            @Override
            public void subscribed(String channel, long count) {
                assertThat(channel).isEqualTo("graylog");
            }
        });
        pubSub.sync().subscribe("graylog");

        final DateTime timestamp = new DateTime(2016, 9, 5, 11, 0, DateTimeZone.UTC);
        final Map<String, Object> messageFields = ImmutableMap.<String, Object>builder()
                .put(Message.FIELD_ID, "061b5ed0-734a-11e6-8e18-6c4008b8fc28")
                .put(Message.FIELD_MESSAGE, "TEST")
                .put(Message.FIELD_SOURCE, "integration.test")
                .put(Message.FIELD_TIMESTAMP, timestamp)
                .put(Message.FIELD_LEVEL, 5)
                .put("facility", "IntegrationTest")
                .put("string", "foobar")
                .put("bool", true)
                .put("int", 42)
                .put("long", 4242424242L)
                .put("float", 23.42f)
                .put("double", 23.42d)
                .put("big_decimal", new BigDecimal("42424242424242424242"))
                .build();
        final Message message = new Message(messageFields);

        output.write(message);

        while (receivedMessages.isEmpty()) {
            Thread.sleep(100L);
        }

        final String expectedMessage = "{" +
                "\"version\":\"1.1\"," +
                "\"host\":\"integration.test\"" +
                ",\"short_message\":\"TEST\"," +
                "\"timestamp\":1.4730732E9," +
                "\"big_decimal\":42424242424242424242," +
                "\"string\":\"foobar\"," +
                "\"bool\":true," +
                "\"level\":5," +
                "\"double\":23.42," +
                "\"float\":23.42," +
                "\"int\":42," +
                "\"long\":4242424242," +
                "\"_id\":\"061b5ed0-734a-11e6-8e18-6c4008b8fc28\"," +
                "\"facility\":\"IntegrationTest\"," +
                "\"_forwarder_cluster_id\":\"GRAYLOG-CLUSTER-ID\"," +
                "\"_forwarder_node_id\":\"GRAYLOG-NODE-ID\"}";
        assertThat(receivedMessages)
                .isNotEmpty()
                .containsOnly(expectedMessage);
    }
}
