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
package org.graylog.plugins.redis;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BasicRedisIT {
    private static final String REDIS_HOST = System.getProperty("redis.host", "127.0.0.1");
    private static final int REDIS_PORT = Integer.getInteger("redis.port", RedisURI.DEFAULT_REDIS_PORT);

    @Test
    public void foo() {
        final RedisURI redisURI = RedisURI.create(REDIS_HOST, REDIS_PORT);
        final RedisClient client = RedisClient.create(redisURI);

        try {
            final String ping = client.connect().sync().ping();
            assertThat(ping).isEqualTo("PONG");
        } finally {
            client.shutdown();
        }
    }
}
