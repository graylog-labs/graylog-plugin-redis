package org.graylog.plugins.redis.internal;

import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.resource.ClientResources;
import com.lambdaworks.redis.resource.DefaultClientResources;
import org.graylog2.plugin.configuration.Configuration;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class RedisClientBuilder {
    private final Configuration configuration;

    public RedisClientBuilder(Configuration configuration) {
        this.configuration = requireNonNull(configuration);
    }

    public RedisClient buildClient() {
        final String redisURIString = configuration.getString(RedisClientConfiguration.CK_REDIS_URI, "redis://127.0.0.1/");
        final int timeout = configuration.getInt(RedisClientConfiguration.CK_TIMEOUT, 60);
        final int threadsIo = configuration.getInt(RedisClientConfiguration.CK_THREADS_IO, DefaultClientResources.DEFAULT_IO_THREADS);
        final int threadsComputation = configuration.getInt(RedisClientConfiguration.CK_THREADS_COMPUTATION, DefaultClientResources.DEFAULT_COMPUTATION_THREADS);
        final int requestQueueSize = configuration.getInt(RedisClientConfiguration.CK_REQUEST_QUEUE_SIZE, ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE);
        final boolean autoReconnect = configuration.getBoolean(RedisClientConfiguration.CK_AUTO_RECONNECT, ClientOptions.DEFAULT_AUTO_RECONNECT);
        final boolean pingBeforeActivateConnection = configuration.getBoolean(RedisClientConfiguration.CK_PING_BEFORE_ACTIVATE_CONNECTION, ClientOptions.DEFAULT_PING_BEFORE_ACTIVATE_CONNECTION);

        final ClientResources clientResources = DefaultClientResources.builder()
                .ioThreadPoolSize(threadsIo)
                .computationThreadPoolSize(threadsComputation)
                .build();

        // TODO SSL/TLS and TCP connection options
        final ClientOptions clientOptions = ClientOptions.builder()
                .requestQueueSize(requestQueueSize)
                .autoReconnect(autoReconnect)
                .pingBeforeActivateConnection(pingBeforeActivateConnection)
                .build();

        final RedisURI redisURI = RedisURI.create(redisURIString);
        final RedisClient redisClient = RedisClient.create(clientResources, redisURI);
        redisClient.setDefaultTimeout(timeout, TimeUnit.SECONDS);
        redisClient.setOptions(clientOptions);

        return redisClient;
    }
}
