package org.graylog.plugins.redis.internal;

import com.google.common.collect.ImmutableList;
import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.resource.DefaultClientResources;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;

import static java.util.Objects.requireNonNull;

public class RedisClientConfiguration extends ConfigurationRequest {
    static final String CK_REDIS_URI = "redis_uri";
    static final String CK_TIMEOUT = "timeout";
    static final String CK_THREADS_IO = "threads_io";
    static final String CK_THREADS_COMPUTATION = "threads_computation";
    static final String CK_REQUEST_QUEUE_SIZE = "request_queue_size";
    static final String CK_AUTO_RECONNECT = "auto_reconnect";
    static final String CK_PING_BEFORE_ACTIVATE_CONNECTION = "ping_before_activate_connection";

    public RedisClientConfiguration(ConfigurationRequest configurationRequest) {
        this(requireNonNull(configurationRequest, "configurationRequest").getFields().values());
    }

    RedisClientConfiguration(Iterable<ConfigurationField> fields) {
        addFields(
                ImmutableList.<ConfigurationField>builder()
                        .addAll(fields)
                        .add(new TextField(CK_REDIS_URI,
                                "Redis URI",
                                "redis://localhost",
                                "URI of the Redis server: redis://[password@]host[:port][/databaseNumber]",
                                ConfigurationField.Optional.NOT_OPTIONAL))
                        .add(new NumberField(CK_TIMEOUT,
                                "Timeout (s)",
                                60,
                                "Timeout for the Redis client in seconds",
                                ConfigurationField.Optional.NOT_OPTIONAL))
                        .add(new NumberField(CK_THREADS_IO,
                                "I/O threads",
                                Math.max(DefaultClientResources.MIN_IO_THREADS, DefaultClientResources.DEFAULT_IO_THREADS),
                                "Number of I/O threads",
                                ConfigurationField.Optional.OPTIONAL,
                                NumberField.Attribute.ONLY_POSITIVE))
                        .add(new NumberField(CK_THREADS_COMPUTATION,
                                "Computation threads",
                                Math.max(DefaultClientResources.MIN_COMPUTATION_THREADS, DefaultClientResources.DEFAULT_COMPUTATION_THREADS),
                                "Number of computation threads",
                                ConfigurationField.Optional.OPTIONAL,
                                NumberField.Attribute.ONLY_POSITIVE))
                        .add(new NumberField(CK_REQUEST_QUEUE_SIZE,
                                "Request queue size",
                                ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE,
                                "Request queue size per connection",
                                ConfigurationField.Optional.OPTIONAL,
                                NumberField.Attribute.ONLY_POSITIVE))
                        .add(new BooleanField(CK_AUTO_RECONNECT,
                                "Automatic reconnect",
                                ClientOptions.DEFAULT_AUTO_RECONNECT,
                                "Try to reconnect and re-issue any queued commands if connection was involuntarily closed"))
                        .add(new BooleanField(CK_PING_BEFORE_ACTIVATE_CONNECTION,
                                "Ping before use",
                                ClientOptions.DEFAULT_PING_BEFORE_ACTIVATE_CONNECTION,
                                "Enable initial PING barrier before any connection is usable"))
                        .build()
        );
    }
}
