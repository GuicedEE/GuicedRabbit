package com.guicedee.rabbit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares connection-level configuration for a RabbitMQ client.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.PACKAGE})
public @interface RabbitConnectionOptions
{
    /**
     * @return Name of the connection for diagnostics and bindings.
     */
    String value() default "default";

    /**
     * @return Connection URI, if provided takes precedence over host/port.
     */
    String uri() default "";

    /**
     * @return Optional list of addresses for clustered connections.
     */
    String[] addresses() default {};

    /**
     * @return Username or a wrapped string for environment or system properties.
     */
    String user() default "guest";

    /**
     * @return Password or a wrapped string for environment or system properties.
     */
    String password() default "guest";

    /**
     * @return Hostname to connect to.
     */
    String host() default "";

    /**
     * @return Virtual host name to connect to.
     */
    String virtualHost() default "";

    /**
     * @return Port to connect to.
     */
    int port() default 0;

    /**
     * @return Connection timeout in milliseconds.
     */
    int connectionTimeout() default 0;

    /**
     * @return Requested heartbeat in seconds.
     */
    int requestedHeartbeat() default 0;

    /**
     * @return Handshake timeout in milliseconds.
     */
    int handshakeTimeout() default 0;

    /**
     * @return Requested maximum channel count.
     */
    int requestedChannelMax() default 0;

    /**
     * @return Network recovery interval in milliseconds.
     */
    long networkRecoveryInterval() default 0;

    /**
     * @return Whether automatic recovery is enabled.
     */
    boolean automaticRecoveryEnabled() default true;

    /**
     * @return Whether to attempt automatic recovery on initial connection failures.
     */
    boolean automaticRecoveryOnInitialConnection() default false;

    /**
     * @return Whether to include client properties in connection metadata.
     */
    boolean includeProperties() default true;

    /**
     * @return Whether to use NIO based connections.
     */
    boolean useNio() default false;

    /**
     * @return Reconnect attempts when using Vert.x options.
     */
    int reconnectAttempts() default 0;

    /**
     * @return Reconnect interval in milliseconds when using Vert.x options.
     */
    long reconnectInterval() default 0;

    /**
     * @return Hostname verification algorithm to enforce (empty for default).
     */
    String hostnameVerificationAlgorithm() default "";

    /**
     * @return Application layer protocols to advertise for TLS/ALPN.
     */
    String[] applicationLayerProtocols() default {};

    /**
     * @return Whether to register a write handler with Vert.x.
     */
    boolean registerWriteHandler() default true;

    /**
     * @return Whether to enable publisher confirms for this connection.
     */
    boolean confirmPublishes() default false;
}
