package com.guicedee.rabbit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE,ElementType.PACKAGE})
public @interface RabbitConnectionOptions
{
    /**
     * @return Name of the connection
     */
    String value() default "default";

    String uri() default "";

    String[] addresses() default {};

    /**
     * @return The user or a wrapped string for environment or system properties
     */
    String user() default "guest";

    /**
     * @return System or environment property name
     */
    String password() default "guest";

    String host() default "";

    String virtualHost() default "";

    int port() default 0;

    int connectionTimeout() default 0;

    int requestedHeartbeat() default 0;

    int handshakeTimeout() default 0;

    int requestedChannelMax() default 0;

    long networkRecoveryInterval() default 0;

    boolean automaticRecoveryEnabled() default true;

    boolean automaticRecoveryOnInitialConnection() default false;

    boolean includeProperties() default true;

    boolean useNio() default false;

    int reconnectAttempts() default 0;

    long reconnectInterval() default 0;

    String hostnameVerificationAlgorithm() default "";

    String[] applicationLayerProtocols() default {};

    boolean registerWriteHandler() default true;

    boolean confirmPublishes() default false;
}
