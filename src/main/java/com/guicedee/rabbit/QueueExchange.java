package com.guicedee.rabbit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares an AMQP exchange for a package or a type and supplies defaults for
 * queue bindings created under that exchange.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.PACKAGE})
public @interface QueueExchange
{
    /**
     * @return The exchange name to declare and bind queues to.
     */
    String value() default "default";

    /**
     * @return Whether to declare a dead-letter exchange alongside the main exchange.
     */
    boolean createDeadLetter() default false;

    /**
     * @return Whether the exchange is durable.
     */
    boolean durable() default false;

    /**
     * @return Whether the exchange is auto-deleted when unused.
     */
    boolean autoDelete() default false;

    /**
     * @return The AMQP exchange type to declare.
     */
    ExchangeType exchangeType() default ExchangeType.Direct;

    /**
     * Supported AMQP exchange types.
     */
    enum ExchangeType
    {
        Direct,
        Fanout,
        Topic,
        Headers
        ;

        /**
         * @return The exchange type name in lower-case as required by the AMQP API.
         */
        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }
}
