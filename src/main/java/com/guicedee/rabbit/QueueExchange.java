package com.guicedee.rabbit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE,ElementType.PACKAGE})
public @interface QueueExchange
{
    /**
     * @return The name of the exchange
     */
    String value() default "default";

    boolean createDeadLetter() default false;

    boolean durable() default false;
    boolean autoDelete() default false;

    ExchangeType exchangeType() default ExchangeType.Direct;

    enum ExchangeType
    {
        Direct,
        Fanout,
        Topic,
        Headers
        ;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }
}
