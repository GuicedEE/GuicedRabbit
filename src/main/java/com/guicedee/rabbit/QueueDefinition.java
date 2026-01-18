package com.guicedee.rabbit;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a queue name and options for a consumer class or a publisher field.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD})
public @interface QueueDefinition
{
    /**
     * @return The queue name this definition applies to.
     */
    String value();

    /**
     * @return Queue option overrides for this definition.
     */
    QueueOptions options() default @QueueOptions;

    /**
     * @return The exchange name to bind to, or {@code default} to use the package exchange.
     */
    String exchange() default "default";

}
