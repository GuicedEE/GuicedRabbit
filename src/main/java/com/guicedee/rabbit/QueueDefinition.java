package com.guicedee.rabbit;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE,
        ElementType.FIELD})
public @interface QueueDefinition
{
    /**
     * @return string The name of the queue that this is configuring for
     */
    String value();

    /**
     * @return A set of queue options for configuration
     */
    QueueOptions options() default @QueueOptions;

    /**
     * @return The name of the exchange to bind to, defaults to the original name
     */
    String exchange() default "default";

}
