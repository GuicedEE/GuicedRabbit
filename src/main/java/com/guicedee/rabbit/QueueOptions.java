package com.guicedee.rabbit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface QueueOptions
{
    int priority() default 0;
    int fetchCount() default 0;
    boolean durable() default false;
    boolean delete() default false;
    boolean autoAck() default true;
    boolean consumerExclusive() default false;
    boolean singleConsumer() default false;
    int ttl() default 0;
    boolean noLocal() default false;
    boolean keepMostRecent() default true;
    int maxInternalQueueSize() default Integer.MAX_VALUE;
}
