package com.guicedee.rabbit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares queue-level configuration options applied when the queue and consumer are created.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface QueueOptions {
    /**
     * @return Maximum priority value for the queue (0 disables priority).
     */
    int priority() default 0;

    /**
     * @return Prefetch count for the consumer (0 leaves default).
     */
    int fetchCount() default 0;

    /**
     * @return Whether the queue should be durable.
     */
    boolean durable() default false;

    /**
     * @return Whether to auto-delete the queue when the last consumer disconnects.
     */
    boolean delete() default false;

    /**
     * @return Whether consumers should auto-acknowledge messages.
     */
    boolean autoAck() default false;

    /**
     * @return Whether the queue is exclusive to the declaring connection.
     */
    boolean consumerExclusive() default false;

    /**
     * @return Whether to enforce a single active consumer.
     */
    boolean singleConsumer() default false;

    /**
     * @return Message time-to-live in milliseconds (0 disables).
     */
    int ttl() default 0;

    /**
     * @return Whether the broker should avoid delivering messages back to the same connection.
     */
    boolean noLocal() default false;

    /**
     * @return Whether to keep only the most recent messages when backlog exceeds limits.
     */
    boolean keepMostRecent() default true;

    /**
     * @return Maximum internal queue size for the client-side consumer buffer.
     */
    int maxInternalQueueSize() default Integer.MAX_VALUE;

    /**
     * @return Whether to wrap message handling in a transaction.
     */
    boolean transacted() default true;

    /**
     * Auto start/bind the consumer on startup, otherwise make an injection call to start
     *
     * @return Whether consumers should be automatically started/bound on startup.
     */
    boolean autobind() default true;

    /**
     * @return Number of consumer instances to create.
     */
    int consumerCount() default 1;

    /**
     * @return Whether the configuration should use a dedicated channel.
     */
    boolean dedicatedChannel() default false;

    /**
     * @return Whether to use a dedicated connection for this queue.
     */
    boolean dedicatedConnection() default false;
}
