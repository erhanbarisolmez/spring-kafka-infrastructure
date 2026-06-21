package com.selftech.kafka.core.handler;

import com.selftech.kafka.core.event.Event;

/**
 * Event Handler Interface - FAZE 2
 *
 * Tüm event handler'ların implement etmesi gereken base interface.
 * Generic type parameter ile type-safe event handling sağlar.
 *
 * Kullanım örneği:
 * public class LockBoxEventHandler implements EventHandler<LockBoxEvent> {
 *     public void handle(LockBoxEvent event) {
 *         // Business logic
 *     }
 * }
 *
 * Features:
 * - Type-safe event handling (compile-time checked)
 * - Single responsibility: Bir handler = bir event tipi
 * - Async capable: Runnable olarak execute edilebilir
 * - Exception propagation: Handler'ın kendi exception'ını fırlatması
 *
 * NOTE: Type parameter T is not bound to Event interface to support Avro-generated
 * classes which cannot implement custom interfaces due to Avro code generator limitations.
 *
 * @param <T> Event type that this handler processes
 */
@FunctionalInterface
public interface EventHandler<T> {

    /**
     * Handle the event
     *
     * @param event The event to process
     * @throws Exception If event processing fails
     *                   Exception'ı fırlatma sonrası retry logic trigger olur
     */
    void handle(T event) throws Exception;

    /**
     * Get handler name for logging and monitoring
     * Default: sınıf adı
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Check if this handler can process the given event
     * Default: always true
     * Override for conditional handling
     */
    default boolean canHandle(T event) {
        return true;
    }

    /**
     * Get event type that this handler processes
     * Used for handler registry lookup
     */
    default Class<T> getEventType() {
        // Reflection-based generic type extraction
        @SuppressWarnings("unchecked")
        Class<T> eventType = (Class<T>) java.lang.reflect.ParameterizedType.class
            .cast(this.getClass().getGenericInterfaces()[0])
            .getActualTypeArguments()[0];
        return eventType;
    }

    /**
     * Get handler priority (for multiple handlers per event)
     * Higher priority executes first
     * Default: 0 (normal priority)
     *
     * Priority levels:
     * - -1: Low priority (validation, logging)
     * -  0: Normal priority (default)
     * -  1: High priority (critical business logic)
     *
     * @return Handler priority value
     */
    default int getPriority() {
        return 0;
    }

    /**
     * Check if handler execution should stop chain
     * If true, subsequent handlers for same event won't be called
     * Default: false (continue to next handler)
     *
     * @return true to stop handler chain execution
     */
    default boolean stopsChain() {
        return false;
    }
}
