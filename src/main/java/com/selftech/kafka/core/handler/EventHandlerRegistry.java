package com.selftech.kafka.core.handler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import com.selftech.kafka.core.event.Event;

/**
 * Event Handler Registry - FAZE 3 ENTERPRISE
 *
 * Central registry for managing event handlers.
 * Allows handlers to be registered, looked up, and invoked by event type.
 *
 * Responsibilities:
 * 1. Register handlers by event type (at startup via @PostConstruct)
 * 2. Lookup handlers for a given event (by class type)
 * 3. Manage handler ordering/priority
 * 4. Support multiple handlers per event type
 * 5. Validation and error handling
 * 6. Metrics and monitoring
 *
 * Architecture:
 * ```
 * CentralEventConsumer
 *      ↓
 * Receives event from Kafka
 *      ↓
 * Looks up handler(s) via EventHandlerRegistry.findHandlers(eventClass)
 *      ↓
 * Executes handlers in priority order
 *      ↓
 * Returns success or failure
 * ```
 *
 * Handler Registration:
 * - Automatic: Spring scans @Component classes implementing EventHandler<T>
 * - Manual: registry.register(new MyEventHandler(), MyEvent.class)
 * - Priority: Handlers with higher priority execute first
 *
 * Usage Example:
 * @Service
 * public class LockBoxEventHandler implements EventHandler<LockBoxEvent> {
 *     @Override
 *     public void handle(LockBoxEvent event) throws Exception {
 *         // Update database
 *         lockBoxRepository.updateFromEvent(event);
 *     }
 * }
 *
 * Then in EventHandlerRegistry:
 * registry.register(new LockBoxEventHandler(), LockBoxEvent.class);
 *
 * And in CentralEventConsumer:
 * List<EventHandler<?>> handlers = registry.findHandlers(LockBoxEvent.class);
 * for (EventHandler<?> handler : handlers) {
 *     handler.handle(event);
 * }
 *
 * Design Patterns:
 * - Strategy Pattern: Different handlers = different strategies
 * - Registry Pattern: Central registry for handlers
 * - Chain of Responsibility: Multiple handlers can process same event
 * - Observer Pattern: Handlers observe specific event types
 */
@Service
@Slf4j
public class EventHandlerRegistry {

    /**
     * Map of event class → list of handlers
     * ConcurrentHashMap for thread-safe operations
     * Handlers are ordered by priority (high to low)
     *
     * Key: Event class type (e.g., LockBoxEvent.class)
     * Value: List of handlers for that event type (sorted by priority)
     */
    private final Map<Class<?>, List<EventHandler<?>>> handlerMap = new ConcurrentHashMap<>();

    /**
     * Register an event handler for a specific event type
     *
     * @param handler Handler implementation
     * @param eventType Event class type (e.g., LockBoxEvent.class)
     * @return true if successfully registered, false if already exists
     *
     * Usage:
     * registry.register(new LockBoxEventHandler(), LockBoxEvent.class);
     *
     * NOTE: Type parameter T is not bound to Event to support Avro-generated classes
     */
    public <T> boolean register(EventHandler<T> handler, Class<T> eventType) {
        try {
            if (handler == null || eventType == null) {
                log.warn("Cannot register handler: handler or eventType is null");
                return false;
            }

            // Check if handler already registered for this type
            List<EventHandler<?>> handlers = handlerMap.computeIfAbsent(
                eventType,
                k -> Collections.synchronizedList(new ArrayList<>())
            );

            // Check for duplicate
            if (handlers.stream().anyMatch(h -> h.getClass().equals(handler.getClass()))) {
                log.warn("Handler {} already registered for event type {}",
                    handler.getName(), eventType.getSimpleName());
                return false;
            }

            // Add handler to list
            handlers.add(handler);

            // Sort by priority (descending)
            handlers.sort((h1, h2) -> Integer.compare(h2.getPriority(), h1.getPriority()));

            log.info("Handler registered successfully - Handler: {}, EventType: {}, Priority: {}",
                handler.getName(), eventType.getSimpleName(), handler.getPriority());

            return true;

        } catch (Exception e) {
            log.error("Failed to register handler: {} for event type: {}",
                handler != null ? handler.getName() : "unknown",
                eventType != null ? eventType.getSimpleName() : "unknown", e);
            return false;
        }
    }

    /**
     * Find all handlers for a specific event type
     *
     * @param eventType Event class type
     * @return List of handlers sorted by priority (high to low), or empty list if none found
     *
     * Usage:
     * List<EventHandler<?>> handlers = registry.findHandlers(LockBoxEvent.class);
     *
     * NOTE: Type parameter T is not bound to Event to support Avro-generated classes
     */
    public <T> List<EventHandler<?>> findHandlers(Class<T> eventType) {
        if (eventType == null) {
            log.warn("Cannot find handlers: eventType is null");
            return Collections.emptyList();
        }

        // Direct lookup
        List<EventHandler<?>> handlers = handlerMap.getOrDefault(eventType, Collections.emptyList());

        if (!handlers.isEmpty()) {
            log.debug("Found {} handlers for event type: {}", handlers.size(), eventType.getSimpleName());
        } else {
            log.debug("No handlers found for event type: {}", eventType.getSimpleName());
        }

        return new ArrayList<>(handlers);  // Return copy to prevent external modification
    }

    /**
     * Find a single handler for an event type (by name)
     * Useful when you want to invoke a specific handler
     *
     * @param eventType Event class type
     * @param handlerName Handler name to search for
     * @return Handler if found, null otherwise
     *
     * NOTE: Type parameter T is not bound to Event to support Avro-generated classes
     */
    public <T> EventHandler<?> findHandler(Class<T> eventType, String handlerName) {
        if (eventType == null || handlerName == null) {
            return null;
        }

        List<EventHandler<?>> handlers = handlerMap.getOrDefault(eventType, Collections.emptyList());
        return handlers.stream()
            .filter(h -> h.getName().equals(handlerName))
            .findFirst()
            .orElse(null);
    }

    /**
     * Get all registered event types
     *
     * @return Set of all event class types with handlers
     */
    public Set<Class<?>> getRegisteredEventTypes() {
        return Collections.unmodifiableSet(handlerMap.keySet());
    }

    /**
     * Get count of handlers for a specific event type
     *
     * @param eventType Event class type
     * @return Number of handlers for this event type
     */
    public int getHandlerCount(Class<?> eventType) {
        if (eventType == null) {
            return 0;
        }
        List<EventHandler<?>> handlers = handlerMap.get(eventType);
        return handlers != null ? handlers.size() : 0;
    }

    /**
     * Get total count of registered handlers (across all event types)
     *
     * @return Total number of registered handlers
     */
    public int getTotalHandlerCount() {
        return handlerMap.values().stream()
            .mapToInt(List::size)
            .sum();
    }

    /**
     * Unregister a specific handler
     *
     * @param eventType Event class type
     * @param handlerName Handler name to remove
     * @return true if handler was found and removed, false otherwise
     */
    public boolean unregister(Class<?> eventType, String handlerName) {
        if (eventType == null || handlerName == null) {
            return false;
        }

        List<EventHandler<?>> handlers = handlerMap.get(eventType);
        if (handlers != null) {
            boolean removed = handlers.removeIf(h -> h.getName().equals(handlerName));
            if (removed) {
                log.info("Handler unregistered - HandlerName: {}, EventType: {}",
                    handlerName, eventType.getSimpleName());
                if (handlers.isEmpty()) {
                    handlerMap.remove(eventType);
                }
                return true;
            }
        }

        return false;
    }

    /**
     * Clear all handlers for a specific event type
     *
     * @param eventType Event class type
     */
    public void clearHandlers(Class<?> eventType) {
        if (eventType != null) {
            handlerMap.remove(eventType);
            log.info("Cleared all handlers for event type: {}", eventType.getSimpleName());
        }
    }

    /**
     * Clear all registered handlers (mainly for testing)
     */
    public void clearAll() {
        handlerMap.clear();
        log.info("Cleared all registered handlers");
    }

    /**
     * Get registry status for monitoring/debugging
     *
     * @return RegistryStatus with statistics
     */
    public RegistryStatus getStatus() {
        return RegistryStatus.builder()
            .registeredEventTypes(getRegisteredEventTypes().size())
            .totalHandlers(getTotalHandlerCount())
            .eventTypes(new ArrayList<>(getRegisteredEventTypes().stream()
                .map(c -> c.getSimpleName() + "(" + getHandlerCount(c) + ")")
                .toList()))
            .timestamp(java.time.Instant.now())
            .build();
    }

    /**
     * Log registry status (for debugging)
     */
    public void logStatus() {
        RegistryStatus status = getStatus();
        log.info("=== Event Handler Registry Status ===");
        log.info("Registered Event Types: {}", status.getRegisteredEventTypes());
        log.info("Total Handlers: {}", status.getTotalHandlers());
        log.info("Details: {}", status.getEventTypes());
        log.info("Updated: {}", status.getTimestamp());
        log.info("=====================================");
    }

    /**
     * Registry status data class
     */
    public static class RegistryStatus {
        private int registeredEventTypes;
        private int totalHandlers;
        private List<String> eventTypes;
        private java.time.Instant timestamp;

        public RegistryStatus(int registeredEventTypes, int totalHandlers, List<String> eventTypes, java.time.Instant timestamp) {
            this.registeredEventTypes = registeredEventTypes;
            this.totalHandlers = totalHandlers;
            this.eventTypes = eventTypes;
            this.timestamp = timestamp;
        }

        public static RegistryStatusBuilder builder() {
            return new RegistryStatusBuilder();
        }

        public int getRegisteredEventTypes() {
            return registeredEventTypes;
        }

        public int getTotalHandlers() {
            return totalHandlers;
        }

        public List<String> getEventTypes() {
            return eventTypes;
        }

        public java.time.Instant getTimestamp() {
            return timestamp;
        }

        public static class RegistryStatusBuilder {
            private int registeredEventTypes;
            private int totalHandlers;
            private List<String> eventTypes;
            private java.time.Instant timestamp;

            public RegistryStatusBuilder registeredEventTypes(int registeredEventTypes) {
                this.registeredEventTypes = registeredEventTypes;
                return this;
            }

            public RegistryStatusBuilder totalHandlers(int totalHandlers) {
                this.totalHandlers = totalHandlers;
                return this;
            }

            public RegistryStatusBuilder eventTypes(List<String> eventTypes) {
                this.eventTypes = eventTypes;
                return this;
            }

            public RegistryStatusBuilder timestamp(java.time.Instant timestamp) {
                this.timestamp = timestamp;
                return this;
            }

            public RegistryStatus build() {
                return new RegistryStatus(registeredEventTypes, totalHandlers, eventTypes, timestamp);
            }
        }

        @Override
        public String toString() {
            return String.format(
                "RegistryStatus{types=%d, handlers=%d, timestamp=%s}",
                registeredEventTypes, totalHandlers, timestamp
            );
        }
    }
}
