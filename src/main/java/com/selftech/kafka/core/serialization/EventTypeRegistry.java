package com.selftech.kafka.core.serialization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Service;

/**
 * Event Type Registry
 *
 * Dynamic registry: Event type string → Avro SpecificRecord class mapping.
 * Each consuming service registers its own event types at startup via
 * the register() method or Spring @Configuration classes.
 *
 * Usage:
 *   // Register event types in your service's @Configuration
 *   @Bean
 *   public EventTypeRegistrar myEventTypeRegistrar(EventTypeRegistry registry) {
 *       registry.register("LockBoxEvent", LockBoxEvent.class);
 *       registry.register("PaymentEvent", PaymentEvent.class);
 *       return () -> {};
 *   }
 *
 *   // Lookup at runtime
 *   Class<? extends SpecificRecord> clazz = registry.getEventClass("LockBoxEvent");
 */
@Service
@Slf4j
public class EventTypeRegistry {

    private final Map<String, Class<? extends SpecificRecord>> eventTypeMap = new ConcurrentHashMap<>();

    /**
     * Register an event type mapping
     *
     * @param eventTypeName Event type name (e.g., "LockBoxEvent")
     * @param eventClass Avro SpecificRecord class
     */
    public void register(String eventTypeName, Class<? extends SpecificRecord> eventClass) {
        if (eventTypeName == null || eventClass == null) {
            log.warn("Cannot register null event type name or class");
            return;
        }
        eventTypeMap.put(eventTypeName, eventClass);
        log.info("Registered event type: {} → {}", eventTypeName, eventClass.getName());
    }

    /**
     * Get Avro class for a given event type name
     *
     * @param eventTypeName Event type string (e.g., "ParkExitEvent", "LockBoxEvent")
     * @return Avro SpecificRecord class, or null if not found
     */
    public Class<? extends SpecificRecord> getEventClass(String eventTypeName) {
        if (eventTypeName == null || eventTypeName.isEmpty()) {
            log.warn("Event type name cannot be null or empty");
            return null;
        }

        Class<? extends SpecificRecord> clazz = eventTypeMap.get(eventTypeName);

        if (clazz == null) {
            log.warn("Unknown event type: {} - registered types: {}",
                    eventTypeName, eventTypeMap.keySet());
            return null;
        }

        log.debug("Found event class for type: {} → {}", eventTypeName, clazz.getSimpleName());
        return clazz;
    }

    /**
     * Check if event type is registered
     */
    public boolean isEventTypeRegistered(String eventTypeName) {
        return eventTypeMap.containsKey(eventTypeName);
    }

    /**
     * Get all registered event type names
     */
    public Set<String> getRegisteredEventTypes() {
        return Collections.unmodifiableSet(eventTypeMap.keySet());
    }

    /**
     * Get total count of registered event types
     */
    public int getEventTypeCount() {
        return eventTypeMap.size();
    }

    /**
     * Log registry status
     */
    public void logStatus() {
        log.info("=== Event Type Registry Status ===");
        log.info("Total registered event types: {}", getEventTypeCount());
        log.info("Event types: {}", getRegisteredEventTypes());
        log.info("====================================");
    }

    /**
     * Get human-readable information about an event type
     */
    public String getEventTypeInfo(String eventTypeName) {
        Class<? extends SpecificRecord> clazz = getEventClass(eventTypeName);
        if (clazz == null) {
            return null;
        }
        return String.format(
                "EventType: %s, FullyQualifiedName: %s, CanonicalName: %s",
                eventTypeName, clazz.getName(), clazz.getCanonicalName()
        );
    }
}
