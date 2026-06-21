package com.selftech.kafka.core.serialization;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Service;

import com.selftech.selfpark.avro.ParkEntryEvent;
import com.selftech.selfpark.avro.ParkExitEvent;
import com.selftech.smartlock.avro.LockBoxEvent;
import com.selftech.smartlock.avro.LockDeviceEvent;
import com.selftech.smartlock.avro.PaymentEvent;
import com.selftech.smartlock.avro.TransactionEvent;
import com.selftech.smartlock.kafka.events.LockOperationEvent;
import com.selftech.kafka.models.avro.AnomalyDetectedEvent;
import com.selftech.kafka.models.avro.SensorDataEvent;
import com.selftech.kafka.models.avro.SmsEvent;/*  */

/**
 * Event Type Registry - FAZE 4.5 AVRO STANDARDIZATION
 *
 * Merkezi registry: Event type string → Avro SpecificRecord class mapping
 *
 * Purpose:
 * 1. Polymorphic deserialization - eventType string'ine göre class bul
 * 2. Type safety - compile-time checked class references
 * 3. Centralized management - Tüm event types bir yerde
 * 4. Easy lookup - O(1) hash lookup performance
 *
 * Usage:
 *
 * // Bulunduğun event type'ını bilerek deserialize et
 * String eventType = "ParkExitEvent";  // Bu info database'den gelir
 * Class<? extends SpecificRecord> clazz = eventTypeRegistry.getEventClass(eventType);
 * // clazz = ParkExitEvent.class
 *
 * ParkExitEvent event = eventSerializationService.deserializeBase64ToAvro(
 *     base64Payload,
 *     clazz  // ← Bulduğumuz class
 * );
 *
 * Event Hierarchy:
 * ├─ SmartLock Events (6)
 * │  ├─ LockBoxEvent
 * │  ├─ LockDeviceEvent
 * │  ├─ LockOperationEvent
 * │  ├─ PaymentEvent
 * │  ├─ TransactionEvent
 * │  └─ (reserved for future)
 * │
 * ├─ SelfPark Events (2)
 * │  ├─ ParkEntryEvent
 * │  └─ ParkExitEvent
 * │
 * ├─ Kafka Core Events (3)
 * │  ├─ AnomalyDetectedEvent
 * │  ├─ SensorDataEvent
 * │  └─ SmsEvent
 * │
 * └─ Total: 11 Avro Event Types
 *
 * @author FAZE 4.5 Implementation
 */
@Service
@Slf4j
public class EventTypeRegistry {

    /**
     * Immutable map: Event type name → Avro SpecificRecord class
     * Compile-time type safety, runtime O(1) lookup
     */
    private static final Map<String, Class<? extends SpecificRecord>> EVENT_TYPE_MAP;

    static {
        Map<String, Class<? extends SpecificRecord>> map = new HashMap<>();

        // ============ SmartLock Events ============
        map.put("LockBoxEvent", LockBoxEvent.class);
        map.put("LockDeviceEvent", LockDeviceEvent.class);
        map.put("LockOperationEvent", LockOperationEvent.class);
        map.put("PaymentEvent", PaymentEvent.class);
        map.put("TransactionEvent", TransactionEvent.class);

        // ============ SelfPark Events ============
        map.put("ParkEntryEvent", ParkEntryEvent.class);
        map.put("ParkExitEvent", ParkExitEvent.class);

        // ============ Kafka Core Events (Anomaly Detection, Monitoring) ============
        map.put("AnomalyDetectedEvent", AnomalyDetectedEvent.class);
        map.put("SensorDataEvent", SensorDataEvent.class);
        map.put("SmsEvent", SmsEvent.class);

        // Make immutable for thread safety
        EVENT_TYPE_MAP = Collections.unmodifiableMap(map);

        log.info("EventTypeRegistry initialized with {} event types", EVENT_TYPE_MAP.size());
        log.debug("Registered event types: {}", EVENT_TYPE_MAP.keySet());
    }

    /**
     * Get Avro class for a given event type name
     *
     * Used for polymorphic deserialization:
     * 1. Database'den eventType string'ini al
     * 2. registry.getEventClass(eventType) ile class bul
     * 3. EventSerializationService ile typed deserialization yap
     *
     * @param eventTypeName Event type string (e.g., "ParkExitEvent", "LockBoxEvent")
     * @return Avro SpecificRecord class, or null if not found
     *
     * Example:
     * Class<? extends SpecificRecord> clazz = registry.getEventClass("ParkExitEvent");
     * // clazz = ParkExitEvent.class
     *
     * if (clazz == null) {
     *     log.warn("Unknown event type: {}", eventTypeName);
     *     return null;
     * }
     */
    public Class<? extends SpecificRecord> getEventClass(String eventTypeName) {
        if (eventTypeName == null || eventTypeName.isEmpty()) {
            log.warn("Event type name cannot be null or empty");
            return null;
        }

        Class<? extends SpecificRecord> clazz = EVENT_TYPE_MAP.get(eventTypeName);

        if (clazz == null) {
            log.warn("Unknown event type: {} - registered types: {}",
                    eventTypeName, EVENT_TYPE_MAP.keySet());
            return null;
        }

        log.debug("Found event class for type: {} → {}", eventTypeName, clazz.getSimpleName());
        return clazz;
    }

    /**
     * Check if event type is registered
     *
     * @param eventTypeName Event type string
     * @return true if registered, false otherwise
     */
    public boolean isEventTypeRegistered(String eventTypeName) {
        return EVENT_TYPE_MAP.containsKey(eventTypeName);
    }

    /**
     * Get all registered event type names
     *
     * Useful for logging, validation, statistics
     *
     * @return Unmodifiable set of event type names
     */
    public Set<String> getRegisteredEventTypes() {
        return EVENT_TYPE_MAP.keySet();
    }

    /**
     * Get total count of registered event types
     *
     * @return Number of event types
     */
    public int getEventTypeCount() {
        return EVENT_TYPE_MAP.size();
    }

    /**
     * Log registry status (for debugging)
     */
    public void logStatus() {
        log.info("=== Event Type Registry Status ===");
        log.info("Total registered event types: {}", getEventTypeCount());
        log.info("Event types: {}", getRegisteredEventTypes());
        log.info("====================================");
    }

    /**
     * Get human-readable information about an event type
     *
     * @param eventTypeName Event type string
     * @return Info string or null if not found
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
