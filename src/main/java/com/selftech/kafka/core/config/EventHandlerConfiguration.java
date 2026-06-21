package com.selftech.kafka.core.config;

import com.selftech.anomaly_detection.handler.AnomalyAlertEventHandler;
import com.selftech.kafka.core.handler.EventHandlerRegistry;
import com.selftech.kafka.models.avro.AnomalyDetectedEvent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Event Handler Configuration - PHASE 5
 *
 * Registers all event handlers with the EventHandlerRegistry at application startup.
 *
 * Architecture:
 * 1. Spring Boot starts
 * 2. @PostConstruct method runs
 * 3. All handlers are registered with their event types
 * 4. CentralEventConsumer can now lookup and invoke handlers
 *
 * Flow:
 * Kafka → CentralEventConsumer → EventHandlerRegistry.findHandlers(eventClass)
 *      → Returns registered handlers → Invoke handler.handle(event)
 *
 * Adding New Handlers:
 * 1. Create handler class implementing EventHandler<T>
 * 2. Add @Component annotation to handler
 * 3. Autowire handler in this configuration class
 * 4. Register in registerHandlers() method
 *
 * Example:
 * @Autowired
 * private ParkingAnomalyHandler parkingAnomalyHandler;
 *
 * In registerHandlers():
 * registry.register(parkingAnomalyHandler, ParkingAnomalyEvent.class);
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class EventHandlerConfiguration {

    private final EventHandlerRegistry registry;
    private final AnomalyAlertEventHandler anomalyAlertEventHandler;

    /**
     * Register all event handlers at application startup
     *
     * This method is called automatically by Spring after the bean is constructed
     * and all dependencies are injected.
     *
     * Each handler is registered with its corresponding event type.
     * The registry validates and stores the handler for later lookup.
     */
    @PostConstruct
    public void registerHandlers() {
        log.info("=== Starting Event Handler Registration ===");

        try {
            // Register anomaly alert handler
            boolean registered = registry.register(
                anomalyAlertEventHandler,
                AnomalyDetectedEvent.class
            );

            if (!registered) {
                log.error("Failed to register AnomalyAlertEventHandler - may already be registered");
            }

            // TODO: Register additional handlers here as they are implemented
            // Example:
            // registry.register(parkingAnomalyHandler, ParkingAnomalyEvent.class);
            // registry.register(lockBoxEventHandler, LockBoxEvent.class);
            // registry.register(sensorDataHandler, SensorDataEvent.class);

            // Log final registry status
            log.info("=== Event Handler Registration Complete ===");
            registry.logStatus();

        } catch (Exception e) {
            log.error("Error during event handler registration", e);
            throw new RuntimeException("Failed to initialize event handlers", e);
        }
    }

    /**
     * Validate that critical handlers are registered
     * Called after registration to ensure required handlers are present
     */
    private void validateCriticalHandlers() {
        // Check for anomaly handler (critical for Phase 5)
        int anomalyHandlerCount = registry.getHandlerCount(AnomalyDetectedEvent.class);
        if (anomalyHandlerCount == 0) {
            log.error("CRITICAL: No handlers registered for AnomalyDetectedEvent");
            throw new RuntimeException("Missing critical handler: AnomalyDetectedEvent");
        }

        log.info("Validation passed - All critical handlers are registered");
    }
}
