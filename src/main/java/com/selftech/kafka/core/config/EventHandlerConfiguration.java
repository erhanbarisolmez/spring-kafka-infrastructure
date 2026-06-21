package com.selftech.kafka.core.config;

import com.selftech.kafka.core.handler.EventHandler;
import com.selftech.kafka.core.handler.EventHandlerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Event Handler Configuration
 *
 * Auto-discovers all EventHandler beans and registers them with the EventHandlerRegistry.
 * Each consuming service provides its own EventHandler implementations as Spring beans,
 * and this configuration automatically registers them at startup.
 *
 * Usage in consuming services:
 * 1. Create handler class implementing EventHandler<T>
 * 2. Add @Component annotation to handler
 * 3. Handler is automatically discovered and registered
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class EventHandlerConfiguration {

    private final EventHandlerRegistry registry;

    @Autowired(required = false)
    private List<EventHandler<?>> eventHandlers;

    @PostConstruct
    public void registerHandlers() {
        log.info("=== Starting Event Handler Auto-Registration ===");

        if (eventHandlers == null || eventHandlers.isEmpty()) {
            log.info("No EventHandler beans found - skipping registration");
            return;
        }

        for (EventHandler<?> handler : eventHandlers) {
            try {
                Class<?> eventType = handler.getEventType();
                registerHandlerUnchecked(handler, eventType);
                log.info("Auto-registered handler: {} for event type: {}",
                    handler.getName(), eventType.getSimpleName());
            } catch (Exception e) {
                log.warn("Could not auto-register handler: {} - {}",
                    handler.getName(), e.getMessage());
            }
        }

        log.info("=== Event Handler Registration Complete ===");
        registry.logStatus();
    }

    @SuppressWarnings("unchecked")
    private <T> void registerHandlerUnchecked(EventHandler<?> handler, Class<?> eventType) {
        registry.register((EventHandler<T>) handler, (Class<T>) eventType);
    }
}
