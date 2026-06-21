package com.selftech.kafka.converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Event Converter Factory - FAZE 4.5
 *
 * Polymorphic factory for JSON to Avro event conversion.
 *
 * Purpose:
 * 1. Manage all event converters (SensorDataEventConverter,
 * LockBoxEventConverter, etc.)
 * 2. Route incoming JSON to appropriate converter based on topic name
 * 3. Provide centralized converter discovery and lifecycle management
 * 4. Support for future converter implementations
 *
 * Architecture:
 * ```
 * JSON Input (from MQTT/API)
 * ↓
 * EventConverterFactory.getConverter(topic)
 * ↓
 * Topic-based lookup (topic patterns)
 * ↓
 * Appropriate EventConverter (SensorDataEventConverter, etc.)
 * ↓
 * Convert to Avro event
 * ↓
 * Kafka producer
 * ```
 *
 * Usage:
 * ```
 * // Get converter for topic
 * EventConverter<?> converter =
 * factory.getConverter("smartlock.mqtt.sensorData.v0");
 *
 * // Convert JSON to Avro
 * SensorDataEvent event = (SensorDataEvent) converter.convert(jsonInput);
 *
 * // Publish to Kafka
 * eventProducer.sendEvent(topic, key, event);
 * ```
 *
 * Converter Registration:
 * Converters are auto-discovered via Spring component scanning:
 * 1. All beans implementing EventConverter are collected
 * 2. Added to factory's converter map at startup
 * 3. Factory becomes a @Service bean with all converters injected
 *
 * Example:
 * 
 * @Component
 *            public class SensorDataEventConverter extends
 *            AbstractEventConverter<SensorDataEvent> {
 *            // Implementation...
 *            }
 *
 *            // Factory automatically discovers and registers this converter
 */
@Service
@Slf4j
public class EventConverterFactory {

  /**
   * Map: topic pattern → converter
   * Used for quick lookup by topic name
   */
  private final Map<String, EventConverter<?>> converterRegistry;

  /**
   * List of all registered converters
   * Used for pattern matching and iteration
   */
  private final List<EventConverter<?>> converters;

  /**
   * Create factory with auto-discovered converters
   *
   * Spring automatically injects all EventConverter implementations
   * into the converters list parameter.
   *
   * @param converters All EventConverter implementations found via component
   *                   scanning
   */
  @Autowired(required = false)
  public EventConverterFactory(List<EventConverter<?>> converters) {
    this.converters = converters != null ? converters : List.of();
    this.converterRegistry = new HashMap<>();

    // Initialize registry
    initializeConverters();
  }

  /**
   * Initialize converter registry
   *
   * Called during factory construction to register all discovered converters
   */
  private void initializeConverters() {
    if (converters.isEmpty()) {
      log.warn("No EventConverter implementations found - converter factory will be empty");
      return;
    }

    log.info("Initializing EventConverterFactory with {} converters", converters.size());

    for (EventConverter<?> converter : converters) {
      log.info("Registering converter: {} for patterns: {}",
          converter.getConverterName(),
          String.join(", ", converter.getTopicPatterns()));

      // Store converter by topic pattern
      for (String pattern : converter.getTopicPatterns()) {
        converterRegistry.put(pattern, converter);
      }
    }

    log.info("EventConverterFactory initialized with {} converters and {} topic patterns",
        converters.size(), converterRegistry.size());
    logStatus();
  }

  /**
   * Get converter for a given topic
   *
   * Looks up converter using topic-based pattern matching:
   * 1. Try exact pattern match first
   * 2. Try wildcard pattern matching
   * 3. Return null if no converter found (will be routed to DLQ)
   *
   * @param topic Topic name (e.g., "smartlock.mqtt.sensorData.v0")
   * @return Appropriate EventConverter or null if not found
   *
   *         Example:
   *         EventConverter<?> converter =
   *         factory.getConverter("smartlock.mqtt.sensorData.v0");
   *         if (converter != null) {
   *         SensorDataEvent event = (SensorDataEvent)
   *         converter.convert(jsonInput);
   *         }
   */
  public EventConverter<?> getConverter(String topic) {
    if (topic == null || topic.isEmpty()) {
      log.warn("Cannot get converter for null or empty topic");
      return null;
    }

    // Try exact pattern match first (faster)
    if (converterRegistry.containsKey(topic)) {
      log.debug("Found converter for topic '{}' via exact pattern match", topic);
      return converterRegistry.get(topic);
    }

    // Try wildcard pattern matching
    for (EventConverter<?> converter : converters) {
      if (converter.canHandle(topic)) {
        log.debug("Found converter for topic '{}' via pattern matching: {}",
            topic, converter.getConverterName());
        return converter;
      }
    }

    log.warn("No converter found for topic: {}. Available converters: {}",
        topic, getAvailableTopicPatterns());
    return null;
  }

  /**
   * Get converter by event type class
   *
   * Useful when you know the target event type but not the topic
   *
   * @param eventClass Target Avro event class (e.g., SensorDataEvent.class)
   * @return Converter for this event type or null if not found
   *
   *         Example:
   *         EventConverter<SensorDataEvent> converter =
   *         factory.getConverterByEventClass(SensorDataEvent.class);
   */
  @SuppressWarnings("unchecked")
  public <T extends SpecificRecord> EventConverter<T> getConverterByEventClass(Class<T> eventClass) {
    if (eventClass == null) {
      log.warn("Cannot get converter for null event class");
      return null;
    }

    for (EventConverter<?> converter : converters) {
      if (converter.getTargetEventClass().equals(eventClass)) {
        log.debug("Found converter for event class: {}", eventClass.getSimpleName());
        return (EventConverter<T>) converter;
      }
    }

    log.warn("No converter found for event class: {}. Available event classes: {}",
        eventClass.getSimpleName(), getAvailableEventClasses());
    return null;
  }

  /**
   * Check if converter exists for given topic
   *
   * @param topic Topic name
   * @return true if converter is available
   */
  public boolean hasConverter(String topic) {
    return getConverter(topic) != null;
  }

  /**
   * Get all registered converters
   *
   * Useful for statistics and monitoring
   *
   * @return Unmodifiable list of all converters
   */
  public List<EventConverter<?>> getAllConverters() {
    return List.copyOf(converters);
  }

  /**
   * Get all available topic patterns
   *
   * Useful for logging and validation
   *
   * @return All topic patterns this factory can handle
   */
  public String[] getAvailableTopicPatterns() {
    return converterRegistry.keySet().toArray(new String[0]);
  }

  /**
   * Get all available event classes
   *
   * Useful for validation and statistics
   *
   * @return All target event classes supported by registered converters
   */
  public String[] getAvailableEventClasses() {
    return converters.stream()
        .map(c -> c.getTargetEventClass().getSimpleName())
        .distinct()
        .toArray(String[]::new);
  }

  /**
   * Get total count of registered converters
   */
  public int getConverterCount() {
    return converters.size();
  }

  /**
   * Convert JSON using appropriate converter
   *
   * End-to-end conversion helper:
   * 1. Find converter for topic
   * 2. Convert JSON to Avro
   * 3. Return event or throw exception
   *
   * @param topic     Topic name (used to select converter)
   * @param jsonInput JSON string to convert
   * @return Avro event ready for Kafka
   * @throws EventConversionException   if conversion fails
   * @throws ConverterNotFoundException if no converter found for topic
   *
   *                                    Example:
   *                                    try {
   *                                    SpecificRecord event =
   *                                    factory.convert("smartlock.mqtt.sensorData.v0",
   *                                    jsonInput);
   *                                    eventProducer.sendEvent(topic, key,
   *                                    event);
   *                                    } catch (ConverterNotFoundException e) {
   *                                    log.error("No converter for topic: {}",
   *                                    topic);
   *                                    // Route to DLQ
   *                                    } catch (EventConversionException e) {
   *                                    log.error("Conversion failed: {}",
   *                                    e.getMessage());
   *                                    // Route to DLQ
   *                                    }
   */
  public SpecificRecord convert(String topic, String jsonInput)
      throws EventConversionException, ConverterNotFoundException {

    log.debug("Converting JSON for topic: {}", topic);

    // Find converter
    EventConverter<?> converter = getConverter(topic);
    if (converter == null) {
      String errorMsg = String.format("No converter found for topic: %s. Available topics: %s",
          topic, String.join(", ", getAvailableTopicPatterns()));
      log.error(errorMsg);
      throw new ConverterNotFoundException(topic, errorMsg);
    }

    // Convert
    try {
      SpecificRecord event = converter.convert(jsonInput);
      log.debug("JSON conversion successful for topic: {} using converter: {}",
          topic, converter.getConverterName());
      return event;

    } catch (EventConversionException e) {
      log.error("Conversion failed for topic: {} - {}", topic, e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Log factory status (for debugging and monitoring)
   */
  public void logStatus() {
    log.info("=== EventConverterFactory Status ===");
    log.info("Total converters: {}", getConverterCount());
    log.info("Available event classes: {}", String.join(", ", getAvailableEventClasses()));

    converters.forEach(c -> log.info("  - {}: patterns={}",
        c.getConverterName(),
        String.join(", ", c.getTopicPatterns())));

    log.info("=====================================");
  }

  /**
   * Exception thrown when no converter found for topic
   */
  public static class ConverterNotFoundException extends Exception {
    private final String topic;

    public ConverterNotFoundException(String topic, String message) {
      super(message);
      this.topic = topic;
    }

    public String getTopic() {
      return topic;
    }
  }
}
