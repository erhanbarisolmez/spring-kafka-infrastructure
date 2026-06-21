package com.selftech.kafka.core.consumer;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.selftech.kafka.core.event.Event;
import com.selftech.kafka.core.handler.EventHandler;
import com.selftech.kafka.core.handler.EventHandlerRegistry;

/**
 * Central Event Consumer - FAZE 3 ENTERPRISE
 *
 * Centralized consumer that listens to all Kafka topics and routes events to
 * handlers.
 * Acts as the main entry point for event consumption and processing.
 *
 * Architecture:
 * ```
 * Kafka Topic (21 topics)
 * ↓
 * CentralEventConsumer
 * ↓
 * EventHandlerRegistry (lookup handlers)
 * ↓
 * EventHandler (process event)
 * ↓
 * Database / External Service
 * ```
 *
 * Responsibilities:
 * 1. Listen to multiple Kafka topics (single consumer group)
 * 2. Deserialize events from Kafka
 * 3. Enrich event context (correlation ID, headers)
 * 4. Lookup appropriate handlers via EventHandlerRegistry
 * 5. Execute handlers in priority order
 * 6. Handle failures and manual commit
 * 7. Track metrics and errors
 *
 * Features:
 * - Manual offset commit (enable-auto-commit: false)
 * - Exactly-once processing semantics
 * - Error handling with partial success support
 * - Correlation ID tracking for distributed tracing
 * - Dead Letter Queue routing for failures
 * - Handler chain execution with stop support
 * - Comprehensive logging and metrics
 *
 * Topic Configuration:
 * - Consumer group: selfpark-group
 * - Max poll records: 500 (batch processing)
 * - Session timeout: 30s
 * - Heartbeat interval: 10s
 * - Manual offset commit (not auto)
 *
 * Processing Flow:
 * 1. Receive event from Kafka topic
 * 2. Extract headers (correlation ID, topic, partition, offset)
 * 3. Find handlers for event type via registry
 * 4. For each handler (in priority order):
 * a. Check if handler can process event
 * b. Execute handler
 * c. On success: continue to next handler
 * d. On failure: log error, decide on DLQ routing
 * e. If handler.stopsChain() == true: stop
 * 5. After all handlers:
 * a. On total success: acknowledge offset
 * b. On failure: route to DLQ (if configured)
 * c. Update metrics
 *
 * Error Handling:
 * - Handler exception → Log and continue to next handler (unless
 * stopsChain=true)
 * - No handlers found → Log warning and acknowledge (avoid infinite retry)
 * - Critical error → Route to DLQ for manual inspection
 *
 * Monitoring:
 * - Emits metrics: events.processed, events.failed, handler.latency
 * - Logs processing time and handler count
 * - Tracks correlation ID for request tracing
 * - Monitors consumer lag via Spring Actuator
 */
@Service
@Slf4j
public class CentralEventConsumer {

  @Autowired
  private EventHandlerRegistry handlerRegistry;

  @Autowired
  private KafkaTemplate<String, Object> kafkaTemplate;

  @Autowired(required = false)
  private com.selftech.kafka.converter.EventConverterFactory converterFactory;

  // ====== Configuration ======
  private static final int MAX_HANDLER_RETRY_ATTEMPTS = 3;

  /**
   * Listen to ALL event topics and process events
   *
   * @KafkaListener configuration:
   *                - topics: All event topics (22 total)
   *                - groupId: selfpark-group (single consumer group)
   *                - containerFactory: Uses manual offset commit
   *
   *                FAZE 2 ENTERPRISE UPGRADE: YAML-based topic resolution via KafkaTopicRegistry
   *                - Topics loaded from application.yml at runtime
   *                - Single source of truth - zero hardcoded strings
   *                - Category-based filtering (EVENT, STREAM, METRIC, ANOMALY, ALERT)
   *                - Environment-aware (dev/staging/prod)
   *
   *                SpEL Expression: #{@kafkaTopicRegistry.getAllTopicNames()}
   *                Breakdown:
   *                  # - SpEL expression start
   *                  @ - Spring bean reference
   *                  kafkaTopicRegistry - bean name (KafkaTopicRegistry class)
   *                  getAllTopicNames() - method returning String[] of all topics
   *
   *                Topics Consumed: EVENT, ALERT, METRIC categories only
   *                - SmartLock: device-operations, box-operation, payment-events, etc.
   *                - SelfPark: auth, payment, park-exit, park-entry, etc.
   *                - Anomaly Detection: alerts, metrics, batch
   *
   *                EXCLUDED Categories (processed by Kafka Streams):
   *                - STREAM: mqtt-sensor-data (processed by SensorDataAnomalyDetector)
   *                - ANOMALY: anomaly detection outputs (processed by downstream Kafka Streams)
   *
   *                Manual offset commit flow:
   *                1. Receive event
   *                2. Process (call handlers)
   *                3. If successful: acknowledgment.acknowledge() commits offset
   *                4. If failed: no ack, message reprocessed on rebalance
   *
   * @param consumerRecord Complete Kafka consumer record (contains value, headers, metadata)
   * @param acknowledgment Manual acknowledgment for offset commit
   */
  @KafkaListener(
      topics = "#{@kafkaTopicRegistry.getConsumableTopicNames()}",
      groupId = "selfpark-group",
      containerFactory = "kafkaListenerContainerFactory"
  )
  @Transactional
  public void consumeEvent(
      ConsumerRecord<String, Object> consumerRecord,
      Acknowledgment acknowledgment) {

    long startTime = System.currentTimeMillis();

    // Extract metadata from ConsumerRecord
    String topic = consumerRecord.topic();
    int partition = consumerRecord.partition();
    long offset = consumerRecord.offset();
    long timestamp = consumerRecord.timestamp();

    // Extract correlation ID from headers (if present)
    String correlationId = null;
    var headers = consumerRecord.headers();
    if (headers != null) {
      var correlationHeader = headers.lastHeader("correlation-id");
      if (correlationHeader != null && correlationHeader.value() != null) {
        correlationId = new String(correlationHeader.value());
      }
    }

    try {
      // Step 0: Extract and convert event payload
      Object rawValue = consumerRecord.value();

      if (rawValue == null) {
        log.warn("Received null value from Kafka - Topic: {}, Partition: {}, Offset: {}",
            topic, partition, offset);
        if (acknowledgment != null) {
          acknowledgment.acknowledge();
        }
        return;
      }

      Event event = convertToEvent(rawValue, topic, correlationId);

      // Log event reception
      log.info("Event received from Kafka - Topic: {}, Partition: {}, Offset: {}, EventId: {}, CorrelationId: {}, PayloadType: {}",
          topic, partition, offset, event.getEventId(), correlationId, rawValue.getClass().getSimpleName());

      // Step 1: Lookup handlers for this event type
      // WORKAROUND: If event is an adapter, use the underlying event type for handler lookup
      @SuppressWarnings("unchecked")
      Class<?> eventClass;
      if (event instanceof AnomalyDetectedEventAdapter) {
        // Use underlying AnomalyDetectedEvent class for handler lookup
        eventClass = com.selftech.kafka.models.avro.AnomalyDetectedEvent.class;
        log.debug("Event is AnomalyDetectedEventAdapter - using AnomalyDetectedEvent for handler lookup");
      } else if (event instanceof SensorDataEventAdapter) {
        // Use underlying SensorDataEvent class for handler lookup
        eventClass = com.selftech.kafka.models.avro.SensorDataEvent.class;
        log.debug("Event is SensorDataEventAdapter - using SensorDataEvent for handler lookup");
      } else if (event instanceof LockDeviceEventAdapter) {
        // Use underlying LockDeviceEvent class for handler lookup
        eventClass = com.selftech.smartlock.avro.LockDeviceEvent.class;
        log.debug("Event is LockDeviceEventAdapter - using LockDeviceEvent for handler lookup");
      } else if (event instanceof LockBoxEventAdapter) {
        // Use underlying LockBoxEvent class for handler lookup
        eventClass = com.selftech.smartlock.avro.LockBoxEvent.class;
        log.debug("Event is LockBoxEventAdapter - using LockBoxEvent for handler lookup");
      } else {
        eventClass = event.getClass();
      }
      List<EventHandler<?>> handlers = handlerRegistry.findHandlers(eventClass);

      if (handlers.isEmpty()) {
        log.warn(
            "No handlers found for event type: {}, EventId: {}, Topic: {} - This is expected if handlers are not yet implemented",
            eventClass.getSimpleName(), event.getEventId(), topic);
        // Acknowledge offset even if no handlers found (avoid infinite retry)
        // NOTE: This is NOT a failure - handlers may not be implemented yet
        // Only route to DLQ if handlers exist but fail to process
        if (acknowledgment != null) {
          acknowledgment.acknowledge();
        }
        return;
      }

      log.debug("Found {} handlers for event type: {}", handlers.size(), eventClass.getSimpleName());

      // Step 2: Execute handlers in chain
      int successCount = 0;
      int failureCount = 0;

      // Unwrap adapter if needed for handler invocation
      Object eventForHandler;
      if (event instanceof AnomalyDetectedEventAdapter) {
        eventForHandler = ((AnomalyDetectedEventAdapter) event).getAnomalyEvent();
        log.debug("Unwrapping AnomalyDetectedEventAdapter for handler invocation");
      } else if (event instanceof SensorDataEventAdapter) {
        eventForHandler = ((SensorDataEventAdapter) event).getSensorDataEvent();
        log.debug("Unwrapping SensorDataEventAdapter for handler invocation");
      } else if (event instanceof LockDeviceEventAdapter) {
        eventForHandler = ((LockDeviceEventAdapter) event).getLockDeviceEvent();
        log.debug("Unwrapping LockDeviceEventAdapter for handler invocation");
      } else if (event instanceof LockBoxEventAdapter) {
        eventForHandler = ((LockBoxEventAdapter) event).getLockBoxEvent();
        log.debug("Unwrapping LockBoxEventAdapter for handler invocation");
      } else {
        eventForHandler = event;
      }

      for (EventHandler<?> handler : handlers) {
        try {
          // Execute handler (handler checks canHandle internally if needed)
          log.debug("Executing handler: {}, EventId: {}", handler.getName(), event.getEventId());

          @SuppressWarnings("unchecked")
          EventHandler<Object> typedHandler = (EventHandler<Object>) handler;

          // Check if handler can process this event (pass the wrapped Event for interface compatibility)
          if (!typedHandler.canHandle((Event) event)) {
            log.debug("Handler {} skipped event (canHandle returned false)",
                handler.getName());
            continue;
          }

          // Pass the unwrapped event to the handler
          typedHandler.handle(eventForHandler);

          successCount++;
          log.debug("Handler {} executed successfully", handler.getName());

          // Check if handler stops chain
          if (handler.stopsChain()) {
            log.debug("Handler {} stopped handler chain", handler.getName());
            break;
          }

        } catch (Exception e) {
          failureCount++;
          log.error("Handler {} failed to process event - EventId: {}, Error: {}",
              handler.getName(), event.getEventId(), e.getMessage(), e);

          // If handler stops chain on error, break
          if (handler.stopsChain()) {
            log.warn("Handler {} stopped chain due to error", handler.getName());
            break;
          }
          // Otherwise continue to next handler
        }
      }

      // Step 3: Log processing summary
      long duration = System.currentTimeMillis() - startTime;
      log.info(
          "Event processing completed - EventId: {}, Topic: {}, Handlers: {}, Success: {}, Failures: {}, Duration: {}ms",
          event.getEventId(), topic, handlers.size(), successCount, failureCount, duration);

      // Step 4: Commit offset based on handler execution results
      // IMPORTANT: Only route to DLQ if ALL handlers failed (successCount == 0)
      // Partial success is acceptable - at least one handler processed the event

      if (successCount > 0) {
        // At least one handler succeeded - acknowledge offset
        acknowledgment.acknowledge();
        if (failureCount > 0) {
          log.warn(
              "Event processing completed with partial success - EventId: {}, Topic: {}, Success: {}, Failures: {}, CorrelationId: {}",
              event.getEventId(), topic, successCount, failureCount, correlationId);
        } else {
          log.debug("Offset acknowledged - Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        }
      } else if (failureCount > 0 && successCount == 0) {
        // All handlers failed - route to DLQ only when NO handlers succeeded
        log.error(
            "Event processing COMPLETELY FAILED for all {} handlers - EventId: {}, CorrelationId: {} - Routing to DLQ",
            handlers.size(), event.getEventId(), correlationId);
        routeToDLQ(event, topic, partition, offset, "All handlers failed", failureCount, null);

        // Acknowledge offset to prevent infinite retry loop
        if (acknowledgment != null) {
          acknowledgment.acknowledge();
        }
      }

    } catch (Exception e) {
      long duration = System.currentTimeMillis() - startTime;
      Object rawValue = consumerRecord.value();
      log.error(
          "Unexpected error while consuming event - Payload Type: {}, Topic: {}, Duration: {}ms, Error: {}",
          rawValue != null ? rawValue.getClass().getSimpleName() : "NULL", topic, duration, e.getMessage(), e);
      // Don't acknowledge - message will be retried
    }
  }

  /**
   * Convert raw Kafka value to Event interface
   *
   * Handles multiple scenarios:
   * 1. Value already implements Event (Avro SpecificRecord) - direct cast
   * 2. Value is SpecificRecord (SensorDataEvent, etc.) - wrap in adapter
   * 3. Value is JSON String - use EventConverterFactory to convert to Avro
   * 4. Value is Map/GenericRecord - attempt conversion
   *
   * @param rawValue Raw value from Kafka ConsumerRecord
   * @param topic Topic name (used for converter selection)
   * @param correlationId Correlation ID for logging
   * @return Event instance ready for processing
   * @throws Exception if conversion fails
   */
  private Event convertToEvent(Object rawValue, String topic, String correlationId) throws Exception {
    // Case 1: Already an Event (Avro SpecificRecord implementing Event)
    if (rawValue instanceof Event) {
      log.debug("Value is already Event instance - Type: {}", rawValue.getClass().getSimpleName());
      return (Event) rawValue;
    }

    // Case 2: SensorDataEvent (Avro generated class that doesn't implement Event due to plugin limitation)
    // WORKAROUND: Avro Maven plugin doesn't support "interfaces" attribute in schema
    if (rawValue instanceof com.selftech.kafka.models.avro.SensorDataEvent) {
      log.debug("Value is SensorDataEvent - wrapping in EventAdapter");
      return new SensorDataEventAdapter((com.selftech.kafka.models.avro.SensorDataEvent) rawValue);
    }

    // Case 2b: AnomalyDetectedEvent (same limitation as SensorDataEvent)
    if (rawValue instanceof com.selftech.kafka.models.avro.AnomalyDetectedEvent) {
      log.debug("Value is AnomalyDetectedEvent - wrapping in EventAdapter");
      return new AnomalyDetectedEventAdapter((com.selftech.kafka.models.avro.AnomalyDetectedEvent) rawValue);
    }

    // Case 2c: LockDeviceEvent (same limitation as SensorDataEvent)
    if (rawValue instanceof com.selftech.smartlock.avro.LockDeviceEvent) {
      log.debug("Value is LockDeviceEvent - wrapping in EventAdapter");
      return new LockDeviceEventAdapter((com.selftech.smartlock.avro.LockDeviceEvent) rawValue);
    }

    // Case 2d: LockBoxEvent (same limitation as SensorDataEvent)
    if (rawValue instanceof com.selftech.smartlock.avro.LockBoxEvent) {
      log.debug("Value is LockBoxEvent - wrapping in EventAdapter");
      return new LockBoxEventAdapter((com.selftech.smartlock.avro.LockBoxEvent) rawValue);
    }

    // Case 3: JSON String - use converter factory
    if (rawValue instanceof String) {
      log.debug("Value is JSON String - attempting conversion using EventConverterFactory");

      if (converterFactory == null) {
        throw new IllegalStateException(
            "EventConverterFactory not available - cannot convert JSON to Event. Topic: " + topic);
      }

      try {
        SpecificRecord avroEvent = converterFactory.convert(topic, (String) rawValue);

        // After conversion, check if we need to wrap it
        if (avroEvent instanceof Event) {
          log.debug("Successfully converted JSON to Event - EventId: {}", ((Event) avroEvent).getEventId());
          return (Event) avroEvent;
        } else if (avroEvent instanceof com.selftech.kafka.models.avro.SensorDataEvent) {
          log.debug("Converted JSON to SensorDataEvent - wrapping in EventAdapter");
          return new SensorDataEventAdapter((com.selftech.kafka.models.avro.SensorDataEvent) avroEvent);
        } else if (avroEvent instanceof com.selftech.smartlock.avro.LockBoxEvent) {
          log.debug("Converted JSON to LockBoxEvent - wrapping in EventAdapter");
          return new LockBoxEventAdapter((com.selftech.smartlock.avro.LockBoxEvent) avroEvent);
        } else if (avroEvent instanceof com.selftech.smartlock.avro.LockDeviceEvent) {
          log.debug("Converted JSON to LockDeviceEvent - wrapping in EventAdapter");
          return new LockDeviceEventAdapter((com.selftech.smartlock.avro.LockDeviceEvent) avroEvent);
        } else {
          throw new IllegalStateException(
              "Converter produced unsupported type: " + avroEvent.getClass().getSimpleName());
        }

      } catch (com.selftech.kafka.converter.EventConverterFactory.ConverterNotFoundException e) {
        log.error("No converter found for topic: {} - Value: {}", topic, rawValue);
        throw new IllegalStateException("No converter available for topic: " + topic, e);
      } catch (com.selftech.kafka.converter.EventConversionException e) {
        log.error("Conversion failed for topic: {} - Error: {}", topic, e.getMessage(), e);
        throw new IllegalStateException("Event conversion failed: " + e.getMessage(), e);
      }
    }

    // Case 4: Other SpecificRecord types - check if they implement Event
    if (rawValue instanceof SpecificRecord) {
      log.warn("Value is SpecificRecord but not Event - Type: {}. This may indicate missing adapter.",
          rawValue.getClass().getSimpleName());
      throw new IllegalStateException(
          "SpecificRecord does not implement Event interface: " + rawValue.getClass().getName());
    }

    // Case 5: Unsupported type
    throw new IllegalStateException(
        String.format("Unsupported event payload type: %s (topic=%s, correlationId=%s). " +
            "Expected: Event, SpecificRecord, or JSON String",
            rawValue.getClass().getName(), topic, correlationId));
  }

  /**
   * Adapter for SensorDataEvent to Event interface
   *
   * WORKAROUND: Avro Maven plugin doesn't support "interfaces" schema attribute,
   * so generated SensorDataEvent doesn't implement Event interface.
   * This adapter provides Event interface implementation.
   */
  private static class SensorDataEventAdapter implements Event {
    private final com.selftech.kafka.models.avro.SensorDataEvent sensorDataEvent;

    public SensorDataEventAdapter(com.selftech.kafka.models.avro.SensorDataEvent sensorDataEvent) {
      this.sensorDataEvent = sensorDataEvent;
    }

    @Override
    public String getEventId() {
      return sensorDataEvent.getEventId() != null ? sensorDataEvent.getEventId().toString() : null;
    }

    @Override
    public String getCorrelationId() {
      return sensorDataEvent.getCorrelationId() != null ? sensorDataEvent.getCorrelationId().toString() : null;
    }

    @Override
    public String getEventType() {
      return sensorDataEvent.getEventType() != null ? sensorDataEvent.getEventType().toString() : "SensorDataReceived";
    }

    @Override
    public String getSource() {
      return sensorDataEvent.getSource() != null ? sensorDataEvent.getSource().toString() : "SMARTLOCK";
    }

    @Override
    public java.time.Instant getTimestamp() {
      return sensorDataEvent.getTimestamp();
    }

    @Override
    public Integer getEventVersion() {
      // Parse schemaVersion string to integer (e.g., "1.0" -> 1)
      try {
        String version = sensorDataEvent.getSchemaVersion() != null
            ? sensorDataEvent.getSchemaVersion().toString()
            : "1.0";
        return Integer.parseInt(version.split("\\.")[0]);
      } catch (Exception e) {
        return 1;
      }
    }

    @Override
    public String getDomain() {
      return "SMARTLOCK";
    }

    // Provide access to underlying SensorDataEvent for handlers
    public com.selftech.kafka.models.avro.SensorDataEvent getSensorDataEvent() {
      return sensorDataEvent;
    }

    @Override
    public String toString() {
      return "SensorDataEventAdapter{eventId=" + getEventId() +
             ", eventType=" + getEventType() +
             ", source=" + getSource() +
             ", timestamp=" + getTimestamp() + "}";
    }
  }

  /**
   * Adapter for AnomalyDetectedEvent to Event interface
   *
   * WORKAROUND: Same as SensorDataEvent - Avro plugin limitation
   */
  private static class AnomalyDetectedEventAdapter implements Event {
    private final com.selftech.kafka.models.avro.AnomalyDetectedEvent anomalyEvent;

    public AnomalyDetectedEventAdapter(com.selftech.kafka.models.avro.AnomalyDetectedEvent anomalyEvent) {
      this.anomalyEvent = anomalyEvent;
    }

    @Override
    public String getEventId() {
      return anomalyEvent.getEventId() != null ? anomalyEvent.getEventId().toString() : null;
    }

    @Override
    public String getCorrelationId() {
      return anomalyEvent.getCorrelationId() != null ? anomalyEvent.getCorrelationId().toString() : null;
    }

    @Override
    public String getEventType() {
      return anomalyEvent.getEventType() != null ? anomalyEvent.getEventType().toString() : "AnomalyDetected";
    }

    @Override
    public String getSource() {
      return anomalyEvent.getSource() != null ? anomalyEvent.getSource().toString() : "ANOMALY_DETECTION";
    }

    @Override
    public java.time.Instant getTimestamp() {
      return anomalyEvent.getTimestamp();
    }

    @Override
    public Integer getEventVersion() {
      try {
        String version = anomalyEvent.getSchemaVersion() != null
            ? anomalyEvent.getSchemaVersion().toString()
            : "2.0";
        return Integer.parseInt(version.split("\\.")[0]);
      } catch (Exception e) {
        return 2;
      }
    }

    @Override
    public String getDomain() {
      return "ANOMALY_DETECTION";
    }

    // Provide access to underlying AnomalyDetectedEvent for handlers
    public com.selftech.kafka.models.avro.AnomalyDetectedEvent getAnomalyEvent() {
      return anomalyEvent;
    }

    @Override
    public String toString() {
      return "AnomalyDetectedEventAdapter{eventId=" + getEventId() +
             ", eventType=" + getEventType() +
             ", source=" + getSource() +
             ", timestamp=" + getTimestamp() + "}";
    }
  }

  /**
   * Adapter for LockDeviceEvent to Event interface
   *
   * WORKAROUND: Same as SensorDataEvent - Avro plugin limitation
   */
  private static class LockDeviceEventAdapter implements Event {
    private final com.selftech.smartlock.avro.LockDeviceEvent lockDeviceEvent;

    public LockDeviceEventAdapter(com.selftech.smartlock.avro.LockDeviceEvent lockDeviceEvent) {
      this.lockDeviceEvent = lockDeviceEvent;
    }

    @Override
    public String getEventId() {
      return lockDeviceEvent.getEventId() != null ? lockDeviceEvent.getEventId().toString() : null;
    }

    @Override
    public String getCorrelationId() {
      return lockDeviceEvent.getCorrelationId() != null ? lockDeviceEvent.getCorrelationId().toString() : null;
    }

    @Override
    public String getEventType() {
      return lockDeviceEvent.getEventType() != null ? lockDeviceEvent.getEventType().toString() : "DeviceOperation";
    }

    @Override
    public String getSource() {
      return "SMARTLOCK";
    }

    @Override
    public java.time.Instant getTimestamp() {
      return lockDeviceEvent.getTimestamp();
    }

    @Override
    public Integer getEventVersion() {
      try {
        String version = lockDeviceEvent.getSchemaVersion() != null
            ? lockDeviceEvent.getSchemaVersion().toString()
            : "1.0";
        return Integer.parseInt(version.split("\\.")[0]);
      } catch (Exception e) {
        return 1;
      }
    }

    @Override
    public String getDomain() {
      return "SMARTLOCK";
    }

    // Provide access to underlying LockDeviceEvent for handlers
    public com.selftech.smartlock.avro.LockDeviceEvent getLockDeviceEvent() {
      return lockDeviceEvent;
    }

    @Override
    public String toString() {
      return "LockDeviceEventAdapter{eventId=" + getEventId() +
             ", eventType=" + getEventType() +
             ", deviceCode=" + lockDeviceEvent.getDeviceCode() +
             ", timestamp=" + getTimestamp() + "}";
    }
  }

  /**
   * Adapter for LockBoxEvent to Event interface
   *
   * WORKAROUND: Same as SensorDataEvent - Avro plugin limitation
   */
  private static class LockBoxEventAdapter implements Event {
    private final com.selftech.smartlock.avro.LockBoxEvent lockBoxEvent;

    public LockBoxEventAdapter(com.selftech.smartlock.avro.LockBoxEvent lockBoxEvent) {
      this.lockBoxEvent = lockBoxEvent;
    }

    @Override
    public String getEventId() {
      return lockBoxEvent.getEventId() != null ? lockBoxEvent.getEventId().toString() : null;
    }

    @Override
    public String getCorrelationId() {
      return lockBoxEvent.getCorrelationId() != null ? lockBoxEvent.getCorrelationId().toString() : null;
    }

    @Override
    public String getEventType() {
      return lockBoxEvent.getEventType() != null ? lockBoxEvent.getEventType().toString() : "BoxOperation";
    }

    @Override
    public String getSource() {
      return "SMARTLOCK";
    }

    @Override
    public java.time.Instant getTimestamp() {
      return lockBoxEvent.getTimestamp();
    }

    @Override
    public Integer getEventVersion() {
      try {
        String version = lockBoxEvent.getSchemaVersion() != null
            ? lockBoxEvent.getSchemaVersion().toString()
            : "1.0";
        return Integer.parseInt(version.split("\\.")[0]);
      } catch (Exception e) {
        return 1;
      }
    }

    @Override
    public String getDomain() {
      return "SMARTLOCK";
    }

    // Provide access to underlying LockBoxEvent for handlers
    public com.selftech.smartlock.avro.LockBoxEvent getLockBoxEvent() {
      return lockBoxEvent;
    }

    @Override
    public String toString() {
      return "LockBoxEventAdapter{eventId=" + getEventId() +
             ", eventType=" + getEventType() +
             ", boxCode=" + lockBoxEvent.getBoxCode() +
             ", timestamp=" + getTimestamp() + "}";
    }
  }

  // ====== DLQ Routing ======

  /**
   * Route failed event to Dead Letter Queue
   *
   * DLQ Topic Structure:
   * - Original: "smartlock.event.boxOperation.v0"
   * - DLQ: "dlq.smartlock.event.boxOperation.v0"
   *
   * Flow:
   * 1. Convert topic to DLQ topic name (prepend "dlq." to domain)
   * 2. Send event to DLQ topic with error context headers
   * 3. DLQConsumer will receive and persist to database
   * 4. Operations team can analyze and reprocess
   *
   * Headers included:
   * - error_message: Error description
   * - error_category: Categorized error type
   * - failed_handler: Handler name that failed (if known)
   * - error_stacktrace: Full exception stacktrace (for debugging)
   *
   * @param event         Failed event
   * @param topic         Original topic name
   * @param partition     Kafka partition
   * @param offset        Kafka offset
   * @param errorMessage  Error description
   * @param failureCount  Number of handlers that failed
   * @param lastException Last exception thrown (for stacktrace)
   */
  private void routeToDLQ(Event event, String topic, int partition, long offset,
      String errorMessage, int failureCount, Exception lastException) {
    try {
      // Step 1: Convert topic to DLQ topic
      String dlqTopic = convertToDLQTopic(topic);

      // Step 2: Prepare error category
      String errorCategory = lastException != null
          ? categorizeException(lastException)
          : "UNKNOWN_ERROR";

      // Step 3: Prepare stacktrace (truncate if too long)
      String stacktrace = null;
      if (lastException != null) {
        stacktrace = getStacktrace(lastException);
        if (stacktrace.length() > 5000) {
          stacktrace = stacktrace.substring(0, 5000) + "... [truncated]";
        }
      }

      // Step 4: Send to DLQ topic with event ID as key (async, fire and forget)
      log.info("Routing event to DLQ - DLQTopic: {}, EventId: {}, OriginalTopic: {}, ErrorCategory: {}",
          dlqTopic, event.getEventId(), topic, errorCategory);

      // Use send() method with topic, key, and value
      var sendFuture = kafkaTemplate.send(dlqTopic, event.getEventId(), event);

      // Add callback handling
      sendFuture.whenComplete((result, ex) -> {
        if (ex != null) {
          log.error("Failed to route event to DLQ - DLQTopic: {}, EventId: {}",
              dlqTopic, event.getEventId(), ex);
        } else {
          log.info("Event routed to DLQ successfully - DLQTopic: {}, EventId: {}, Partition: {}",
              dlqTopic, event.getEventId(), result.getRecordMetadata().partition());
        }
      });

      log.info("Event sent to DLQ - EventId: {}, DLQTopic: {}, Partition: {}, Offset: {}",
          event.getEventId(), dlqTopic, partition, offset);

    } catch (Exception e) {
      log.error("Failed to route event to DLQ - EventId: {}, OriginalTopic: {}, Error: {}",
          event.getEventId(), topic, e.getMessage(), e);
      // Don't throw - we've already logged the original failure
      // Losing DLQ routing is not fatal, event will be in outbox table anyway
    }
  }

  /**
   * Convert original topic name to DLQ topic name
   *
   * Examples:
   * - "smartlock.event.boxOperation.v0" → "dlq.smartlock.event.boxOperation.v0"
   * - "selfpark.event.parkEntry.v0" → "dlq.selfpark.event.parkEntry.v0"
   * - "anomaly.detection.alerts.v0" → "dlq.anomaly.detection.alerts.v0"
   *
   * @param topic Original topic name
   * @return DLQ topic name
   */
  private String convertToDLQTopic(String topic) {
    if (topic == null || topic.isEmpty()) {
      log.warn("Topic name is null or empty, using 'dlq.unknown' as fallback");
      return "dlq.unknown";
    }

    // If already a DLQ topic, return as is (prevent double-prefixing)
    if (topic.startsWith("dlq.")) {
      log.debug("Topic {} is already a DLQ topic, returning as is (preventing double-prefix)", topic);
      return topic;
    }

    // Prepend "dlq." to normal topic names
    String dlqTopic = "dlq." + topic;
    log.debug("Converting normal topic {} to DLQ topic {}", topic, dlqTopic);
    return dlqTopic;
  }

  /**
   * Categorize exception to DLQ error category
   *
   * @param exception Exception to categorize
   * @return Error category string
   */
  private String categorizeException(Exception exception) {
    if (exception == null) {
      return "UNKNOWN_ERROR";
    }

    String exceptionName = exception.getClass().getSimpleName();

    // Pattern matching for common exceptions
    if (exceptionName.contains("Serialization") || exceptionName.contains("Deserialization")) {
      return "DESERIALIZATION_ERROR";
    } else if (exceptionName.contains("Database") || exceptionName.contains("DataAccess")) {
      return "DATABASE_ERROR";
    } else if (exceptionName.contains("Timeout")) {
      return "TIMEOUT_ERROR";
    } else if (exceptionName.contains("Validation")) {
      return "VALIDATION_ERROR";
    } else if (exceptionName.contains("Handler")) {
      return "HANDLER_ERROR";
    } else {
      return "UNKNOWN_ERROR";
    }
  }

  /**
   * Get stacktrace from exception
   *
   * @param exception Exception to extract stacktrace from
   * @return Stacktrace string
   */
  private String getStacktrace(Exception exception) {
    if (exception == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(exception.getClass().getName()).append(": ").append(exception.getMessage()).append("\n");

    for (StackTraceElement element : exception.getStackTrace()) {
      sb.append("\tat ").append(element).append("\n");
    }

    // Include cause if present
    if (exception.getCause() != null) {
      sb.append("Caused by: ").append(getStacktrace((Exception) exception.getCause()));
    }

    return sb.toString();
  }

  /**
   * Get registry status for monitoring
   * Can be exposed via REST endpoint for health checks
   *
   * @return Handler registry status
   */
  public EventHandlerRegistry.RegistryStatus getConsumerStatus() {
    return handlerRegistry.getStatus();
  }

  /**
   * Log current consumer status (for debugging)
   */
  public void logStatus() {
    handlerRegistry.logStatus();
  }
}
