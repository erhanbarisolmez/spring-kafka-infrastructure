package com.selftech.kafka.converter.converters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.selftech.kafka.converter.AbstractEventConverter;
import com.selftech.kafka.converter.EventConversionException;
import com.selftech.smartlock.avro.LockBoxEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * LockBoxEvent Converter - FAZE 4.5
 *
 * Converts JSON box operation data to Avro LockBoxEvent
 *
 * Purpose:
 * Normalize and convert JSON box operation events to Avro format.
 * Handles box status changes, door operations, weight changes, etc.
 *
 * Handles:
 * 1. Required field validation (eventId, boxCode, eventType, status, doorOpen, currentWeight, timestamp)
 * 2. Optional field mapping with null defaults
 * 3. Timestamp format conversion (ISO8601 string → epoch milliseconds)
 * 4. Device code array conversion
 * 5. Metadata map conversion
 *
 * Input JSON Example:
 * ```
 * {
 *   "boxCode": "BOX-001",
 *   "eventType": "BoxOpened",
 *   "status": "EMPTY",
 *   "doorOpen": true,
 *   "currentWeight": 0.0,
 *   "previousWeight": 5.2,
 *   "expectedWeight": 10.0,
 *   "deviceCount": 2,
 *   "deviceCodes": ["LOCK-001", "LOCK-002"],
 *   "userId": "USER-123",
 *   "operationType": "UPDATE",
 *   "temperature": 22.5,
 *   "humidity": 65.0,
 *   "gpsCoordinates": "40.7128,-74.0060",
 *   "batteryLevel": 95.5,
 *   "timestamp": "2025-11-03T14:21:31.759Z",
 *   "correlationId": "corr-123",
 *   "metadata": {
 *     "source": "MQTT",
 *     "version": "1.0"
 *   }
 * }
 * ```
 *
 * Output:
 * LockBoxEvent with all fields properly typed and timestamp as epoch millis
 *
 * Supported Topic Patterns:
 * - "smartlock.event.boxOperation.v0" (exact match)
 * - "smartlock.event.box*" (wildcard)
 * - "smartlock.event.*" (broader pattern)
 *
 * Error Handling:
 * - Missing required fields: throws EventConversionException
 * - Invalid timestamp format: uses current time as fallback
 * - Invalid JSON: throws EventConversionException
 * - Array parsing: gracefully handles missing array elements
 *
 * Timestamp Handling:
 * Automatically detects and converts:
 * - ISO8601 format: "2025-11-03T14:21:31.759Z" → epoch millis
 * - Epoch seconds: 1699010491 → epoch millis
 * - Epoch millis: 1699010491759 → unchanged
 * - If null or missing: uses System.currentTimeMillis()
 *
 * @author FAZE 4.5 Implementation
 */
@Component
@Slf4j
public class LockBoxEventConverter extends AbstractEventConverter<LockBoxEvent> {

    // Topic patterns this converter handles
    private static final String[] TOPIC_PATTERNS = {
        "smartlock.event.boxOperation.v0",
        "smartlock.event.box*",
        "smartlock.event.*"
    };

    /**
     * Convert JSON box operation data to Avro LockBoxEvent
     *
     * @param jsonInput JSON string from MQTT/API
     * @return LockBoxEvent ready for Kafka publishing
     * @throws EventConversionException if conversion fails
     */
    @Override
    public LockBoxEvent convert(String jsonInput) throws EventConversionException {
        logConversionStart(jsonInput);

        try {
            // Step 1: Parse JSON
            JsonNode node = parseJson(jsonInput);

            // Step 2: Validate required fields
            validateRequiredFields(node, "boxCode", "eventType", "status", "doorOpen", "currentWeight", "timestamp");

            // Step 3: Extract required fields
            String eventId = extractString(node, "eventId", false, generateEventId());
            String boxCode = extractString(node, "boxCode", true, null);
            String eventType = extractString(node, "eventType", true, null);
            String status = extractString(node, "status", true, null);
            Boolean doorOpen = extractBoolean(node, "doorOpen", true, null);
            Double currentWeight = extractDouble(node, "currentWeight", true, null);

            // Step 4: Extract optional fields
            String previousStatus = extractString(node, "previousStatus", false, null);
            Double previousWeight = extractDouble(node, "previousWeight", false, null);
            Double expectedWeight = extractDouble(node, "expectedWeight", false, null);
            Long deviceCount = extractLong(node, "deviceCount", false, 0L);
            String userId = extractString(node, "userId", false, null);
            String operationType = extractString(node, "operationType", false, null);
            Double temperature = extractDouble(node, "temperature", false, null);
            Double humidity = extractDouble(node, "humidity", false, null);
            String gpsCoordinates = extractString(node, "gpsCoordinates", false, null);
            Double batteryLevel = extractDouble(node, "batteryLevel", false, null);
            String correlationId = extractString(node, "correlationId", false, null);

            // Step 5: Convert device codes array
            List<CharSequence> deviceCodes = extractDeviceCodesArray(node);

            // Step 6: Convert metadata map
            Map<CharSequence, CharSequence> metadata = extractMetadataMap(node);

            // Step 7: Handle timestamp conversion
            java.time.Instant timestamp;
            if (node.has("timestamp") && !node.get("timestamp").isNull()) {
                timestamp = java.time.Instant.ofEpochMilli(normalizeTimestamp(node.get("timestamp")));
            } else {
                logger.warn("Timestamp field missing, using current time. EventId: {}", eventId);
                timestamp = java.time.Instant.now();
            }

            // Step 8: Build Avro event using generated builder
            LockBoxEvent event = LockBoxEvent.newBuilder()
                .setEventId(eventId)
                .setBoxCode(boxCode)
                .setEventType(eventType)
                .setPreviousStatus(previousStatus)
                .setStatus(status)
                .setDoorOpen(doorOpen)
                .setCurrentWeight(currentWeight)
                .setPreviousWeight(previousWeight)
                .setExpectedWeight(expectedWeight)
                .setDeviceCount(Math.toIntExact(deviceCount != null ? deviceCount : 0L))
                .setDeviceCodes(deviceCodes)
                .setUserId(userId)
                .setOperationType(operationType)
                .setTemperature(temperature)
                .setHumidity(humidity)
                .setGpsCoordinates(gpsCoordinates)
                .setBatteryLevel(batteryLevel)
                .setTimestamp(timestamp)
                .setCorrelationId(correlationId)
                .setSchemaVersion("1.0")
                .setMetadata(metadata)
                .build();

            logConversionSuccess(eventId);
            return event;

        } catch (EventConversionException e) {
            logConversionFailure("Event conversion exception", e);
            throw e;
        } catch (Exception e) {
            String errorMsg = "Unexpected error during conversion: " + e.getMessage();
            logConversionFailure(errorMsg, e);
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                jsonInput,
                errorMsg,
                e
            );
        }
    }

    /**
     * Extract device codes array from JSON
     *
     * Handles:
     * - Array format: ["CODE1", "CODE2"]
     * - Missing array: returns empty list
     * - Null array: returns empty list
     *
     * @param node Parent JSON node
     * @return List of CharSequence device codes or empty list
     */
    private List<CharSequence> extractDeviceCodesArray(JsonNode node) {
        if (node == null || !node.has("deviceCodes")) {
            return null;
        }

        JsonNode codesNode = node.get("deviceCodes");
        if (codesNode.isNull()) {
            return null;
        }

        if (!codesNode.isArray()) {
            logger.warn("deviceCodes field is not an array, skipping");
            return null;
        }

        List<CharSequence> codes = new java.util.ArrayList<>();
        codesNode.forEach(codeNode -> codes.add(codeNode.asText()));
        return codes.isEmpty() ? null : codes;
    }

    /**
     * Extract metadata map from JSON
     *
     * Handles:
     * - Object format: {"key": "value", ...}
     * - Missing object: returns empty map
     * - Null object: returns empty map
     *
     * @param node Parent JSON node
     * @return Map of metadata with CharSequence values or null
     */
    private Map<CharSequence, CharSequence> extractMetadataMap(JsonNode node) {
        if (node == null || !node.has("metadata")) {
            return null;
        }

        JsonNode metadataNode = node.get("metadata");
        if (metadataNode.isNull()) {
            return null;
        }

        if (!metadataNode.isObject()) {
            logger.warn("metadata field is not an object, skipping");
            return null;
        }

        Map<CharSequence, CharSequence> metadata = new HashMap<>();
        metadataNode.fields().forEachRemaining(entry ->
            metadata.put(entry.getKey(), entry.getValue().asText())
        );
        return metadata.isEmpty() ? null : metadata;
    }

    /**
     * Get target Avro event class
     */
    @Override
    public Class<LockBoxEvent> getTargetEventClass() {
        return LockBoxEvent.class;
    }

    /**
     * Get topic patterns this converter handles
     */
    @Override
    public String[] getTopicPatterns() {
        return TOPIC_PATTERNS;
    }

    /**
     * Get converter name for logging
     */
    @Override
    public String getConverterName() {
        return "LockBoxEventConverter";
    }
}
