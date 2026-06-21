package com.selftech.kafka.converter;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract Event Converter Base Class - FAZE 4.5
 *
 * Provides common functionality for all event converters:
 * - JSON parsing with error handling
 * - Timestamp format normalization (ISO8601 → epoch millis)
 * - Field extraction with type safety
 * - Validation utilities
 * - Logging and error handling
 *
 * Subclasses implement convert() to build specific Avro events.
 *
 * @param <T> Target Avro event type
 */
public abstract class AbstractEventConverter<T extends SpecificRecord> implements EventConverter<T> {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractEventConverter.class);
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    // Timestamp format support
    private static final DateTimeFormatter ISO8601_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    /**
     * Parse JSON string to JsonNode
     *
     * @param jsonInput JSON string
     * @return Parsed JsonNode
     * @throws EventConversionException if JSON is invalid
     */
    protected JsonNode parseJson(String jsonInput) throws EventConversionException {
        try {
            if (jsonInput == null || jsonInput.trim().isEmpty()) {
                throw new EventConversionException(
                    getTargetEventClass().getSimpleName(),
                    jsonInput,
                    "JSON input is null or empty"
                );
            }

            JsonNode node = objectMapper.readTree(jsonInput);
            logger.debug("Successfully parsed JSON for {}", getConverterName());
            return node;

        } catch (Exception e) {
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                jsonInput,
                "Invalid JSON format: " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Convert timestamp from various formats to epoch milliseconds
     *
     * Supported formats:
     * 1. ISO8601 string: "2025-11-03T14:21:31.759Z"
     * 2. Epoch seconds: 1699010491
     * 3. Epoch milliseconds: 1699010491759
     *
     * @param timestampValue Raw timestamp value from JSON
     * @return Epoch milliseconds (long)
     * @throws EventConversionException if format is invalid
     */
    protected long normalizeTimestamp(Object timestampValue) throws EventConversionException {
        try {
            if (timestampValue == null) {
                // Use current time as fallback
                logger.warn("Timestamp is null, using current time");
                return System.currentTimeMillis();
            }

            // Case 1: String (ISO8601 format)
            if (timestampValue instanceof String) {
                String timestampStr = ((String) timestampValue).trim();

                // Try ISO8601 format first
                try {
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestampStr, ISO8601_FORMATTER);
                    long epochMillis = zonedDateTime.toInstant().toEpochMilli();
                    logger.debug("Converted ISO8601 timestamp '{}' to epoch millis: {}", timestampStr, epochMillis);
                    return epochMillis;
                } catch (DateTimeParseException e) {
                    // Fallback: try parsing as numeric string
                    try {
                        long epochValue = Long.parseLong(timestampStr);

                        // Distinguish between epoch seconds and epoch millis
                        // Epoch seconds are typically smaller (< 1e10)
                        // Epoch millis are typically larger (> 1e12)
                        if (epochValue < 1_000_000_000_000L) {
                            // Likely epoch seconds, convert to millis
                            long epochMillis = epochValue * 1000;
                            logger.debug("Converted epoch seconds '{}' to millis: {}", epochValue, epochMillis);
                            return epochMillis;
                        } else {
                            // Likely epoch millis already
                            logger.debug("Using epoch millis value: {}", epochValue);
                            return epochValue;
                        }

                    } catch (NumberFormatException nfe) {
                        throw new EventConversionException(
                            getTargetEventClass().getSimpleName(),
                            null,
                            "Cannot parse timestamp string: " + timestampStr
                        );
                    }
                }
            }

            // Case 2: Numeric (long or int)
            if (timestampValue instanceof Number) {
                long epochValue = ((Number) timestampValue).longValue();

                // Distinguish between epoch seconds and epoch millis
                if (epochValue < 1_000_000_000_000L) {
                    // Likely epoch seconds, convert to millis
                    long epochMillis = epochValue * 1000;
                    logger.debug("Converted epoch seconds {} to millis: {}", epochValue, epochMillis);
                    return epochMillis;
                } else {
                    // Likely epoch millis already
                    logger.debug("Using epoch millis value: {}", epochValue);
                    return epochValue;
                }
            }

            // Case 3: Instant object
            if (timestampValue instanceof Instant) {
                long epochMillis = ((Instant) timestampValue).toEpochMilli();
                logger.debug("Converted Instant to epoch millis: {}", epochMillis);
                return epochMillis;
            }

            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                null,
                "Unsupported timestamp type: " + timestampValue.getClass().getSimpleName()
            );

        } catch (EventConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                null,
                "Timestamp conversion error: " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Extract string field from JSON node
     *
     * @param node Parent JSON node
     * @param fieldName Field name
     * @param required If true, throws exception if field is missing
     * @param defaultValue Default value if field is missing or null
     * @return Field value or default
     * @throws EventConversionException if required field is missing
     */
    protected String extractString(JsonNode node, String fieldName, boolean required, String defaultValue)
            throws EventConversionException {
        if (node == null || !node.has(fieldName)) {
            if (required) {
                throw new EventConversionException(
                    getTargetEventClass().getSimpleName(),
                    null,
                    "Required field missing: " + fieldName
                );
            }
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);
        if (field.isNull()) {
            return defaultValue;
        }

        return field.asText();
    }

    /**
     * Extract double field from JSON node
     */
    protected Double extractDouble(JsonNode node, String fieldName, boolean required, Double defaultValue)
            throws EventConversionException {
        if (node == null || !node.has(fieldName)) {
            if (required) {
                throw new EventConversionException(
                    getTargetEventClass().getSimpleName(),
                    null,
                    "Required field missing: " + fieldName
                );
            }
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);
        if (field.isNull()) {
            return defaultValue;
        }

        try {
            return field.asDouble();
        } catch (Exception e) {
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                null,
                "Cannot parse double for field '" + fieldName + "': " + field.asText(),
                e
            );
        }
    }

    /**
     * Extract boolean field from JSON node
     */
    protected Boolean extractBoolean(JsonNode node, String fieldName, boolean required, Boolean defaultValue)
            throws EventConversionException {
        if (node == null || !node.has(fieldName)) {
            if (required) {
                throw new EventConversionException(
                    getTargetEventClass().getSimpleName(),
                    null,
                    "Required field missing: " + fieldName
                );
            }
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);
        if (field.isNull()) {
            return defaultValue;
        }

        return field.asBoolean();
    }

    /**
     * Extract long field from JSON node
     */
    protected Long extractLong(JsonNode node, String fieldName, boolean required, Long defaultValue)
            throws EventConversionException {
        if (node == null || !node.has(fieldName)) {
            if (required) {
                throw new EventConversionException(
                    getTargetEventClass().getSimpleName(),
                    null,
                    "Required field missing: " + fieldName
                );
            }
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);
        if (field.isNull()) {
            return defaultValue;
        }

        try {
            return field.asLong();
        } catch (Exception e) {
            throw new EventConversionException(
                getTargetEventClass().getSimpleName(),
                null,
                "Cannot parse long for field '" + fieldName + "': " + field.asText(),
                e
            );
        }
    }

    /**
     * Validate required fields are present in JSON
     *
     * @param node JSON node to validate
     * @param requiredFields Field names that must be present
     * @throws EventConversionException if any required field is missing
     */
    protected void validateRequiredFields(JsonNode node, String... requiredFields) throws EventConversionException {
        for (String fieldName : requiredFields) {
            if (node == null || !node.has(fieldName) || node.get(fieldName).isNull()) {
                throw new EventConversionException(
                    getTargetEventClass().getSimpleName(),
                    null,
                    "Required field missing or null: " + fieldName
                );
            }
        }
    }

    /**
     * Generate unique event ID
     *
     * Can be overridden by subclasses if needed
     *
     * @return UUID-based event ID
     */
    protected String generateEventId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Check if converter can handle given topic based on patterns
     *
     * Supports wildcard patterns:
     * - "smartlock.mqtt.sensorData.v0" (exact match)
     * - "smartlock.mqtt.*" (prefix match)
     * - "*" (match all)
     *
     * @param topic Topic name to check
     * @return true if topic matches any pattern
     */
    @Override
    public boolean canHandle(String topic) {
        if (topic == null || topic.isEmpty()) {
            return false;
        }

        String[] patterns = getTopicPatterns();
        for (String pattern : patterns) {
            if (topicMatches(topic, pattern)) {
                logger.debug("Topic '{}' matches pattern '{}' for {}", topic, pattern, getConverterName());
                return true;
            }
        }

        logger.debug("Topic '{}' does not match any pattern for {}: {}",
            topic, getConverterName(), Arrays.toString(patterns));
        return false;
    }

    /**
     * Check if topic matches pattern
     *
     * Pattern matching rules:
     * - "*" matches any topic
     * - "smartlock.*" matches "smartlock.foo" but not "smartlock.foo.bar"
     * - "smartlock.*.v0" matches "smartlock.foo.v0", "smartlock.bar.v0"
     *
     * @param topic Topic name
     * @param pattern Pattern to match
     * @return true if topic matches pattern
     */
    protected boolean topicMatches(String topic, String pattern) {
        if (pattern.equals("*")) {
            return true;
        }

        // Simple wildcard matching - convert glob pattern to regex
        String regexPattern = pattern
            .replace(".", "\\.")  // Escape dots
            .replace("*", "[^.]*");  // * matches anything except dots (for topic segments)

        return topic.matches(regexPattern);
    }

    /**
     * Log conversion start
     */
    protected void logConversionStart(String jsonInput) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting {} conversion - Input: {}", getConverterName(),
                truncateForLogging(jsonInput, 500));
        }
    }

    /**
     * Log conversion success
     */
    protected void logConversionSuccess(String eventId) {
        logger.debug("{} conversion successful - EventId: {}", getConverterName(), eventId);
    }

    /**
     * Log conversion failure
     */
    protected void logConversionFailure(String reason, Exception e) {
        logger.error("{} conversion failed - Reason: {}", getConverterName(), reason, e);
    }

    /**
     * Truncate string for logging
     */
    protected String truncateForLogging(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "... [truncated]";
    }

    @Override
    public String toString() {
        return String.format("%s(targetClass=%s, patterns=%s)",
            getConverterName(),
            getTargetEventClass().getSimpleName(),
            Arrays.toString(getTopicPatterns())
        );
    }
}
