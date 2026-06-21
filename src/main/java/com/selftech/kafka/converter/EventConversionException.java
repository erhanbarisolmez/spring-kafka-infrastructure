package com.selftech.kafka.converter;

/**
 * Event Conversion Exception - FAZE 4.5
 *
 * Thrown when JSON to Avro conversion fails due to:
 * - Invalid JSON format
 * - Missing required fields
 * - Type mismatches
 * - Validation failures
 * - Timestamp format issues
 * - Field normalization errors
 *
 * Used by EventConverter implementations to communicate conversion failures.
 */
public class EventConversionException extends Exception {

    private final String eventType;
    private final String jsonInput;
    private final String failureReason;

    /**
     * Create conversion exception with detailed context
     *
     * @param eventType Target event type (e.g., "SensorDataEvent")
     * @param jsonInput Original JSON input (truncated for large inputs)
     * @param failureReason Human-readable reason for failure
     * @param cause Root exception
     */
    public EventConversionException(String eventType, String jsonInput, String failureReason, Throwable cause) {
        super(String.format(
            "Failed to convert JSON to %s: %s. Input: %s",
            eventType, failureReason, truncateJson(jsonInput)
        ), cause);
        this.eventType = eventType;
        this.jsonInput = jsonInput;
        this.failureReason = failureReason;
    }

    /**
     * Create conversion exception without cause
     */
    public EventConversionException(String eventType, String jsonInput, String failureReason) {
        this(eventType, jsonInput, failureReason, null);
    }

    /**
     * Create conversion exception with message only
     */
    public EventConversionException(String message) {
        super(message);
        this.eventType = null;
        this.jsonInput = null;
        this.failureReason = message;
    }

    /**
     * Create conversion exception with message and cause
     */
    public EventConversionException(String message, Throwable cause) {
        super(message, cause);
        this.eventType = null;
        this.jsonInput = null;
        this.failureReason = message;
    }

    /**
     * Get target event type
     */
    public String getEventType() {
        return eventType;
    }

    /**
     * Get original JSON input (may be truncated)
     */
    public String getJsonInput() {
        return jsonInput;
    }

    /**
     * Get human-readable failure reason
     */
    public String getFailureReason() {
        return failureReason;
    }

    /**
     * Truncate JSON input for logging (max 500 chars)
     */
    private static String truncateJson(String json) {
        if (json == null || json.length() <= 500) {
            return json;
        }
        return json.substring(0, 500) + "... [truncated]";
    }
}
