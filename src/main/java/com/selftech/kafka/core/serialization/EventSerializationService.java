package com.selftech.kafka.core.serialization;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Service;

/**
 * Event Serialization Service - FAZE 4.5 AVRO STANDARDIZATION
 *
 * Avro binary format ile event'leri serialize/deserialize eder.
 *
 * Flow:
 * 1. Domain Service Event oluşturur (Avro SpecificRecord)
 * 2. EventSerializationService Avro'ya binary olarak serialize eder
 * 3. Binary → Base64 encode (database'e koymak için)
 * 4. Base64 string → JSONB column'a kaydedilir
 * 5. OutboxPoller/DLQReprocessingService'i tarafından:
 *    - Base64 decode
 *    - Avro binary → object deserialize
 *    - Kafka'ya gönder
 *
 * Benefits:
 * - Binary format: Compact, fast, efficient
 * - Schema enforced: Type safety, compatibility
 * - Base64 storage: Human readable in database (can be decoded)
 * - No JSON overhead: Better performance than JSON serialization
 * - Consistent format: All events use same serialization
 *
 * Example Usage:
 *
 * // Serialize Avro object to Base64 (for database storage)
 * ParkExitEvent event = ParkExitEvent.newBuilder()
 *     .setEventId("123")
 *     .setUserId(456)
 *     .build();
 *
 * String base64Payload = eventSerializationService.serializeAvroToBase64(event);
 * // Store base64Payload in database
 *
 * // Deserialize Base64 back to Avro object
 * ParkExitEvent deserialized = eventSerializationService.deserializeBase64ToAvro(
 *     base64Payload,
 *     ParkExitEvent.class
 * );
 *
 * @author FAZE 4.5 Implementation
 */
@Service
@Slf4j
public class EventSerializationService {

    private final ObjectMapper prettyMapper;

    public EventSerializationService() {
        this.prettyMapper = new ObjectMapper();
        this.prettyMapper.registerModule(new JavaTimeModule());
        this.prettyMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.prettyMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Serialize Avro SpecificRecord to JSON string using Avro's native JSON encoder
     * Human-readable format for database storage
     *
     * Uses Avro's JsonEncoder instead of Jackson ObjectMapper because:
     * - Avro generated classes have special structure (Schema, getters without setters)
     * - Jackson cannot properly serialize Avro's internal fields
     * - Avro's JsonEncoder produces schema-compliant JSON
     *
     * @param event Avro SpecificRecord
     * @return JSON string (Avro format)
     */
    public String serializeAvroToJson(SpecificRecord event) throws EventSerializationException {
        if (event == null) {
            throw new EventSerializationException("Event cannot be null");
        }

        try {
            log.debug("Serializing Avro event to JSON - EventType: {}", event.getClass().getSimpleName());

            // Use Avro's native JSON encoder for proper serialization
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // Create JSON encoder with pretty print (last param = true)
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(event.getSchema(), outputStream, true);

            // Create Avro DatumWriter
            DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(event.getSchema());

            // Write event to JSON
            writer.write(event, jsonEncoder);
            jsonEncoder.flush();

            String json = outputStream.toString(StandardCharsets.UTF_8);

            log.debug("Event serialized to JSON successfully - EventType: {}, Size: {} chars",
                event.getClass().getSimpleName(), json.length());
            return json;

        } catch (Exception e) {
            log.error("Failed to serialize Avro event to JSON - EventType: {}", event.getClass().getSimpleName(), e);
            throw new EventSerializationException("Failed to serialize to JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Serialize Avro SpecificRecord to Base64 string
     *
     * Flow:
     * 1. Avro object → binary (Avro DatumWriter)
     * 2. Binary → ByteArrayOutputStream
     * 3. ByteArray → Base64 encode
     * 4. Return Base64 string
     *
     * @param event Avro SpecificRecord (implements SpecificRecord interface)
     * @return Base64 encoded string (safe for database JSONB storage)
     * @throws EventSerializationException If serialization fails
     *
     * Example:
     * ParkExitEvent event = ParkExitEvent.newBuilder()...build();
     * String base64 = serializeAvroToBase64(event);  // "eyJwYXJrRXhpdElkIjoyNDN..."
     */
    public String serializeAvroToBase64(SpecificRecord event) throws EventSerializationException {
        if (event == null) {
            throw new EventSerializationException("Event cannot be null");
        }

        try {
            log.debug("Serializing Avro event to Base64 - EventType: {}", event.getClass().getSimpleName());

            // Step 1: Create Avro DatumWriter (writes Java object to Avro binary)
            DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(
                    ((SpecificRecord) event).getSchema()
            );

            // Step 2: Create ByteArrayOutputStream to hold binary data
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            // Step 3: Create Avro Encoder (converts Java objects to binary)
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);

            // Step 4: Write the event object to binary format
            writer.write(event, encoder);
            encoder.flush();

            // Step 5: Get binary bytes and encode to Base64
            byte[] avroBytes = output.toByteArray();
            String base64 = Base64.getEncoder().encodeToString(avroBytes);

            log.debug("Event serialized successfully - EventType: {}, Base64Size: {} bytes",
                    event.getClass().getSimpleName(), base64.length());

            return base64;

        } catch (Exception e) {
            String eventType = event != null ? event.getClass().getSimpleName() : "unknown";
            log.error("Failed to serialize Avro event to Base64 - EventType: {}", eventType, e);
            throw new EventSerializationException(
                    "Failed to serialize Avro event to Base64: " + e.getMessage(), e
            );
        }
    }

    /**
     * Deserialize Base64 string back to Avro SpecificRecord
     *
     * Flow:
     * 1. Base64 string → decode to binary
     * 2. Binary → Avro object (Avro DatumReader)
     * 3. Return typed object
     *
     * @param base64Payload Base64 encoded Avro binary (from database)
     * @param eventClass Target Avro class (e.g., ParkExitEvent.class)
     * @param <T> Generic type (must implement SpecificRecord)
     * @return Deserialized Avro object
     * @throws EventSerializationException If deserialization fails
     *
     * Example:
     * String base64 = "eyJwYXJrRXhpdElkIjoyNDN...";
     * ParkExitEvent event = deserializeBase64ToAvro(base64, ParkExitEvent.class);
     */
    @SuppressWarnings("unchecked")
    public <T extends SpecificRecord> T deserializeBase64ToAvro(String base64Payload, Class<T> eventClass)
            throws EventSerializationException {
        if (base64Payload == null || base64Payload.isEmpty()) {
            throw new EventSerializationException("Base64 payload cannot be null or empty");
        }

        if (eventClass == null) {
            throw new EventSerializationException("Event class cannot be null");
        }

        try {
            log.debug("Deserializing Base64 to Avro object - TargetClass: {}", eventClass.getSimpleName());

            // Step 1: Decode Base64 to binary
            byte[] avroBytes = Base64.getDecoder().decode(base64Payload);

            // Step 2: Get Avro schema from class (using reflection to access SCHEMA$ field)
            org.apache.avro.Schema schema = null;
            try {
                // All Avro generated classes have a static SCHEMA$ field
                java.lang.reflect.Field schemaField = eventClass.getDeclaredField("SCHEMA$");
                schemaField.setAccessible(true);
                schema = (org.apache.avro.Schema) schemaField.get(null);
            } catch (NoSuchFieldException e) {
                // Fallback: create instance and get schema
                T instance = eventClass.getDeclaredConstructor().newInstance();
                schema = instance.getSchema();
            }

            if (schema == null) {
                throw new EventSerializationException("Unable to get Avro schema for class: " + eventClass.getSimpleName());
            }

            // Step 3: Create Avro DatumReader with proper SpecificData MODEL
            // Use SpecificDatumReader constructor that accepts schema and SpecificData
            DatumReader<T> reader = new SpecificDatumReader<>(schema);

            // Step 4: Create Avro Decoder (converts binary to Java objects)
            Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);

            // Step 5: Read the binary data back to typed object
            T deserialized = reader.read(null, decoder);

            log.debug("Event deserialized successfully - TargetClass: {}, BinarySize: {} bytes",
                    eventClass.getSimpleName(), avroBytes.length);

            return deserialized;

        } catch (EventSerializationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to deserialize Base64 to Avro - TargetClass: {}, Error: {}",
                    eventClass.getSimpleName(), e.getMessage(), e);
            throw new EventSerializationException(
                    "Failed to deserialize Base64 to Avro: " + e.getMessage(), e
            );
        }
    }

    /**
     * Deserialize JSON string back to Avro SpecificRecord using Avro's native JSON decoder
     * Used by OutboxPoller to read events stored as JSON in database
     *
     * Uses Avro's JsonDecoder instead of Jackson ObjectMapper because:
     * - serializeAvroToJson() uses Avro's JsonEncoder
     * - Format must match: Avro JSON ↔ Avro JSON
     * - Avro's JsonDecoder can read Avro-formatted JSON correctly
     *
     * @param jsonPayload JSON string (from database eventPayload column, Avro format)
     * @param eventClass Target Avro class (e.g., ParkExitEvent.class)
     * @param <T> Generic type (must implement SpecificRecord)
     * @return Deserialized Avro object
     * @throws EventSerializationException If deserialization fails
     *
     * Example:
     * String json = "{\"eventId\":\"123\",\"userId\":456,...}";
     * ParkExitEvent event = deserializeJsonToAvro(json, ParkExitEvent.class);
     */
    @SuppressWarnings("unchecked")
    public <T extends SpecificRecord> T deserializeJsonToAvro(String jsonPayload, Class<T> eventClass)
            throws EventSerializationException {
        if (jsonPayload == null || jsonPayload.isEmpty()) {
            throw new EventSerializationException("JSON payload cannot be null or empty");
        }

        if (eventClass == null) {
            throw new EventSerializationException("Event class cannot be null");
        }

        try {
            log.debug("Deserializing JSON to Avro object - TargetClass: {}", eventClass.getSimpleName());

            // Step 1: Get Avro schema from class
            org.apache.avro.Schema schema = null;
            try {
                java.lang.reflect.Field schemaField = eventClass.getDeclaredField("SCHEMA$");
                schemaField.setAccessible(true);
                schema = (org.apache.avro.Schema) schemaField.get(null);
            } catch (NoSuchFieldException e) {
                T instance = eventClass.getDeclaredConstructor().newInstance();
                schema = instance.getSchema();
            }

            if (schema == null) {
                throw new EventSerializationException("Unable to get Avro schema for class: " + eventClass.getSimpleName());
            }

            // Step 2: Create Avro JSON decoder
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, jsonPayload);

            // Step 3: Create Avro DatumReader
            DatumReader<T> reader = new SpecificDatumReader<>(schema);

            // Step 4: Read JSON to typed Avro object
            T deserialized = reader.read(null, jsonDecoder);

            log.debug("Event deserialized from JSON successfully - TargetClass: {}, PayloadSize: {} chars",
                    eventClass.getSimpleName(), jsonPayload.length());

            return deserialized;

        } catch (EventSerializationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to deserialize JSON to Avro - TargetClass: {}, Error: {}",
                    eventClass.getSimpleName(), e.getMessage(), e);
            throw new EventSerializationException(
                    "Failed to deserialize JSON to Avro: " + e.getMessage(), e
            );
        }
    }

    /**
     * Get the event class name from Base64 payload
     * Useful for polymorphic deserialization
     *
     * Note: We need EventTypeRegistry for this - added in next step
     *
     * @param base64Payload Base64 encoded payload
     * @return Event class name (hint: this is a placeholder for future enhancement)
     */
    public String getEventClassName(String base64Payload) {
        // TODO: Parse Base64 → Avro binary → extract class name from schema
        // For now, use EventTypeRegistry with event metadata
        return null;
    }

    /**
     * Custom exception for serialization errors
     */
    public static class EventSerializationException extends Exception {
        public EventSerializationException(String message) {
            super(message);
        }

        public EventSerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
