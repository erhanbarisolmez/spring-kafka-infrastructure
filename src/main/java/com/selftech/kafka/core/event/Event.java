package com.selftech.kafka.core.event;

import java.io.Serializable;
import java.time.Instant;

/**
 * Base Event Interface - FAZE 2 (UPDATED TO INTERFACE)
 *
 * Tüm domain event'lerin implement etmesi gereken base interface.
 * Distributed tracing, audit trail ve correlation için gerekli metadataları içerir.
 *
 * Kullanım örneği:
 * - Avro Generated Events: SensorDataEvent implements Event (via javaAnnotation)
 * - LockBoxEvent implements Event
 * - ParkEntryEvent implements Event
 * - PaymentEvent implements Event
 *
 * Features:
 * - Event ID: Unique event identifier (UUID)
 * - Correlation ID: Distributed tracing across services
 * - Event Type: Event sınıf adı (LockBoxCreated, ParkEntryCreated, etc)
 * - Source: Hangi servis/domain gönderdi (SMARTLOCK, SELFPARK, ANOMALY)
 * - Timestamp: Event oluşturma zamanı
 * - Event Version: Schema versioning için
 * - Domain: Business domain (optional)
 *
 * Note: This interface is implemented by Avro-generated classes via javaAnnotation.
 * All Avro schemas must have: "javaAnnotation": "com.selftech.kafka.core.event.Event"
 */
public interface Event extends Serializable {

    /**
     * Unique event identifier (UUID)
     * Her event'in unique ID'si olmalı
     */
    String getEventId();

    /**
     * Correlation ID for distributed tracing
     * Tüm related operations'ı trace etmek için kullanılır
     * Örnek: REST request başında generate edilir,
     *        tüm downstream işlemlere propagate edilir
     */
    String getCorrelationId();

    /**
     * Event type identifier
     * Event'in ne olduğunu gösterir (sınıf adı)
     * Örnek: LockBoxCreated, PaymentInitiated, ParkEntryCreated
     */
    String getEventType();

    /**
     * Event source service/domain
     * Hangi servis'ten gönderildığini gösterir
     * Örnek: SMARTLOCK, SELFPARK, ANOMALY_DETECTION
     */
    String getSource();

    /**
     * Event timestamp (creation time)
     * Event'in ne zaman oluşturulduğu
     * Instant type (java.time.Instant)
     */
    Instant getTimestamp();

    /**
     * Schema version for event versioning
     * Event schema'sının versiyonu (future-proofing)
     * Default: 1
     * Schema changes → version increment
     */
    default Integer getEventVersion() {
        return 1;
    }

    /**
     * Business domain identifier
     * Optional: Hangi business domain'e ait
     * Örnek: DEVICE_MANAGEMENT, PAYMENT, PARKING
     */
    default String getDomain() {
        return null;
    }
}
