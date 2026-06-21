package com.selftech.kafka.event.publisher;

/**
 * Base interface for all event publisher services in the system
 *
 * Provides a unified contract for all event publishing operations across domains.
 * All domain-specific event publishers implement this interface to ensure:
 * - Consistent error handling and logging patterns
 * - Reliable event publishing via Outbox Pattern (TIER 1 critical events)
 * - Best-effort async publishing (TIER 2 non-critical events)
 * - Type-safe operations and clear contracts
 *
 * Hybrid Architecture Pattern:
 * - Event Publishers implement this interface (flexible, testable, mockable)
 * - Business Managers use direct class injection (lean, fast, no overhead)
 * - Creates optimal balance for Docker/K8s deployments
 *
 * Implementation Guidelines:
 * ═════════════════════════════════════════════════════════════════
 *
 * TIER 1 - Critical Events (Reliable Publishing via Outbox):
 * ─────────────────────────────────────────────────────────────
 * Method Pattern: publishXxxEventReliable(Entity entity) throws Exception
 *
 * Characteristics:
 * - Persists to outbox table in same @Transactional boundary
 * - OutboxPoller publishes to Kafka asynchronously
 * - Guarantees exactly-once delivery even if Kafka unavailable
 * - Transaction rollback ensures atomicity with business operation
 * - Exception handling: Must propagate to caller for transaction control
 *
 * Examples:
 * - publishParkEntryEventReliable(ParkEntry entry)
 * - publishPaymentInitiatedEventReliable(DeviceOperation operation)
 * - publishBoxCreatedEventReliable(LockBox box)
 *
 * Usage in Managers:
 *   @Transactional
 *   public Entity save(Entity entity) {
 *       Entity saved = repository.save(entity);
 *       try {
 *           publisher.publishXxxEventReliable(saved);  // Same transaction
 *       } catch (Exception e) {
 *           logger.error("Event publishing failed", e);
 *           throw new RuntimeException("Event publishing failed - transaction will rollback", e);
 *       }
 *       return saved;
 *   }
 *
 * ═════════════════════════════════════════════════════════════════
 *
 * TIER 2 - Non-Critical Events (Best-Effort Async Publishing):
 * ──────────────────────────────────────────────────────────────
 * Method Pattern: publishXxxEvent(Entity entity)
 *
 * Characteristics:
 * - Published asynchronously via CentralEventCoordinator.publishEventAsync()
 * - Does NOT persist to outbox table
 * - Uses best-effort semantics (may be lost if Kafka unavailable)
 * - Does NOT block caller (async publishing)
 * - Exception handling: Caught and logged, not propagated
 *
 * Examples:
 * - publishParkEntryEvent(ParkEntry entry)  [notification-only]
 * - publishDeviceStatusChangedEvent(LockDevice device, String previousStatus)
 *
 * Usage in Controllers/Services:
 *   public void notifyExternalSystem(Entity entity) {
 *       try {
 *           publisher.publishXxxEvent(entity);  // Fire-and-forget
 *       } catch (Exception e) {
 *           logger.warn("Non-critical event publishing failed", e);
 *           // Continue normally - business operation unaffected
 *       }
 *   }
 *
 * ═════════════════════════════════════════════════════════════════
 *
 * @author Architecture Team - Phase 4.8
 * @since 2024 - Hybrid Architecture Implementation
 */
public interface IEventPublisherService {

    /**
     * Get the publisher service name for logging and monitoring purposes
     *
     * Used in:
     * - Log messages for easy identification
     * - Metrics and monitoring dashboards
     * - Dead Letter Queue (DLQ) routing
     * - Service health checks
     *
     * @return Service name (e.g., "ParkEventPublisher", "LockBoxEventPublisher", "PaymentEventPublisher")
     */
    String getPublisherName();

    /**
     * Check if this publisher service is ready to accept events
     *
     * Health check that validates:
     * - CentralEventCoordinator is available
     * - Database connections are healthy
     * - Required dependencies are initialized
     *
     * Used in:
     * - Spring Boot @EnableHealthIndicators
     * - Kubernetes readiness probes
     * - Service startup verification
     *
     * @return true if publisher can safely accept events, false if degraded
     */
    boolean isHealthy();
}
