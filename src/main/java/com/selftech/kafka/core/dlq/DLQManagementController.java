package com.selftech.kafka.core.dlq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

/**
 * DLQ Management Controller - FAZE 4
 *
 * REST API endpoints for Dead Letter Queue management.
 * Provides operations team with tools to:
 * 1. Monitor DLQ events (list, filter, search)
 * 2. Analyze failures (view details, error patterns)
 * 3. Manage event status (analyze, fix, ignore, reprocess)
 * 4. Trigger manual reprocessing
 * 5. View health metrics and statistics
 *
 * API Endpoints:
 * - GET /api/dlq/health - DLQ health status
 * - GET /api/dlq/events - List all DLQ events (paginated)
 * - GET /api/dlq/events?status=RECEIVED - Events by status
 * - GET /api/dlq/events/requiring-action - Events needing attention
 * - GET /api/dlq/events/ready-for-reprocessing - Events ready to retry
 * - GET /api/dlq/events/by-topic?topic=smartlock.event.boxOperation.v0 - Events by topic
 * - GET /api/dlq/events/by-error?category=HANDLER_ERROR - Events by error category
 * - GET /api/dlq/{dlqId} - Get event details
 * - GET /api/dlq/{dlqId}/payload - Get event payload
 * - POST /api/dlq/{dlqId}/analyze - Mark as analyzed with notes
 * - POST /api/dlq/{dlqId}/fix - Mark as fixed (ready to reprocess)
 * - POST /api/dlq/{dlqId}/ignore - Mark as ignored
 * - POST /api/dlq/{dlqId}/reprocess - Trigger manual reprocessing
 * - POST /api/dlq/reprocess/batch - Batch reprocessing
 * - POST /api/dlq/archive/batch - Archive events
 * - GET /api/dlq/stats/error-categories - Error distribution
 * - GET /api/dlq/stats/by-topic - Stats by topic
 * - GET /api/dlq/stats/by-domain - Stats by domain
 *
 * Security:
 * - All endpoints require authentication (TODO: @PreAuthorize)
 * - Sensitive data: Full stacktraces, event payloads (restricted to admins)
 * - Audit trail: All modifications logged with username
 *
 * Response Format:
 * - Success: HTTP 200, JSON response
 * - Not found: HTTP 404
 * - Bad request: HTTP 400
 * - Server error: HTTP 500
 *
 * Typical Usage Workflow:
 * 1. GET /api/dlq/health - Check DLQ health
 * 2. GET /api/dlq/events/requiring-action - View events needing action
 * 3. GET /api/dlq/{dlqId} - View event details and error
 * 4. POST /api/dlq/{dlqId}/analyze - Record root cause analysis
 * 5. Fix deployed (code/config changes)
 * 6. POST /api/dlq/{dlqId}/fix - Mark as fixed
 * 7. POST /api/dlq/{dlqId}/reprocess - Test reprocessing
 * 8. If successful: Automatic scheduler will reprocess batch
 * 9. If failed: Repeat steps 3-7 or mark as IGNORED
 *
 * @author FAZE 4 Implementation
 */


@RestController
@RequestMapping("/api/dlq")
@Tag(name = "DLQ Management", description = "Dead Letter Queue Management API")
@Slf4j
public class DLQManagementController {

    @Autowired
    private DLQEventService dlqEventService;

    @Autowired
    private DLQReprocessingService reprocessingService;

    // ====== Health and Status ======

    /**
     * Get DLQ health status
     * Used for monitoring dashboard and alerts
     *
     * @return DLQ health metrics
     */
    @GetMapping("/health")
    public ResponseEntity<DLQHealthStatus> getHealth() {
        DLQHealthStatus status = dlqEventService.getHealthStatus();
        log.info("DLQ health requested - Status: {}", status.getStatusString());
        return ResponseEntity.ok(status);
    }

    /**
     * Get reprocessing statistics
     *
     * @return Reprocessing stats
     */
    @GetMapping("/stats/reprocessing")
    public ResponseEntity<DLQReprocessingService.DLQReprocessingStats> getReprocessingStats() {
        DLQReprocessingService.DLQReprocessingStats stats = reprocessingService.getStats();
        return ResponseEntity.ok(stats);
    }

    // ====== Event Retrieval ======

    /**
     * Get all DLQ events (paginated)
     *
     * @param page Page number (default: 0)
     * @param size Page size (default: 20, max: 100)
     * @param sort Sort order (e.g., "createdAt,desc")
     * @return Page of DLQ events
     */
    @GetMapping("/events")
    public ResponseEntity<Page<DLQEvent>> getEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "createdAt,desc") String sort) {

        if (size > 100) size = 100;  // Limit max page size

        String[] sortParts = sort.split(",");
        Sort.Direction direction = sortParts.length > 1 && "asc".equals(sortParts[1])
            ? Sort.Direction.ASC
            : Sort.Direction.DESC;
        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, sortParts[0]));

        Page<DLQEvent> events = dlqEventService.getEventsByStatus(null, pageable);
        return ResponseEntity.ok(events);
    }

    /**
     * Get events by status
     *
     * @param status Event status (RECEIVED, ANALYZED, FIXED, etc)
     * @param page Page number
     * @param size Page size
     * @return Page of events
     */
    @GetMapping("/events/by-status")
    public ResponseEntity<Page<DLQEvent>> getEventsByStatus(
            @RequestParam String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        try {
            DLQEvent.DLQStatus dlqStatus = DLQEvent.DLQStatus.valueOf(status.toUpperCase());
            Pageable pageable = PageRequest.of(page, Math.min(size, 100));
            Page<DLQEvent> events = dlqEventService.getEventsByStatus(dlqStatus, pageable);
            return ResponseEntity.ok(events);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid status: {}", status);
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get events requiring action (RECEIVED or ANALYZED)
     *
     * @param page Page number
     * @param size Page size
     * @return Page of events
     */
    @GetMapping("/events/requiring-action")
    public ResponseEntity<Page<DLQEvent>> getEventsRequiringAction(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        Pageable pageable = PageRequest.of(page, Math.min(size, 100), Sort.by("createdAt").ascending());
        Page<DLQEvent> events = dlqEventService.getEventsRequiringAction(pageable);
        return ResponseEntity.ok(events);
    }

    /**
     * Get events ready for reprocessing (FIXED or IGNORED)
     *
     * @param page Page number
     * @param size Page size
     * @return Page of events
     */
    @GetMapping("/events/ready-for-reprocessing")
    public ResponseEntity<Page<DLQEvent>> getEventsReadyForReprocessing(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {

        Pageable pageable = PageRequest.of(page, Math.min(size, 100), Sort.by("createdAt").ascending());
        Page<DLQEvent> events = dlqEventService.getEventsReadyForReprocessing(pageable);
        return ResponseEntity.ok(events);
    }

    /**
     * Get events by topic
     *
     * @param topic Topic name to filter by
     * @param page Page number
     * @param size Page size
     * @return Page of events
     */
    @GetMapping("/events/by-topic")
    public ResponseEntity<Page<DLQEvent>> getEventsByTopic(
            @RequestParam String topic,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Pageable pageable = PageRequest.of(page, Math.min(size, 100), Sort.by("createdAt").descending());
        Page<DLQEvent> events = dlqEventService.getEventsByTopic(topic, pageable);
        return ResponseEntity.ok(events);
    }

    /**
     * Get events by error category
     *
     * @param category Error category
     * @param page Page number
     * @param size Page size
     * @return Page of events
     */
    @GetMapping("/events/by-error")
    public ResponseEntity<Page<DLQEvent>> getEventsByErrorCategory(
            @RequestParam String category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        try {
            DLQEvent.ErrorCategory errorCategory = DLQEvent.ErrorCategory.valueOf(category.toUpperCase());
            Pageable pageable = PageRequest.of(page, Math.min(size, 100), Sort.by("createdAt").descending());
            Page<DLQEvent> events = dlqEventService.getEventsByErrorCategory(errorCategory, pageable);
            return ResponseEntity.ok(events);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid error category: {}", category);
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get single DLQ event by ID
     *
     * @param dlqId DLQ event ID
     * @return Event details or 404 if not found
     */
    @GetMapping("/{dlqId}")
    public ResponseEntity<DLQEvent> getEvent(@PathVariable String dlqId) {
        Optional<DLQEvent> event = dlqEventService.getEventById(dlqId);
        return event.map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get event payload as formatted JSON
     *
     * @param dlqId DLQ event ID
     * @return Event payload
     */
    @GetMapping("/{dlqId}/payload")
    public ResponseEntity<Map<String, String>> getEventPayload(@PathVariable String dlqId) {
        Optional<DLQEvent> event = dlqEventService.getEventById(dlqId);
        if (event.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        Map<String, String> response = new HashMap<>();
        response.put("eventId", event.get().getEventId());
        response.put("eventType", event.get().getEventType());
        response.put("payload", event.get().getEventPayload());
        return ResponseEntity.ok(response);
    }

    // ====== Status Transitions ======

    /**
     * Mark event as analyzed
     * Operations team recorded root cause analysis
     *
     * @param dlqId DLQ event ID
     * @param request Request with analysis notes
     * @return Updated event
     */
    @PostMapping("/{dlqId}/analyze")
    public ResponseEntity<?> markAsAnalyzed(
            @PathVariable String dlqId,
            @RequestBody AnalyzeRequest request) {

        try {
            dlqEventService.markAsAnalyzed(dlqId, request.getNotes(), "API_USER");
            Optional<DLQEvent> event = dlqEventService.getEventById(dlqId);
            log.info("DLQ event marked as analyzed - DLQId: {}", dlqId);
            return ResponseEntity.ok(event.orElse(null));
        } catch (DLQEventService.DLQException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Mark event as fixed (ready for reprocessing)
     * System fix has been deployed
     *
     * @param dlqId DLQ event ID
     * @return Updated event
     */
    @PostMapping("/{dlqId}/fix")
    public ResponseEntity<?> markAsFixed(@PathVariable String dlqId) {
        try {
            dlqEventService.markAsFixed(dlqId, "API_USER");
            Optional<DLQEvent> event = dlqEventService.getEventById(dlqId);
            log.info("DLQ event marked as fixed - DLQId: {}", dlqId);
            return ResponseEntity.ok(event.orElse(null));
        } catch (DLQEventService.DLQException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Mark event as ignored
     * Not a real error, deliberately skipped
     *
     * @param dlqId DLQ event ID
     * @param request Request with ignore reason
     * @return Updated event
     */
    @PostMapping("/{dlqId}/ignore")
    public ResponseEntity<?> markAsIgnored(
            @PathVariable String dlqId,
            @RequestBody IgnoreRequest request) {

        try {
            dlqEventService.markAsIgnored(dlqId, request.getReason(), "API_USER");
            Optional<DLQEvent> event = dlqEventService.getEventById(dlqId);
            log.info("DLQ event marked as ignored - DLQId: {}", dlqId);
            return ResponseEntity.ok(event.orElse(null));
        } catch (DLQEventService.DLQException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", e.getMessage()));
        }
    }

    // ====== Manual Reprocessing ======

    /**
     * Trigger manual reprocessing of single event
     * Useful for operators to test fix before scheduling automatic batch
     *
     * @param dlqId DLQ event ID
     * @return Result of reprocessing attempt
     */
    @PostMapping("/{dlqId}/reprocess")
    public ResponseEntity<?> manualReprocess(@PathVariable String dlqId) {
        try {
            boolean success = reprocessingService.manualReprocess(dlqId);
            Optional<DLQEvent> event = dlqEventService.getEventById(dlqId);

            Map<String, Object> response = new HashMap<>();
            response.put("dlqId", dlqId);
            response.put("success", success);
            response.put("event", event.orElse(null));

            log.info("Manual reprocessing triggered - DLQId: {}, Success: {}", dlqId, success);
            return ResponseEntity.ok(response);
        } catch (DLQEventService.DLQException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Batch reprocessing of multiple events
     * Useful for testing multiple fixes at once
     *
     * @param dlqIds List of DLQ event IDs
     * @return Reprocessing results for each event
     */
    @PostMapping("/reprocess/batch")
    public ResponseEntity<?> batchReprocess(@RequestBody List<String> dlqIds) {
        Map<String, Object> results = new HashMap<>();
        int successCount = 0;
        int failureCount = 0;

        for (String dlqId : dlqIds) {
            try {
                boolean success = reprocessingService.manualReprocess(dlqId);
                if (success) {
                    successCount++;
                } else {
                    failureCount++;
                }
                results.put(dlqId, Map.of("success", success));
            } catch (DLQEventService.DLQException e) {
                failureCount++;
                results.put(dlqId, Map.of("success", false, "error", e.getMessage()));
            }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("total", dlqIds.size());
        response.put("success", successCount);
        response.put("failures", failureCount);
        response.put("results", results);

        log.info("Batch reprocessing completed - Total: {}, Success: {}, Failures: {}",
            dlqIds.size(), successCount, failureCount);

        return ResponseEntity.ok(response);
    }

    // ====== Statistics ======

    /**
     * Get error distribution statistics
     *
     * @return Count by error category
     */
    @GetMapping("/stats/error-categories")
    public ResponseEntity<Map<String, Long>> getErrorCategoryStats() {
        Map<String, Long> stats = new HashMap<>();

        for (DLQEvent.ErrorCategory category : DLQEvent.ErrorCategory.values()) {
            long count = dlqEventService.getCountByErrorCategory(category);
            stats.put(category.name(), count);
        }

        return ResponseEntity.ok(stats);
    }

    /**
     * Get statistics by topic
     *
     * @return Count by topic (top 20)
     */
    @GetMapping("/stats/by-topic")
    public ResponseEntity<List<Map<String, Object>>> getTopicStats() {
        // TODO: Implement comprehensive topic statistics
        List<Map<String, Object>> stats = new ArrayList<>();
        return ResponseEntity.ok(stats);
    }

    /**
     * Get statistics by domain (smartlock, selfpark, etc)
     *
     * @return Count by domain
     */
    @GetMapping("/stats/by-domain")
    public ResponseEntity<Map<String, Long>> getDomainStats() {
        Map<String, Long> stats = new HashMap<>();
        // TODO: Query unique domains and count
        return ResponseEntity.ok(stats);
    }

    // ====== Request/Response DTOs ======

    /**
     * Request body for marking event as analyzed
     */
    public static class AnalyzeRequest {
        private String notes;

        public String getNotes() { return notes; }
        public void setNotes(String notes) { this.notes = notes; }
    }

    /**
     * Request body for marking event as ignored
     */
    public static class IgnoreRequest {
        private String reason;

        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }
}
