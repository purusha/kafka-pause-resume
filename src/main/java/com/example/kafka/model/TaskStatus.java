package com.example.kafka.model;

import java.time.Instant;

public record TaskStatus(
        String taskId,
        String status,      // PENDING, RUNNING, COMPLETED, FAILED
        Instant updatedAt,
        String message
) {
    public static TaskStatus pending(String taskId) {
        return new TaskStatus(taskId, "PENDING", Instant.now(), "Task in coda");
    }

    public static TaskStatus running(String taskId) {
        return new TaskStatus(taskId, "RUNNING", Instant.now(), "Elaborazione in corso...");
    }

    public static TaskStatus completed(String taskId) {
        return new TaskStatus(taskId, "COMPLETED", Instant.now(), "Task completato con successo");
    }

    public static TaskStatus failed(String taskId, String reason) {
        return new TaskStatus(taskId, "FAILED", Instant.now(), "Errore: " + reason);
    }
}
