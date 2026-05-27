package com.example.kafka.model;

public record TaskCommand(
        String taskId,
        String type,
        int durationSeconds  // simula quanto "dura" il task
) {}
