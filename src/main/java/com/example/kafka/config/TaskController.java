package com.example.kafka.config;

import com.example.kafka.consumer.PauseResumeConsumer;
import com.example.kafka.model.TaskStatus;
import com.example.kafka.producer.TaskProducer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskProducer producer;
    private final PauseResumeConsumer consumer;

    public TaskController(TaskProducer producer, PauseResumeConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    /**
     * Invia un nuovo task.
     * durationSeconds simula quanto "dura" l'elaborazione.
     *
     * Esempio: POST /api/tasks?type=REPORT&durationSeconds=15
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> submitTask(
            @RequestParam(defaultValue = "GENERIC") String type,
            @RequestParam(defaultValue = "10") int durationSeconds) {

        String taskId = producer.sendTask(type, durationSeconds);
        return ResponseEntity.accepted().body(Map.of(
                "taskId", taskId,
                "status", "PENDING",
                "message", "Task in coda, usa GET /api/tasks/" + taskId + " per monitorare"
        ));
    }

    /**
     * Controlla lo stato di un task specifico.
     */
    @GetMapping("/{taskId}")
    public ResponseEntity<TaskStatus> getTask(@PathVariable String taskId) {
        TaskStatus status = consumer.getTaskStatus(taskId);
        return ResponseEntity.ok(status);
    }

    /**
     * Lista tutti i task con il loro stato.
     */
    @GetMapping
    public ResponseEntity<Map<String, TaskStatus>> getAllTasks() {
        return ResponseEntity.ok(consumer.getAllTasks());
    }

    /**
     * Mostra le partizioni attualmente in pausa.
     * Utile per osservare il comportamento pause/resume.
     */
    @GetMapping("/debug/paused-partitions")
    public ResponseEntity<Map<TopicPartition, Boolean>> getPausedPartitions() {
        return ResponseEntity.ok(consumer.getPausedPartitions());
    }
}
