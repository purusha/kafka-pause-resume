package com.example.kafka.producer;

import com.example.kafka.model.TaskCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class TaskProducer {

    private static final Logger log = LoggerFactory.getLogger(TaskProducer.class);

    @Value("${app.kafka.topic.input}")
    private String inputTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public TaskProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public String sendTask(String type, int durationSeconds) {
        String taskId = UUID.randomUUID().toString().substring(0, 8);
        TaskCommand cmd = new TaskCommand(taskId, type, durationSeconds);

        try {
            String json = objectMapper.writeValueAsString(cmd);
            // Usiamo taskId come key → garantisce ordine per stesso task
            kafkaTemplate.send(inputTopic, taskId, json);
            log.info("📤 Task inviato: id={} type={} duration={}s", taskId, type, durationSeconds);
            return taskId;
        } catch (Exception e) {
            throw new RuntimeException("Errore invio task su Kafka", e);
        }
    }
}
