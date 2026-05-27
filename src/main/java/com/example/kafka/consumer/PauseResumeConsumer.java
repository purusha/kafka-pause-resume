package com.example.kafka.consumer;

import com.example.kafka.model.TaskCommand;
import com.example.kafka.model.TaskStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class PauseResumeConsumer {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeConsumer.class);

    // Kafka connection
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.consumer.max-poll-interval-ms}")
    private int maxPollInterval;

    @Value("${app.kafka.consumer.max-poll-records}")
    private int maxPollRecords;

    @Value("${app.kafka.consumer.session-timeout-ms}")
    private int sessionTimeout;

    @Value("${app.kafka.consumer.heartbeat-interval-ms}")
    private int heartbeatInterval;

    @Value("${app.kafka.topic.input}")
    private String inputTopic;

    private final ObjectMapper objectMapper;

    // Stato dei task — in produzione sarebbe un DB
    private final ConcurrentHashMap<String, TaskStatus> taskRegistry = new ConcurrentHashMap<>();

    // Canale di comunicazione Thread B → Thread A
    private final ConcurrentHashMap<TopicPartition, Long> pendingCommits = new ConcurrentHashMap<>();

    // Partizioni attualmente in pausa
    private final ConcurrentHashMap<TopicPartition, Boolean> pausedPartitions = new ConcurrentHashMap<>();

    // Thread pool per i task pesanti
    private final ExecutorService taskExecutor = Executors.newFixedThreadPool(4);

    private final AtomicBoolean running = new AtomicBoolean(false);

    private KafkaConsumer<String, String> consumer;
    private Thread pollThread;

    public PauseResumeConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // ------------------------------------------------------------------
    // Avvio — il KafkaConsumer viene costruito qui, dopo che Spring
    // ha iniettato tutti i @Value
    // ------------------------------------------------------------------
    @PostConstruct
    public void start() {
        running.set(true);
        consumer = new KafkaConsumer<>(buildConsumerProperties());
        consumer.subscribe(List.of(inputTopic));

        pollThread = new Thread(this::pollLoop, "kafka-poll-thread");
        pollThread.setDaemon(true);
        pollThread.start();

        log.info("Consumer avviato, in ascolto su topic: {}", inputTopic);
    }

    private Properties buildConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    // ------------------------------------------------------------------
    // Poll loop — Thread A
    // ------------------------------------------------------------------
    private void pollLoop() {
        while (running.get()) {
            try {
                flushPendingCommits();

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record);
                }

            } catch (Exception e) {
                if (running.get()) {
                    log.error("Errore nel poll loop", e);
                }
            }
        }
        consumer.close();
        log.info("Poll loop terminato, consumer chiuso");
    }

    // ------------------------------------------------------------------
    // Gestione record — Thread A
    // ------------------------------------------------------------------
    private void handleRecord(ConsumerRecord<String, String> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());

        log.info("▶ Ricevuto record — partition={} offset={} key={}",
            record.partition(), record.offset(), record.key());

        TaskCommand cmd;
        try {
            cmd = objectMapper.readValue(record.value(), TaskCommand.class);
        } catch (Exception e) {
            log.error("Impossibile deserializzare il messaggio: {}", record.value(), e);
            commitOffset(partition, record.offset());
            return;
        }

        taskRegistry.put(cmd.taskId(), TaskStatus.running(cmd.taskId()));

        consumer.pause(Collections.singleton(partition));
        pausedPartitions.put(partition, true);
        log.info("⏸  Partizione {} messa in pausa per task {}", partition, cmd.taskId());

        final long offsetToCommit = record.offset();
        taskExecutor.submit(() -> processTask(cmd, partition, offsetToCommit));
    }

    // ------------------------------------------------------------------
    // Elaborazione task — Thread B (non tocca mai il KafkaConsumer)
    // ------------------------------------------------------------------
    private void processTask(TaskCommand cmd, TopicPartition partition, long offset) {
        log.info("⚙️  [Thread B] Inizio elaborazione task {} (durata simulata: {}s)",
            cmd.taskId(), cmd.durationSeconds());
        try {
            simulateLongRunningTask(cmd);
            taskRegistry.put(cmd.taskId(), TaskStatus.completed(cmd.taskId()));
            log.info("✅ [Thread B] Task {} completato", cmd.taskId());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            taskRegistry.put(cmd.taskId(), TaskStatus.failed(cmd.taskId(), "Interrotto"));
            log.warn("⚠️  [Thread B] Task {} interrotto", cmd.taskId());

        } catch (Exception e) {
            taskRegistry.put(cmd.taskId(), TaskStatus.failed(cmd.taskId(), e.getMessage()));
            log.error("❌ [Thread B] Task {} fallito", cmd.taskId(), e);

        } finally {
            // Segnala al Thread A di committare e fare resume
            pendingCommits.put(partition, offset);
            log.info("📬 [Thread B] Commit pendente segnalato per partition={} offset={}", partition, offset);
        }
    }

    // ------------------------------------------------------------------
    // Flush commit pendenti — Thread A
    // ------------------------------------------------------------------
    private void flushPendingCommits() {
        if (pendingCommits.isEmpty()) return;

        pendingCommits.forEach((partition, offset) -> {
            commitOffset(partition, offset);
            consumer.resume(Collections.singleton(partition));
            pausedPartitions.remove(partition);
            log.info("▶️  Partizione {} ripresa dopo commit offset={}", partition, offset);
        });
        pendingCommits.clear();
    }

    private void commitOffset(TopicPartition partition, long offset) {
        try {
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(offset + 1)));
            log.info("💾 Offset committato — partition={} offset={}", partition, offset + 1);
        } catch (Exception e) {
            log.error("Errore nel commit offset per partition={}", partition, e);
        }
    }

    // ------------------------------------------------------------------
    // Simulazione task oneroso
    // ------------------------------------------------------------------
    private void simulateLongRunningTask(TaskCommand cmd) throws InterruptedException {
        int total = cmd.durationSeconds();
        log.info("   [{}] Elaborazione: 0/{} secondi", cmd.taskId(), total);
        for (int i = 1; i <= total; i++) {
            Thread.sleep(1_000);
            if (i % 5 == 0 || i == total) {
                log.info("   [{}] Elaborazione: {}/{} secondi", cmd.taskId(), i, total);
            }
        }
    }

    // ------------------------------------------------------------------
    // API per il controller
    // ------------------------------------------------------------------
    public TaskStatus getTaskStatus(String taskId) {
        return taskRegistry.getOrDefault(taskId,
            new TaskStatus(taskId, "NOT_FOUND", null, "Task non trovato"));
    }

    public Map<String, TaskStatus> getAllTasks() {
        return Collections.unmodifiableMap(taskRegistry);
    }

    public Map<TopicPartition, Boolean> getPausedPartitions() {
        return Collections.unmodifiableMap(pausedPartitions);
    }

    // ------------------------------------------------------------------
    // Shutdown
    // ------------------------------------------------------------------
    @PreDestroy
    public void stop() {
        log.info("Shutdown consumer...");

        // 1. Segnala al poll loop di uscire dal while
        running.set(false);

        // 2. Aspetta che il poll thread finisca da solo — consumer.close()
        //    ha bisogno di fare il leave-group con il broker senza essere interrotto.
        if (pollThread != null) {
            try {
                pollThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (pollThread.isAlive()) {
                log.warn("Poll thread non terminato in 10s, forzo interrupt");
                pollThread.interrupt();
            }
        }

        // 3. Shutdown dei task worker
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("Shutdown completato");
    }
}