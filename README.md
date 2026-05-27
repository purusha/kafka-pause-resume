# kafka-pause-resume — Demo pattern pause()/resume()

Progetto Spring Boot per osservare in pratica il pattern Kafka
`pause()`/`resume()` per task a lunga durata.

---

## Avvio rapido

### 1. Avvia Kafka con Docker

```bash
cd docker
docker-compose up -d
```

- **Kafka** → `localhost:9092`
- **Kafka UI** → http://localhost:8090 (per osservare topic, consumer lag, offset)

### 2. Avvia l'applicazione

```bash
./mvnw spring-boot:run
```

L'app si mette in ascolto su `http://localhost:8080`.

---

## API disponibili

| Metodo | URL | Descrizione |
|--------|-----|-------------|
| `POST` | `/api/tasks?type=REPORT&durationSeconds=20` | Invia un task |
| `GET`  | `/api/tasks/{taskId}` | Stato di un task |
| `GET`  | `/api/tasks` | Lista tutti i task |
| `GET`  | `/api/tasks/debug/paused-partitions` | Partizioni attualmente in pausa |

---

## Scenari di test consigliati

### Scenario 1 — Task singolo, osserva pause/resume

```bash
# Invia un task da 15 secondi
curl -X POST "http://localhost:8080/api/tasks?type=REPORT&durationSeconds=15"
# → risponde con { "taskId": "abc123", ... }

# Mentre elabora, verifica che la partizione sia in pausa
curl http://localhost:8080/api/tasks/debug/paused-partitions
# → { "{tasks.input-0}": true }

# Monitora lo stato
curl http://localhost:8080/api/tasks/abc123
# → RUNNING ... poi COMPLETED dopo 15s
```

Osserva i log: vedrai il Thread A fare poll() vuoti mentre Thread B lavora.

---

### Scenario 2 — Task multipli in coda

```bash
# Invia 3 task in rapida successione
curl -X POST "http://localhost:8080/api/tasks?type=A&durationSeconds=10"
curl -X POST "http://localhost:8080/api/tasks?type=B&durationSeconds=10"
curl -X POST "http://localhost:8080/api/tasks?type=C&durationSeconds=10"

# Lista tutti
curl http://localhost:8080/api/tasks
```

Osserva che vengono elaborati **uno alla volta** per partizione
(max.poll.records=1 + pause): il secondo task inizia solo dopo
che il primo ha committato e fatto resume.

---

### Scenario 3 — Verifica heartbeat (il punto chiave)

```bash
# Invia un task lungo 30 secondi
curl -X POST "http://localhost:8080/api/tasks?type=LONG&durationSeconds=30"
```

Apri Kafka UI → Consumer Groups → `task-workers`:
- Lo stato del consumer deve restare **Stable** (non Dead)
- Il lag aumenta mentre la partizione è in pausa, poi si azzera al resume

Senza il pattern pause/resume (con `max.poll.interval.ms` basso),
vedresti il consumer diventare Dead e un rebalance.

---

### Scenario 4 — Simula crash (idempotenza)

1. Avvia un task lungo (60s)
2. Stoppa l'app con Ctrl+C mentre è RUNNING
3. Riavvia l'app
4. Il messaggio verrà riprocessato (offset non committato)
5. L'app lo rileva e lo rielabora

In produzione aggiungeresti un controllo sul DB:
```java
if (taskRegistry.get(cmd.taskId()).status().equals("COMPLETED")) {
    log.info("Task già completato, skip idempotente");
    return;
}
```

---

## Cosa osservare nei log

```
# Thread A riceve il messaggio e pausa la partizione
▶ Ricevuto record — partition=0 offset=0 key=abc123
⏸  Partizione tasks.input-0 messa in pausa per task abc123

# Thread B elabora (i poll() del Thread A tornano vuoti)
⚙️  [Thread B] Inizio elaborazione task abc123 (durata simulata: 15s)
   [abc123] Elaborazione: 5/15 secondi
   [abc123] Elaborazione: 10/15 secondi
   [abc123] Elaborazione: 15/15 secondi
✅ [Thread B] Task abc123 completato

# Thread B segnala il commit pendente
📬 [Thread B] Commit pendente segnalato per partition=tasks.input-0 offset=0

# Thread A (al prossimo poll loop) legge il pending e commita + resume
💾 Offset committato — partition=tasks.input-0 offset=1
▶️  Partizione tasks.input-0 ripresa dopo commit offset=0
```

---

## Parametri chiave da sperimentare

Nel file `application.properties`:

```properties
# Prova ad abbassarlo sotto la durata del task → vedrai il rebalance
app.kafka.consumer.max-poll-interval-ms=600000

# Prova con 5 → vedrai più messaggi consumati prima della pausa
app.kafka.consumer.max-poll-records=1
```
# kafka-pause-resume
