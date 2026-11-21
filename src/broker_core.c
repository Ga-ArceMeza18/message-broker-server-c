#define _POSIX_C_SOURCE 200809L

#include "broker.h"
#include "protocol.h"
#include "partition_storage.h"
#include "message.h"
#include "partition_log.h"

#include <errno.h>
#include <openmpi/mpi.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define BROKER_POLL_MS 200
#define BROKER_BACKLOG 128
#define BROKER_HEARTBEAT_INTERVAL_MS 500
#define BROKER_HEARTBEAT_TIMEOUT_MS 3000

/* 
    UTILITIES     
*/

// utility: create directories recursively
static int make_directories(const char *path) {
    char *copy = strdup(path);
    if (copy == NULL) {
        return -1;
    }
    for (char *p = copy + 1; *p != '\0'; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(copy, 0755) != 0 && errno != EEXIST) {
                free(copy);
                return -1;
            }
            *p = '/';
        }
    }
    int rc = (mkdir(copy, 0755) != 0 && errno != EEXIST) ? -1 : 0;
    free(copy);
    return rc;
}

// utility: set state of broker with thread safety
broker_state_t broker_get_state(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    broker_state_t state = broker->state;
    pthread_mutex_unlock(&broker->state_lock);
    return state;
}

/* 
    OUTBOX UTILITIES  
*/

// out_ut: push a message to the outbox queue
static int outbox_push(broker_t *broker, int dest, int tag) {

    pthread_mutex_lock(&broker->outbox_lock);
    if (broker->outbox_count == BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }
    broker->outbox[broker->outbox_count].dest = dest;
    broker->outbox[broker->outbox_count].tag = tag;
    broker->outbox_count++;
    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}

// out_ut: send a control message to another broker via MPI
static int send_control(broker_t *broker, int dest, int tag) {
    if (!broker->mpi_active || dest < 0 || dest >= broker->world_size) {
        return -1;
    }
    return outbox_push(broker, dest, tag);
}

static int send_metadata(broker_t *broker, int dest, const metadata_update_t *update) {
    if (!broker->mpi_active || dest < 0 || dest >= broker->world_size || update == NULL) {
        return -1;
    }

    pthread_mutex_lock(&broker->outbox_lock);

    if (broker->outbox_count >= BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }

    mpi_outgoing_t *item = &broker->outbox[broker->outbox_count++];

    item->dest = dest;
    item->tag = BROKER_TAG_METADATA;
    item->metadata = *update;

    pthread_mutex_unlock(&broker->outbox_lock);

    return 0;
}

static int send_metadata_ack(broker_t *broker, int dest, uint64_t version, int status) {
    if (!broker->mpi_active || dest < 0 || dest >= broker->world_size) {
        return -1;
    }

    pthread_mutex_lock(&broker->outbox_lock);

    if (broker->outbox_count >= BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }

    mpi_outgoing_t *item = &broker->outbox[broker->outbox_count++];

    item->dest = dest;
    item->tag = BROKER_TAG_METADATA_ACK;
    item->ack.version = version;
    item->ack.status = status;

    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}


static int send_replica_append(broker_t *broker, int dest, const replica_append_t *append) {
    if (!broker->mpi_active || !append || dest < 0 || dest >= broker->world_size) return -1;

    pthread_mutex_lock(&broker->outbox_lock);

    if (broker->outbox_count >= BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }

    mpi_outgoing_t *item = &broker->outbox[broker->outbox_count++];
    item->dest = dest;
    item->tag = BROKER_TAG_REPLICA_APPEND;
    item->replica_append = *append;

    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}

static int send_replica_ack(broker_t *broker, int dest, const replica_ack_t *ack) {
    if (!broker->mpi_active || !ack || dest < 0 || dest >= broker->world_size) return -1;

    pthread_mutex_lock(&broker->outbox_lock);

    if (broker->outbox_count >= BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }

    mpi_outgoing_t *item = &broker->outbox[broker->outbox_count++];
    item->dest = dest;
    item->tag = BROKER_TAG_REPLICA_ACK;
    item->replica_ack = *ack;

    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}

// out_ut: send a replica high watermark message to another broker via MPI
static int send_replica_hw(broker_t *broker, int dest, const replica_hw_t *hw) {
    if (!broker->mpi_active || !hw || dest < 0 || dest >= broker->world_size) return -1;

    pthread_mutex_lock(&broker->outbox_lock);

    if (broker->outbox_count >= BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }

    mpi_outgoing_t *item = &broker->outbox[broker->outbox_count++];
    item->dest = dest;
    item->tag = BROKER_TAG_REPLICA_HW;
    item->replica_hw = *hw;

    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}

// out_ut: send a replica sync message to another broker via MPI
static int send_replica_sync(broker_t *broker, int dest, const replica_sync_t *sync) {
    if (!broker->mpi_active || !sync || dest < 0 || dest >= broker->world_size) return -1;

    pthread_mutex_lock(&broker->outbox_lock);

    if (broker->outbox_count >= BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }

    mpi_outgoing_t *item = &broker->outbox[broker->outbox_count++];

    item->dest = dest;
    item->tag = BROKER_TAG_REPLICA_SYNC;
    item->replica_sync = *sync;

    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}

/*
 * Encola una actualización de metadatos para
 * todos los brokers excepto el coordinador.
 *
 * Retorna 0 si todas las solicitudes se encolaron.
 * Retorna -1 si alguna no pudo encolarse.
 */
static int broadcast_metadata(broker_t *broker, const metadata_update_t *update) {
    if (broker == NULL || update == NULL ||
        !broker->mpi_active) {
        return -1;
    }

    int rc = 0;

    for (int rank = 0; rank < broker->world_size; rank++) {
        if (rank == broker->rank) {
            continue;
        }

        if (send_metadata(broker, rank, update) != 0) {
            fprintf(stderr, "[broker %d] No se pudo encolar metadata para broker %d\n", broker->rank, rank);
            rc = -1;
        }
    }

    return rc;
}


static int metadata_tracking_start(broker_t *broker, uint64_t version) {
    pthread_mutex_lock(&broker->metadata_ack_lock);

    for (int i = 0; i < BROKER_MAX_PENDING_METADATA; i++) {
        metadata_ack_tracker_t *t = &broker->metadata_ack_trackers[i];

        if (!t->active) {
            t->version = version;
            t->expected = broker->world_size - 1;
            t->confirmed = 0;
            t->failed = 0;
            t->active = true;

            pthread_mutex_unlock(&broker->metadata_ack_lock);
            return 0;
        }
    }

    pthread_mutex_unlock(&broker->metadata_ack_lock);
    return -1;
}

static void metadata_tracking_ack(broker_t *broker, uint64_t version, int status) {
    pthread_mutex_lock(&broker->metadata_ack_lock);

    for (int i = 0; i < BROKER_MAX_PENDING_METADATA; i++) {
        metadata_ack_tracker_t *t = &broker->metadata_ack_trackers[i];

        if (!t->active || t->version != version) {
            continue;
        }

        if (status == 0) {
            t->confirmed++;
        } else {
            t->failed++;
        }

        int received = t->confirmed + t->failed;

        fprintf(stderr, "[broker %d] Metadata v%llu: %d/%d ACK, %d errores\n",
                broker->rank, (unsigned long long)version,
                received, t->expected, t->failed);

        if (received >= t->expected) {
            fprintf(stderr, "[broker %d] Metadata v%llu finalizada: %s\n",
                    broker->rank, (unsigned long long)version,
                    t->failed == 0 ? "OK" : "CON ERRORES");

            t->active = false;
        }

        break;
    }

    pthread_mutex_unlock(&broker->metadata_ack_lock);
}


// Solicita el envío de un heartbeat mediante la cola MPI.
static void broker_send_heartbeat(int dest_rank, void *ctx) {
    broker_t *broker = ctx;

    if (broker == NULL) {
        return;
    }

    // Solo el hilo MPI ejecutará MPI_Send.
    // Si la cola está llena, se descarta este heartbeat.
    // El siguiente intervalo permitirá reintentarlo.
    (void)send_control(broker, dest_rank, BROKER_TAG_HEARTBEAT);
}

// Actualiza el estado de sincronización de un broker remoto en todos los logs locales.
static void broker_update_replica_sync(broker_t *broker, int rank, bool in_sync) {
    if (!broker || !broker->cluster_metadata) return;

    pthread_mutex_lock(&broker->topic_lock);
    pthread_rwlock_rdlock(&broker->cluster_metadata->lock);

    for (int t = 0; t < broker->cluster_metadata->num_topics; t++) {
        topic_t *topic = broker->cluster_metadata->topics[t];

        if (!topic) continue;

        for (int p = 0; p < topic->num_partitions; p++) {
            partition_t *partition = &topic->partitions[p];

            if (partition->leader_rank != broker->rank || !partition->log) continue;

            int replica_index = 0;

            for (int r = 0; r < topic->replication_factor; r++) {
                int replica_rank = partition->replica_ranks[r];

                if (replica_rank == broker->rank) continue;

                if (replica_rank == rank) {
                    if (log_set_replica_in_sync(partition->log, replica_index, in_sync) != LOG_OK) {
                        fprintf(stderr, "[broker %d] Error actualizando ISR de replica %d\n", broker->rank, rank);
                    }
                    break;
                }

                replica_index++;
            }
        }
    }

    pthread_rwlock_unlock(&broker->cluster_metadata->lock);
    pthread_mutex_unlock(&broker->topic_lock);
}

// Se ejecuta cuando el detector considera caído un broker.
static void broker_peer_down(int rank, void *ctx) {
    broker_t *broker = ctx;
    if (!broker) return;
    fprintf(stderr, "[broker %d] Broker %d marcado como caido\n", broker->rank, rank);
    broker_update_replica_sync(broker, rank, false);
}

// Solicita la sincronización de todas las particiones locales de un líder remoto.
static void broker_request_replica_sync(broker_t *broker, int leader_rank) {
    if (!broker || !broker->cluster_metadata) return;

    pthread_mutex_lock(&broker->topic_lock);
    pthread_rwlock_rdlock(&broker->cluster_metadata->lock);

    for (int t = 0; t < broker->cluster_metadata->num_topics; t++) {
        topic_t *topic = broker->cluster_metadata->topics[t];

        if (!topic) continue;

        for (int p = 0; p < topic->num_partitions; p++) {
            partition_t *partition = &topic->partitions[p];

            if (partition->leader_rank != leader_rank || !partition->log) continue;

            bool local_replica = false;

            for (int r = 0; r < topic->replication_factor; r++) {
                if (partition->replica_ranks[r] == broker->rank) {
                    local_replica = true;
                    break;
                }
            }

            if (!local_replica) continue;

            replica_sync_t sync = {0};

            snprintf(sync.topic, sizeof(sync.topic), "%s", topic->name);
            sync.partition_id = partition->partition_id;
            sync.next_offset = log_next_offset(partition->log);

            if (send_replica_sync(broker, leader_rank, &sync) != 0) {
                fprintf(stderr, "[broker %d] Error solicitando sincronizacion de %s/%d\n", broker->rank, topic->name, partition->partition_id);
            }
        }
    }

    pthread_rwlock_unlock(&broker->cluster_metadata->lock);
    pthread_mutex_unlock(&broker->topic_lock);
}

// Se ejecuta cuando un broker anteriormente caído se recupera.
static void broker_peer_up(int rank, void *ctx) {
    broker_t *broker = ctx;

    if (!broker) return;

    fprintf(stderr, "[broker %d] Broker %d recuperado; iniciando reconciliacion\n", broker->rank, rank);

    // Si el broker recuperado es lider de alguna particion local,
    // solicitarle los registros pendientes.
    broker_request_replica_sync(broker, rank);

    // Si somos lider de particiones almacenadas por el broker recuperado,
    // pedirle que nos informe su offset real.
    if (send_control(broker, rank, BROKER_TAG_REPLICA_SYNC_REQUEST) != 0) {
        fprintf(stderr, "[broker %d] No se pudo solicitar reconciliacion a broker %d\n", broker->rank, rank);
    }
}

/* 
    STATE TRANSITIONS
*/

// state_tr: pause the broker
int broker_pause(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_ACTIVE) {
        pthread_mutex_unlock(&broker->state_lock);
        return -1;
    }
    broker->state = BROKER_STATE_PAUSED;
    pthread_cond_broadcast(&broker->state_changed);
    pthread_mutex_unlock(&broker->state_lock);
    return 0;
}

// state_tr: resume the broker
int broker_resume(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_PAUSED) {
        pthread_mutex_unlock(&broker->state_lock);
        return -1;
    }
    broker->state = BROKER_STATE_ACTIVE;
    pthread_cond_broadcast(&broker->state_changed);
    pthread_mutex_unlock(&broker->state_lock);
    return 0;
}

//  propagate: send a shutdown message to all other brokers.
//  state_tr: request shutdown of the broker and propagate to others
static void request_shutdown(broker_t *broker, bool propagate) {
    bool first = false;

    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_SHUTTING_DOWN) {
        broker->state = BROKER_STATE_SHUTTING_DOWN;
        first = true;
    }

    pthread_cond_broadcast(&broker->state_changed);
    pthread_mutex_unlock(&broker->state_lock);

    if (first && propagate && broker->mpi_active) {
        for (int rank = 0; rank < broker->world_size; rank++) {
            if (rank != broker->rank) {
                outbox_push(broker, rank, BROKER_TAG_SHUTDOWN);
            }
        }
    }
}

// state_tr: request shutdown of the broker and propagate to others
int broker_request_shutdown(broker_t *broker) {
    request_shutdown(broker, true);
    return 0;
}

/* 
    MPI THREAD
*/

// mpi_ut: flush the outbox queue and send messages via MPI
static void mpi_flush_outbox(broker_t *broker) {
    mpi_outgoing_t pending[BROKER_OUTBOX_CAPACITY];
    int count;

    pthread_mutex_lock(&broker->outbox_lock);
    count = broker->outbox_count;
    memcpy(pending, broker->outbox, (size_t)count * sizeof pending[0]);
    broker->outbox_count = 0;
    pthread_mutex_unlock(&broker->outbox_lock);

    for (int i = 0; i < count; i++) {
        if (pending[i].tag == BROKER_TAG_METADATA) {
            MPI_Send(&pending[i].metadata, (int)sizeof(metadata_update_t), MPI_BYTE, pending[i].dest, BROKER_TAG_METADATA,MPI_COMM_WORLD);
        } else if (pending[i].tag == BROKER_TAG_METADATA_ACK) {
            MPI_Send(&pending[i].ack, (int)sizeof(metadata_ack_t), MPI_BYTE, pending[i].dest, BROKER_TAG_METADATA_ACK, MPI_COMM_WORLD);
        } else if (pending[i].tag == BROKER_TAG_REPLICA_APPEND) {
            MPI_Send(&pending[i].replica_append, (int)sizeof(replica_append_t), MPI_BYTE, pending[i].dest, BROKER_TAG_REPLICA_APPEND, MPI_COMM_WORLD);
        } else if (pending[i].tag == BROKER_TAG_REPLICA_ACK) {
            MPI_Send(&pending[i].replica_ack, (int)sizeof(replica_ack_t), MPI_BYTE, pending[i].dest, BROKER_TAG_REPLICA_ACK, MPI_COMM_WORLD);
        } else if (pending[i].tag == BROKER_TAG_REPLICA_HW) {
            MPI_Send(&pending[i].replica_hw, (int)sizeof(replica_hw_t), MPI_BYTE, pending[i].dest, BROKER_TAG_REPLICA_HW, MPI_COMM_WORLD);
        } else if (pending[i].tag == BROKER_TAG_REPLICA_SYNC) {
            MPI_Send(&pending[i].replica_sync, (int)sizeof(replica_sync_t), MPI_BYTE, pending[i].dest, BROKER_TAG_REPLICA_SYNC, MPI_COMM_WORLD);
        } else {
            int payload = 0;
            MPI_Send(&payload, 1, MPI_INT, pending[i].dest, pending[i].tag, MPI_COMM_WORLD);
        }
    }
}

// mpi_ut: handle a received message based on its tag
static void mpi_handle_message(broker_t *broker, int source_rank, int tag) {
    switch (tag) {
        case BROKER_TAG_REPLICA_SYNC_REQUEST:
            broker_request_replica_sync(broker, source_rank);
            break;
        case BROKER_TAG_SHUTDOWN: request_shutdown(broker, false); break;
        case BROKER_TAG_PAUSE:    broker_pause(broker);            break;
        case BROKER_TAG_RESUME:   broker_resume(broker);           break;
        case BROKER_TAG_HEARTBEAT:
            if (broker->failure_detector_initialized) {
                heartbeat_received(
                    &broker->failure_detector,
                    source_rank,
                    failure_detector_now_ms()
                );
            }
            break;
        default:                                                   break;
    }
}

// mpi_ut: handle a received replica append message
static void handle_replica_append(broker_t *broker, int source, const replica_append_t *append) {
    replica_ack_t ack = {0};

    snprintf(ack.topic, sizeof(ack.topic), "%s", append->topic);
    ack.partition_id = append->partition_id;
    ack.end_offset = -1;
    ack.status = -1;

    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, append->topic);
    partition_t *partition = topic ? topic_get_partition(topic, append->partition_id) : NULL;


    bool local_replica = false;

    if (topic && partition) {
        for (int i = 0; i < topic->replication_factor; i++) {
            if (partition->replica_ranks[i] == broker->rank) {
                local_replica = true;
                break;
            }
        }
    }

    if (partition && local_replica && partition->leader_rank == source && partition->log &&
        append->record_size >= MESSAGE_HEADER_SIZE &&
        append->record_size <= BROKER_REPLICA_RECORD_MAX) {

        long next = log_next_offset(partition->log);
        ack.end_offset = next;
        
        if (next == append->offset) {
            message_t received = {0};
            size_t consumed = 0;

            if (message_deserialize(append->record, append->record_size, &received, &consumed) == MESSAGE_OK) {
                bool valid = consumed == append->record_size && received.offset == append->offset;
                message_destroy(&received);

                if (valid &&
                    log_append_record(partition->log, append->record, append->record_size) == LOG_OK &&
                    log_flush(partition->log) == LOG_OK) {
                    ack.status = 0;
                    ack.end_offset = append->offset + 1;
                }
            }
        } else if (next > append->offset) {
            // Registro posiblemente repetido: no confirmar sin verificarlo.
            fprintf(stderr, "[broker %d] Registro repetido; requiere reconciliacion\n", broker->rank);
        }
    }

    pthread_mutex_unlock(&broker->topic_lock);

    if (send_replica_ack(broker, source, &ack) != 0) {
        fprintf(stderr, "[broker %d] No se pudo encolar ACK de replica\n", broker->rank);
    }
}

// mpi_ut: resend a replica record to a destination broker
static int resend_replica_record(broker_t *broker, const topic_t *topic, partition_t *partition, int dest, long offset) {
    if (!broker || !topic || !partition || !partition->log) return -1;

    long start = log_start_offset(partition->log);
    long end = log_next_offset(partition->log);

    if (offset < start || offset >= end) return -1;

    replica_append_t append = {0};

    snprintf(append.topic, sizeof(append.topic), "%s", topic->name);
    append.partition_id = partition->partition_id;
    append.offset = offset;

    size_t used = 0;
    int count = 0;

    log_status_t rc = log_read_raw(partition->log, offset, append.record, sizeof(append.record), &used, &count);

    if (rc != LOG_OK || count < 1 || used < MESSAGE_HEADER_SIZE) return -1;

    size_t record_size = message_record_size(append.record);

    if (record_size < MESSAGE_HEADER_SIZE || record_size > used || record_size > sizeof(append.record)) return -1;

    append.record_size = record_size;

    return send_replica_append(broker, dest, &append);
}

// mpi_ut: handle a received replica sync message
static void handle_replica_sync(broker_t *broker, int source, const replica_sync_t *sync) {
    if (!broker || !sync || source == broker->rank) return;

    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, sync->topic);
    partition_t *partition = topic ? topic_get_partition(topic, sync->partition_id) : NULL;

    if (!partition || partition->leader_rank != broker->rank || !partition->log) {
        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    int replica_index = 0;
    int found = -1;

    for (int i = 0; i < topic->replication_factor; i++) {
        int rank = partition->replica_ranks[i];

        if (rank == broker->rank) continue;

        if (rank == source) {
            found = replica_index;
            break;
        }

        replica_index++;
    }

    if (found < 0) {
        fprintf(stderr, "[broker %d] Solicitud de sincronizacion rechazada desde %d\n", broker->rank, source);
        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    long start = log_start_offset(partition->log);
    long end = log_next_offset(partition->log);

    if (sync->next_offset < start || sync->next_offset > end) {
        fprintf(stderr, "[broker %d] Offset de sincronizacion invalido: %ld (rango %ld-%ld)\n", 
            broker->rank, sync->next_offset, start, end);
        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    if (log_replica_reset_progress(partition->log, found, sync->next_offset) != LOG_OK) {
        fprintf(stderr, "[broker %d] Error reiniciando progreso de replica %d\n", broker->rank, source);
        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    if (sync->next_offset < end) {
        if (resend_replica_record(broker, topic, partition, source, sync->next_offset) != 0) {
            fprintf(stderr, "[broker %d] Error iniciando sincronizacion de replica %d\n", broker->rank, source);
        }
    } else {
        replica_hw_t hw = {0};

        snprintf(hw.topic, sizeof(hw.topic), "%s", topic->name);
        hw.partition_id = partition->partition_id;
        hw.high_watermark = log_high_watermark(partition->log);

        if (send_replica_hw(broker, source, &hw) != 0) {
            fprintf(stderr, "[broker %d] Error enviando HW a replica %d\n", broker->rank, source);
        }
    }

    pthread_mutex_unlock(&broker->topic_lock);
}

// mpi_ut: handle a received replica ack message
static void handle_replica_ack(broker_t *broker, int source, const replica_ack_t *ack) {
    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, ack->topic);
    partition_t *partition = topic ? topic_get_partition(topic, ack->partition_id) : NULL;

    if (partition && partition->leader_rank == broker->rank && partition->log) {
        int replica_index = 0;
        int found = -1;

        for (int i = 0; i < topic->replication_factor; i++) {
            int rank = partition->replica_ranks[i];

            if (rank == broker->rank) continue;

            if (rank == source) {
                found = replica_index;
                break;
            }

            replica_index++;
        }

        long start = log_start_offset(partition->log);
        long end = log_next_offset(partition->log);
        long previous = -1;

        if (found >= 0 && log_replica_progress(partition->log, found, &previous) == LOG_OK) {

            if (ack->status == 0 && ack->end_offset >= start && ack->end_offset <= end) {

                if (ack->end_offset <= previous) {
                    // ACK duplicado o atrasado: no modificar progreso ni reenviar.
                    pthread_mutex_unlock(&broker->topic_lock);
                    return;
                }

                if (log_replica_ack(partition->log, found, ack->end_offset) != LOG_OK) {
                    fprintf(stderr, "[broker %d] Error actualizando ACK de replica %d\n", broker->rank, source);
                } else {
                    if (ack->end_offset == log_next_offset(partition->log) &&
                        broker->failure_detector_initialized &&
                        !failure_detector_is_down(&broker->failure_detector, source)) {

                        if (log_set_replica_in_sync(partition->log, found, true) != LOG_OK) {
                            fprintf(stderr, "[broker %d] Error reincorporando replica %d al ISR\n", broker->rank, source);
                        }
                    }
                    replica_hw_t hw = {0};

                    snprintf(hw.topic, sizeof(hw.topic), "%s", topic->name);
                    hw.partition_id = ack->partition_id;
                    hw.high_watermark = log_high_watermark(partition->log);

                    for (int i = 0; i < topic->replication_factor; i++) {
                        int dest = partition->replica_ranks[i];

                        if (dest == broker->rank) continue;

                        if (send_replica_hw(broker, dest, &hw) != 0) {
                            fprintf(stderr, "[broker %d] No se pudo encolar HW para broker %d\n", broker->rank, dest);
                        }
                    }

                    if (ack->end_offset < end) {
                        if (resend_replica_record(broker, topic, partition, source, ack->end_offset) != 0) {
                            fprintf(stderr, "[broker %d] No se pudo continuar recuperacion de replica %d\n", broker->rank, source);
                        }
                    }
                }

            } else if (ack->status == -1 && ack->end_offset >= start && ack->end_offset < end) {

                if (resend_replica_record(broker, topic, partition, source, ack->end_offset) != 0) {
                    fprintf(stderr, "[broker %d] No se pudo reenviar offset %ld a replica %d\n", broker->rank, ack->end_offset, source);
                }

            } else {
                fprintf(stderr, "[broker %d] ACK de replica %d rechazado o fallido\n", broker->rank, source);
            }
        }
    }

    pthread_mutex_unlock(&broker->topic_lock);
}

// mpi_ut: handle a received replica high watermark message
static void handle_replica_hw(broker_t *broker, int source, const replica_hw_t *hw) {
    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, hw->topic);
    partition_t *partition = topic ? topic_get_partition(topic, hw->partition_id) : NULL;

    if (partition && partition->leader_rank == source &&
        source != broker->rank && partition->log &&
        hw->high_watermark >= 0) {

        bool is_replica = false;

        for (int i = 0; i < topic->replication_factor; i++) {
            if (partition->replica_ranks[i] == broker->rank) {
                is_replica = true;
                break;
            }
        }

        if (is_replica) {
            log_advance_hw(partition->log, hw->high_watermark);
        }
    }

    pthread_mutex_unlock(&broker->topic_lock);
}



// mpi_ut: receive one message from MPI and optionally handle it
static bool mpi_receive_one(broker_t *broker, bool handle) {
    int flag = 0;
    MPI_Status status;

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);

    if (!flag) {
        return false;
    }


    if (status.MPI_TAG == BROKER_TAG_REPLICA_SYNC) {
        replica_sync_t sync = {0};

        MPI_Recv(&sync, (int)sizeof(sync), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (handle && memchr(sync.topic, '\0', sizeof(sync.topic)) != NULL) {
            handle_replica_sync(broker, status.MPI_SOURCE, &sync);
        }

        return true;
    }

    if (status.MPI_TAG == BROKER_TAG_REPLICA_HW) {
        replica_hw_t hw = {0};

        MPI_Recv(&hw, (int)sizeof(hw), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (handle && memchr(hw.topic, '\0', sizeof(hw.topic)) != NULL) {
            handle_replica_hw(broker, status.MPI_SOURCE, &hw);
        }

        return true;
    }

    if (status.MPI_TAG == BROKER_TAG_REPLICA_APPEND) {
        replica_append_t append = {0};

        MPI_Recv(&append, (int)sizeof(append), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (handle) {
            if (memchr(append.topic, '\0', sizeof(append.topic)) != NULL) {
                handle_replica_append(broker, status.MPI_SOURCE, &append);
            }
        }

        return true;
    }

    if (status.MPI_TAG == BROKER_TAG_REPLICA_ACK) {
        replica_ack_t ack = {0};

        MPI_Recv(&ack, (int)sizeof(ack), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (handle && memchr(ack.topic, '\0', sizeof(ack.topic)) != NULL) {
            handle_replica_ack(broker, status.MPI_SOURCE, &ack);
        }

        return true;
    }

    // 1. Recibir confirmaciones de metadatos (ACK).
    if (status.MPI_TAG == BROKER_TAG_METADATA_ACK) {
        metadata_ack_t ack = {0};

        MPI_Recv(&ack, (int)sizeof(ack), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (handle) {
            if (broker->rank == BROKER_COORDINATOR_RANK) {
                metadata_tracking_ack(broker, ack.version, ack.status);
            }
            
            fprintf(stderr, "[broker %d] ACK metadata version %llu " 
                "desde broker %d: %s\n",
                broker->rank,
                (unsigned long long)ack.version,
                status.MPI_SOURCE,
                ack.status == 0 ? "OK" : "ERROR"
            );
        }

        return true;
    }

    // 2. Recibir actualizaciones de metadatos.
    if (status.MPI_TAG == BROKER_TAG_METADATA) {
        metadata_update_t update = {0};

        MPI_Recv(&update, (int)sizeof(update), MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (handle) {
            int rc = -1;

            pthread_mutex_lock(&broker->topic_lock);

            // Solo el coordinador puede enviar actualizaciones.
            if (status.MPI_SOURCE == BROKER_COORDINATOR_RANK) {

                rc = apply_update(broker->cluster_metadata, &update);

                if (rc == 0) {
                    if (partition_storage_open(broker->cluster_metadata, broker->storage_path, broker->rank) != 0) {
                        fprintf(stderr, "[broker %d] Error abriendo logs del topic %s\n", broker->rank, update.topic);
                        rc = -1;
                    }
                } else if (rc < 0) {
                    fprintf(stderr, "[broker %d] Error aplicando metadata version %llu\n", broker->rank, (unsigned long long)update.version);
                }

            } else {
                fprintf(stderr, "[broker %d] Metadata rechazada desde broker %d\n", broker->rank, status.MPI_SOURCE);
            }

            pthread_mutex_unlock(&broker->topic_lock);

            if (rc == 0) {
                for (int rank = 0; rank < broker->world_size; rank++) {
                    if (rank != broker->rank) {
                        broker_request_replica_sync(broker, rank);
                    }
                }
            }

            // 3. Confirmar el resultado al coordinador.
            // rc == 0: actualización aplicada.
            // rc == 1: actualización ya aplicada.
            // rc < 0: error.
            int ack_status = (rc >= 0) ? 0 : -1;

            if (send_metadata_ack(broker, status.MPI_SOURCE, update.version, ack_status) != 0) {
                fprintf(stderr, "[broker %d] No se pudo encolar ACK\n", broker->rank);
            }
        }

        return true;
    }

    // 4. Recibir mensajes de control existentes.
    int payload = 0;

    MPI_Recv(&payload, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (handle) {
        mpi_handle_message(broker, status.MPI_SOURCE, status.MPI_TAG);
    }

    return true;
}


// mpi_ut: main loop of the MPI thread, flushing the outbox and receiving messages
static void *mpi_loop(void *arg) {
    broker_t *broker = arg;
    const struct timespec nap = {.tv_sec = 0, .tv_nsec = 1000000L};

    for (;;) {
        mpi_flush_outbox(broker);

        pthread_mutex_lock(&broker->outbox_lock);
        bool done = broker->outbox_stop && broker->outbox_count == 0;
        pthread_mutex_unlock(&broker->outbox_lock);
        if (done) {
            break;
        }

        if (!mpi_receive_one(broker, true)) {
            nanosleep(&nap, NULL);
        }
    }

    // comm: receive any remaining messages without handling them
    while (mpi_receive_one(broker, false)) {
    }
    return NULL;
}

/* 
    TCP_CLIENTS
*/


/*
 * Crea un topic en los metadatos locales y abre
 * los logs de las particiones asignadas al broker.
 *
 * Esta función todavía NO propaga metadatos por MPI.
 */
static void handle_create_topic(broker_t *broker, const request_t *request, response_t *response) {
    if (broker_get_state(broker) != BROKER_STATE_ACTIVE) {
        response->status = PROTO_STATUS_ERR;
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_INVALID_STATE);
        return;
    }

    // Solo el coordinador puede crear topics.
    if (broker->rank != BROKER_COORDINATOR_RANK) {
        response->status = PROTO_STATUS_ERR;
        snprintf(response->text_info, sizeof(response->text_info), "Topic creation must be sent to broker %d", BROKER_COORDINATOR_RANK);
        return;
    }

    // Validar parámetros del topic.
    if (request->topic[0] == '\0' || 
        strnlen(request->topic, sizeof(request->topic)) >= sizeof(request->topic) ||
        strchr(request->topic, '/') != NULL ||
        strchr(request->topic, '\\') != NULL ||
        strcmp(request->topic, ".") == 0 ||
        strcmp(request->topic, "..") == 0 ||
        request->partitions <= 0 ||
        request->replication_factor <= 0 ||
        request->replication_factor > broker->world_size ||
        request->replication_factor > MD_MAX_REPLICAS) {

        response->status = PROTO_STATUS_ERR;
        snprintf(response->text_info,
                 sizeof(response->text_info),
                 "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->topic_lock);

    if (find_topic(broker->cluster_metadata, request->topic) != NULL) {
        response->status = PROTO_STATUS_ERR;
        snprintf(response->text_info, sizeof(response->text_info), "Topic already exists");

        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    int rc = create_topic(broker->cluster_metadata, request->topic, request->partitions, request->replication_factor, broker->world_size);

    if (rc != 0) {
        response->status = PROTO_STATUS_ERR;
        snprintf(response->text_info, sizeof(response->text_info), "Could not create topic");

        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    rc = partition_storage_open(broker->cluster_metadata, broker->storage_path, broker->rank);

    if (rc != 0) {
        response->status = PROTO_STATUS_ERR;
        snprintf(response->text_info, sizeof(response->text_info), "%s: topic metadata created, storage open failed", ERR_IO_ERROR);
    
    } else {
        metadata_update_t update = {0};

        // Recuperar la versión asignada por create_topic().
        pthread_rwlock_rdlock(&broker->cluster_metadata->lock);
        update.version = broker->cluster_metadata->version;
        pthread_rwlock_unlock(&broker->cluster_metadata->lock);

        update.type = MD_CREATE_TOPIC;

        snprintf(update.topic, sizeof(update.topic), "%s", request->topic);

        update.num_partitions = request->partitions;
        update.replication_factor = request->replication_factor;
        update.num_brokers = broker->world_size;
        update.retention_bytes = 0;

        int tracking_rc = metadata_tracking_start(broker, update.version);
        int broadcast_rc = -1;

        if (tracking_rc == 0) {
            broadcast_rc = broadcast_metadata(broker, &update);
        }

        if (broadcast_rc == 0) {
            response->status = PROTO_STATUS_OK;

            snprintf(response->text_info, sizeof(response->text_info), "Topic '%s' created locally; propagation queued", request->topic);
        } else {
            response->status = PROTO_STATUS_ERR;

            snprintf(response->text_info, sizeof(response->text_info), "Topic created locally; metadata propagation incomplete");
        }
    }


    pthread_mutex_unlock(&broker->topic_lock);
}

// Maneja una solicitud de producción de mensajes.
static void handle_produce(broker_t *broker, const request_t *request, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    if (broker_get_state(broker) != BROKER_STATE_ACTIVE) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_INVALID_STATE);
        return;
    }

    if (strnlen(request->topic, sizeof(request->topic)) >= sizeof(request->topic) ||
        request->topic[0] == '\0' ||
        strnlen(request->key, sizeof(request->key)) >= sizeof(request->key) ||
        request->value_len == 0 || request->value_len > PROTO_MAX_PAYLOAD) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, request->topic);

    if (topic == NULL) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_NOT_FOUND);
        goto done;
    }

    int key_size = (int)strlen(request->key);
    int partition_id = topic_partition_for_key(topic, request->key, key_size);
    partition_t *partition = topic_get_partition(topic, partition_id);

    if (partition == NULL || partition->leader_rank != broker->rank || partition->log == NULL) {
        snprintf(response->text_info, sizeof(response->text_info), "Partition leader is not available on this broker");
        goto done;
    }

    if (request->acks == PROTO_ACKS_ALL &&
        !log_all_replicas_in_sync(partition->log)) {

        snprintf(response->text_info, sizeof(response->text_info), "Not enough in-sync replicas for acks=all");
        goto done;
    }

    message_t msg = {0};

    if (message_init(&msg, request->key, key_size, request->value, (int)request->value_len) != MESSAGE_OK) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
        goto done;
    }

    long offset = -1;
    log_status_t rc = log_append(partition->log, &msg, &offset);
    message_destroy(&msg);

    if (rc != LOG_OK) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
        goto done;
    }


    if (topic->replication_factor > 1) {
        replica_append_t append = {0};

        snprintf(append.topic, sizeof(append.topic), "%s", topic->name);
        append.partition_id = partition_id;
        append.offset = offset;

        size_t used = 0;
        int count = 0;

        log_status_t read_rc = log_read_raw(partition->log, offset, append.record, sizeof(append.record), &used, &count);

        if (read_rc != LOG_OK || count < 1 || used < MESSAGE_HEADER_SIZE) {
            snprintf(response->text_info, sizeof(response->text_info), "Could not read record for replication");
            goto done;
        }

        size_t first_size = message_record_size(append.record);

        if (first_size < MESSAGE_HEADER_SIZE || first_size > used || first_size > sizeof(append.record)) {
            snprintf(response->text_info, sizeof(response->text_info), "Invalid serialized record");
            goto done;
        }

        append.record_size = first_size;

        for (int i = 0; i < topic->replication_factor; i++) {
            int dest = partition->replica_ranks[i];

            if (dest == broker->rank) {
                continue;
            }

            if (send_replica_append(broker, dest, &append) != 0) {
                snprintf(response->text_info, sizeof(response->text_info), "Message stored locally; replication queue failed");
                goto done;
            }
        }
    }


    // Confirmar la escritura local en disco para acks=leader.
    if (request->acks == PROTO_ACKS_LEADER || request->acks == PROTO_ACKS_ALL) {
        if (log_flush(partition->log) != LOG_OK) {
            snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
            goto done;
        }
    }

    if (request->acks == PROTO_ACKS_ALL) {
        // Liberar topic_lock para permitir que el hilo MPI procese los ACK.
        partition_log_t *log = partition->log;
        pthread_mutex_unlock(&broker->topic_lock);
        log_status_t wait_rc = log_wait_hw(log, offset, 5000);
        pthread_mutex_lock(&broker->topic_lock);

        if (wait_rc == LOG_TIMEOUT) {
            snprintf(response->text_info, sizeof(response->text_info), "Replication confirmation timeout");
            goto done;
        }

        if (wait_rc != LOG_OK) {
            snprintf(response->text_info, sizeof(response->text_info), "Replication confirmation failed");
            goto done;
        }

        if (!log_all_replicas_in_sync(log)) {
            snprintf(response->text_info, sizeof(response->text_info), "Replication quorum changed during confirmation");
            goto done;
        }

        // Persistir el high watermark confirmado.
        if (log_flush(log) != LOG_OK) {
            snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
            goto done;
        }
    }

    response->status = PROTO_STATUS_OK;
    response->partition_id = partition_id;
    response->offset = offset;

    snprintf(response->text_info, sizeof(response->text_info), "Message stored in partition %d at offset %ld", partition_id, offset);

    done:
        pthread_mutex_unlock(&broker->topic_lock);
}

static int make_group_id(const request_t *request, char *buffer, size_t capacity);
static consumer_group_t *find_consumer_group(broker_t *broker, const char *group_id);
static int make_group_id(const request_t *request, char *buffer, size_t capacity);

// Maneja una solicitud de fetch de mensajes.
static void handle_fetch(broker_t *broker, int fd, const request_t *request, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    if (broker_get_state(broker) != BROKER_STATE_ACTIVE) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_INVALID_STATE);
        return;
    }

    if (request->topic[0] == '\0' ||
        strnlen(request->topic, sizeof(request->topic)) >= sizeof(request->topic) ||
        request->partition_id < 0 || (!request->from_beginning && request->offset < 0)) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, request->topic);

    if (request->group[0] != '\0') {
        char group_id[PROTO_MAX_TOPIC_LEN + PROTO_MAX_GROUP_LEN + 32];

        if (make_group_id(request, group_id, sizeof(group_id)) != 0 ||
            request->member_id <= 0) {
            snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
            goto done;
        }

        pthread_mutex_lock(&broker->groups_lock);

        consumer_group_t *group = find_consumer_group(broker, group_id);
        bool authorized = false;

        if (group) {
            pthread_mutex_lock(&group->lock);

            if (request->partition_id >= 0 && request->partition_id < group->partition_count &&
                group->assignment[request->partition_id] == request->member_id) {
                for (int i = 0; i < group->member_count; i++) {
                    if (group->members[i].member_id == request->member_id &&
                        group->members[i].client_fd == fd) {
                        authorized = true;
                        break;
                    }
                }
            }

            pthread_mutex_unlock(&group->lock);
        }

        pthread_mutex_unlock(&broker->groups_lock);

        if (!authorized) {
            snprintf(response->text_info, sizeof(response->text_info),
                "Consumer is not assigned to this partition");
            goto done;
        }
    }

    if (topic == NULL) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_NOT_FOUND);
        goto done;
    }

    partition_t *partition = topic_get_partition(topic, request->partition_id);

    if (partition == NULL) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        goto done;
    }

    if (partition->log == NULL) {
        snprintf(response->text_info, sizeof(response->text_info), "Partition is not stored on this broker");
        goto done;
    }

    long offset = request->from_beginning ? log_start_offset(partition->log) : request->offset;
    long hw = log_high_watermark(partition->log);

    if (offset < log_start_offset(partition->log) || offset > hw) {
        snprintf(response->text_info, sizeof(response->text_info), "Offset outside readable range");
        goto done;
    }

    // No hay todavía un mensaje confirmado en ese offset.
    if (offset == hw) {
        response->status = PROTO_STATUS_OK;
        response->partition_id = request->partition_id;
        response->offset = offset;
        response->data_len = 0;
        snprintf(response->text_info, sizeof(response->text_info), "No messages available");
        goto done;
    }

    message_t msg = {0};
    int count = 0;
    log_status_t rc = log_read(partition->log, offset, &msg, 1, &count);

    if (rc != LOG_OK || count != 1) {
        if (count > 0) message_destroy(&msg);
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
        goto done;
    }

    if (msg.value_size < 0 || msg.value_size > PROTO_MAX_PAYLOAD) {
        message_destroy(&msg);
        snprintf(response->text_info, sizeof(response->text_info), "Message exceeds protocol payload limit");
        goto done;
    }

    response->data_len = (size_t)msg.value_size;
    if (response->data_len > 0) memcpy(response->payload, msg.value, response->data_len);

    response->partition_id = request->partition_id;
    response->offset = msg.offset;
    response->status = PROTO_STATUS_OK;

    snprintf(response->text_info, sizeof(response->text_info), "Message fetched at offset %ld", msg.offset);
    message_destroy(&msg);

    done:
        pthread_mutex_unlock(&broker->topic_lock);
}

// Maneja una solicitud de commit de offset de un consumidor.
static consumer_group_t *find_consumer_group(broker_t *broker, const char *group_id) {
    for (int i = 0; i < broker->num_consumer_groups; i++) {
        consumer_group_t *group = broker->consumer_groups[i];

        if (group && strcmp(group->group_id, group_id) == 0) {
            return group;
        }
    }

    return NULL;
}

// Crea un identificador único para un grupo de consumidores basado en el topic y el nombre del grupo.
static int make_group_id(const request_t *request, char *buffer, size_t capacity) {
    if (request->topic[0] == '\0' || request->group[0] == '\0' ||
        strnlen(request->topic, sizeof(request->topic)) >= sizeof(request->topic) ||
        strnlen(request->group, sizeof(request->group)) >= sizeof(request->group)) {
        return -1;
    }

    int written = snprintf(buffer, capacity, "%zu:%s:%s",
        strlen(request->topic), request->topic, request->group);

    return written < 0 || (size_t)written >= capacity ? -1 : 0;
}

// Maneja una solicitud de commit de offset de un consumidor.
static void handle_consume_join(broker_t *broker, int fd, const request_t *request, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    if (broker_get_state(broker) != BROKER_STATE_ACTIVE) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_INVALID_STATE);
        return;
    }

    char group_id[PROTO_MAX_TOPIC_LEN + PROTO_MAX_GROUP_LEN + 32];

    if (make_group_id(request, group_id, sizeof(group_id)) != 0) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, request->topic);
    int partition_count = topic ? topic->num_partitions : 0;

    pthread_mutex_unlock(&broker->topic_lock);

    if (partition_count <= 0) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_NOT_FOUND);
        return;
    }

    pthread_mutex_lock(&broker->groups_lock);

    consumer_group_t *group = find_consumer_group(broker, group_id);

    if (!group) {
        group = calloc(1, sizeof(*group));

        if (!group || group_init(group, group_id, partition_count) != 0) {
            free(group);
            pthread_mutex_unlock(&broker->groups_lock);
            snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
            return;
        }

        if (broker->num_consumer_groups == broker->consumer_groups_capacity) {
            int capacity = broker->consumer_groups_capacity > 0 ? broker->consumer_groups_capacity * 2 : 4;

            consumer_group_t **groups = realloc(broker->consumer_groups, (size_t)capacity * sizeof(*groups));

            if (!groups) {
                group_destroy(group);
                free(group);
                pthread_mutex_unlock(&broker->groups_lock);
                snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
                return;
            }

            broker->consumer_groups = groups;
            broker->consumer_groups_capacity = capacity;
        }

        broker->consumer_groups[broker->num_consumer_groups++] = group;
    }

    int member_id = 0;

    if (group_join(group, fd, &member_id) != 0) {
        pthread_mutex_unlock(&broker->groups_lock);
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
        return;
    }

    pthread_mutex_lock(&group->lock);
    int generation = group->generation;
    pthread_mutex_unlock(&group->lock);

    response->status = PROTO_STATUS_OK;
    response->member_id = member_id;

    snprintf(response->text_info, sizeof(response->text_info),
        "Joined group %s; member_id=%d; generation=%d",
        request->group, member_id, generation);

    pthread_mutex_unlock(&broker->groups_lock);
}

// Maneja una solicitud de commit de offset de un consumidor.
static void handle_consume_commit(broker_t *broker, int fd, const request_t *request, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    if (broker_get_state(broker) != BROKER_STATE_ACTIVE) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_INVALID_STATE);
        return;
    }

    char group_id[PROTO_MAX_TOPIC_LEN + PROTO_MAX_GROUP_LEN + 32];

    if (make_group_id(request, group_id, sizeof(group_id)) != 0 ||
        request->member_id <= 0 || request->partition_id < 0 || request->offset < 0) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->groups_lock);

    consumer_group_t *group = find_consumer_group(broker, group_id);

    if (!group) {
        pthread_mutex_unlock(&broker->groups_lock);
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_NOT_FOUND);
        return;
    }

    bool valid_member = false;

    pthread_mutex_lock(&group->lock);

    for (int i = 0; i < group->member_count; i++) {
        if (group->members[i].member_id == request->member_id &&
            group->members[i].client_fd == fd) {
            valid_member = true;
            break;
        }
    }

    pthread_mutex_unlock(&group->lock);

    int rc = valid_member ? group_commit(group, request->member_id, request->partition_id, request->offset) : -1;

    pthread_mutex_unlock(&broker->groups_lock);

    if (rc != 0) {
        snprintf(response->text_info, sizeof(response->text_info),
            "Commit rejected: member is invalid or partition is not assigned");
        return;
    }

    response->status = PROTO_STATUS_OK;
    response->partition_id = request->partition_id;
    response->offset = request->offset;

    snprintf(response->text_info, sizeof(response->text_info),
        "Committed offset %ld for partition %d", request->offset, request->partition_id);
}

// Maneja una solicitud de descripción de grupo de consumidores.
static void handle_group_describe(broker_t *broker, const request_t *request, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    char group_id[PROTO_MAX_TOPIC_LEN + PROTO_MAX_GROUP_LEN + 32];

    if (make_group_id(request, group_id, sizeof(group_id)) != 0) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->groups_lock);

    consumer_group_t *group = find_consumer_group(broker, group_id);

    if (!group) {
        pthread_mutex_unlock(&broker->groups_lock);
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_NOT_FOUND);
        return;
    }

    int rc = group_describe(group, response->text_info, sizeof(response->text_info));

    pthread_mutex_unlock(&broker->groups_lock);

    if (rc != 0) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_IO_ERROR);
        return;
    }

    response->status = PROTO_STATUS_OK;
}


static void handle_topic_list(broker_t *broker, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    pthread_mutex_lock(&broker->topic_lock);
    pthread_rwlock_rdlock(&broker->cluster_metadata->lock);

    size_t used = 0;
    int count = broker->cluster_metadata->num_topics;

    for (int i = 0; i < count; i++) {
        topic_t *topic = broker->cluster_metadata->topics[i];
        if (!topic) continue;

        int written = snprintf(response->text_info + used,
            sizeof(response->text_info) - used, "%s\n", topic->name);

        if (written < 0 || (size_t)written >= sizeof(response->text_info) - used) {
            snprintf(response->text_info, sizeof(response->text_info), "Topic list exceeds response limit");
            goto done;
        }

        used += (size_t)written;
    }

    if (used == 0) {
        snprintf(response->text_info, sizeof(response->text_info), "No topics available");
    }

    response->status = PROTO_STATUS_OK;

done:
    pthread_rwlock_unlock(&broker->cluster_metadata->lock);
    pthread_mutex_unlock(&broker->topic_lock);
}

static void handle_topic_describe(broker_t *broker, const request_t *request, response_t *response) {
    response->status = PROTO_STATUS_ERR;

    if (request->topic[0] == '\0' ||
        strnlen(request->topic, sizeof(request->topic)) >= sizeof(request->topic)) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_BAD_REQUEST);
        return;
    }

    pthread_mutex_lock(&broker->topic_lock);

    topic_t *topic = find_topic(broker->cluster_metadata, request->topic);

    if (!topic) {
        snprintf(response->text_info, sizeof(response->text_info), "%s", ERR_NOT_FOUND);
        pthread_mutex_unlock(&broker->topic_lock);
        return;
    }

    size_t used = 0;

    int written = snprintf(response->text_info, sizeof(response->text_info),
        "Topic: %s\nPartitions: %d\nReplication factor: %d\n",
        topic->name, topic->num_partitions, topic->replication_factor);

    if (written < 0 || (size_t)written >= sizeof(response->text_info)) {
        goto too_long;
    }

    used = (size_t)written;

    for (int i = 0; i < topic->num_partitions; i++) {
        partition_t *partition = &topic->partitions[i];

        written = snprintf(response->text_info + used,
            sizeof(response->text_info) - used,
            "Partition %d: leader=%d, replicas=",
            partition->partition_id, partition->leader_rank);

        if (written < 0 || (size_t)written >= sizeof(response->text_info) - used) {
            goto too_long;
        }

        used += (size_t)written;

        for (int j = 0; j < topic->replication_factor; j++) {
            written = snprintf(response->text_info + used,
                sizeof(response->text_info) - used, "%s%d",
                j == 0 ? "" : ",", partition->replica_ranks[j]);

            if (written < 0 || (size_t)written >= sizeof(response->text_info) - used) {
                goto too_long;
            }

            used += (size_t)written;
        }

        written = snprintf(response->text_info + used,
            sizeof(response->text_info) - used, "\n");

        if (written < 0 || (size_t)written >= sizeof(response->text_info) - used) {
            goto too_long;
        }

        used += (size_t)written;
    }

    response->status = PROTO_STATUS_OK;
    pthread_mutex_unlock(&broker->topic_lock);
    return;

    too_long:
        snprintf(response->text_info, sizeof(response->text_info), "Topic description exceeds response limit");
        pthread_mutex_unlock(&broker->topic_lock);
}


// Procesa una solicitud binaria recibida desde un cliente TCP.
static int process_request(broker_t *broker, int fd) {
    request_t request;
    response_t response = {0};

    // Recibir solicitud del cliente.
    if (proto_recv_request(fd, &request) != 0) {
        return -1;
    }

    // Validar la solicitud.
    if (!validate_request(&request)) {
        response.status = PROTO_STATUS_ERR;
        snprintf(response.text_info, sizeof(response.text_info), "%s", ERR_BAD_REQUEST);

        return proto_send_response(fd, &response);
    }

    response.status = PROTO_STATUS_ERR;

    // Procesar operaciones.
    switch (request.op) {
        case PROTO_OP_TOPIC_LIST:
            handle_topic_list(broker, &response);
            break;

        case PROTO_OP_TOPIC_DESCRIBE:
            handle_topic_describe(broker, &request, &response);
            break;

        case PROTO_OP_CONSUME_JOIN:
            handle_consume_join(broker, fd, &request, &response);
            break;

        case PROTO_OP_CONSUME_COMMIT:
            handle_consume_commit(broker, fd, &request, &response);
            break;

        case PROTO_OP_GROUP_DESCRIBE:
            handle_group_describe(broker, &request, &response);
            break;

        case PROTO_OP_PRODUCE:
            handle_produce(broker, &request, &response);
            break;

        case PROTO_OP_CONSUME_FETCH:
            handle_fetch(broker, fd, &request, &response);
            break;

        case PROTO_OP_TOPIC_CREATE:
            handle_create_topic(broker, &request, &response);
            break;

        case PROTO_OP_BROKER_PAUSE:
            if (request.broker_id == broker->rank) {
                if (broker_pause(broker) == 0) {
                    response.status = PROTO_STATUS_OK;
                    snprintf(response.text_info, sizeof(response.text_info), "Broker paused");
                } else {
                    snprintf(response.text_info, sizeof(response.text_info), "%s", ERR_INVALID_STATE);
                }
            } else if (send_control(broker, request.broker_id, BROKER_TAG_PAUSE) == 0) {
                response.status = PROTO_STATUS_OK;
                snprintf(response.text_info, sizeof(response.text_info), "Pause request sent");
            }
            break;

        case PROTO_OP_BROKER_RESUME:
            if (request.broker_id == broker->rank) {
                if (broker_resume(broker) == 0) {
                    response.status = PROTO_STATUS_OK;
                    snprintf(response.text_info, sizeof(response.text_info), "Broker resumed");
                } else {
                    snprintf(response.text_info, sizeof(response.text_info), "%s", ERR_INVALID_STATE);
                }
            } else if (send_control(broker, request.broker_id, BROKER_TAG_RESUME) == 0) {
                response.status = PROTO_STATUS_OK;
                snprintf(response.text_info, sizeof(response.text_info), "Resume request sent");
            }
            break;

        case PROTO_OP_SHUTDOWN:
            broker_request_shutdown(broker);
            response.status = PROTO_STATUS_OK;
            snprintf(response.text_info, sizeof(response.text_info), "Broker shutting down");
            break;

        default:
            snprintf(response.text_info, sizeof(response.text_info), "Operation not implemented yet");
            break;
    }

    return proto_send_response(fd, &response);
}

// Maneja la desconexión de un cliente TCP, eliminando sus miembros de los grupos de consumidores.
static void broker_remove_disconnected_members(broker_t *broker, int fd) {
    pthread_mutex_lock(&broker->groups_lock);

    for (int g = 0; g < broker->num_consumer_groups; g++) {
        consumer_group_t *group = broker->consumer_groups[g];

        if (!group) continue;

        for (;;) {
            int member_id = -1;

            pthread_mutex_lock(&group->lock);

            for (int i = 0; i < group->member_count; i++) {
                if (group->members[i].client_fd == fd) {
                    member_id = group->members[i].member_id;
                    break;
                }
            }

            pthread_mutex_unlock(&group->lock);

            if (member_id < 0) break;

            if (group_leave(group, member_id) != 0) break;
        }
    }

    pthread_mutex_unlock(&broker->groups_lock);
}

// tcp: handle a connection from a client   
static void handle_connection(broker_t *broker, int fd) {

    while (broker_get_state(broker) != BROKER_STATE_SHUTTING_DOWN) {

        struct pollfd pfd = {
            .fd = fd,
            .events = POLLIN,
            .revents = 0
        };

        int ready = poll(&pfd, 1, BROKER_POLL_MS);

        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (ready == 0) {
            continue;
        }

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            break;
        }

        if (pfd.revents & POLLIN) {
            if (process_request(broker, fd) != 0) {
                break;
            }
        }
    }

    broker_remove_disconnected_members(broker, fd);
    close(fd);
}

// tcp: worker thread loop to handle client connections
static void *worker_loop(void *arg) {
    broker_t *broker = arg;
    int fd;

    while (request_queue_pop(broker->request_queue, &fd) == 0) {
        handle_connection(broker, fd);
    }
    return NULL;
}

// tcp: loop to accept new client connections
static void *acceptor_loop(void *arg) {
    broker_t *broker = arg;

    while (broker_get_state(broker) != BROKER_STATE_SHUTTING_DOWN) {
        struct pollfd pfd = {.fd = broker->listen_fd, .events = POLLIN, .revents = 0};
        int ready = poll(&pfd, 1, BROKER_POLL_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0) {
            continue;
        }

        int client_fd = accept(broker->listen_fd, NULL, NULL);
        if (client_fd < 0) {
            continue;
        }
        if (request_queue_push(broker->request_queue, client_fd) != 0) {
            close(client_fd);
        }
    }
    return NULL;
}

// tcp: open a listening socket on the specified port
static int open_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) != 0 ||
        listen(fd, BROKER_BACKLOG) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* 
    LIFE CYCLE
*/

// life_cy: initialize the broker with the specified parameters
int broker_init(broker_t *broker, const char *name, const char *host, int port, int rank, const char *storage_path) {
    if (broker == NULL || name == NULL || host == NULL || storage_path == NULL) {
        return -1;
    }

    // comm: initialize broker structure and allocate memory for strings
    memset(broker, 0, sizeof *broker);
    broker->name = strdup(name);
    broker->host = strdup(host);
    broker->storage_path = strdup(storage_path);
    if (broker->name == NULL || broker->host == NULL || broker->storage_path == NULL) {
        free(broker->name);
        free(broker->host);
        free(broker->storage_path);
        return -1;
    }

    broker->port = port;
    broker->rank = rank;
    broker->listen_fd = -1;
    broker->num_workers = BROKER_DEFAULT_WORKERS;
    broker->state = BROKER_STATE_INACTIVE;

    if (make_directories(storage_path) != 0) {
        free(broker->name);
        free(broker->host);
        free(broker->storage_path);
        return -1;
    }

    if (pthread_mutex_init(&broker->state_lock, NULL) != 0) {
        goto fail_strings;
    }
    if (pthread_cond_init(&broker->state_changed, NULL) != 0) {
        goto fail_state_lock;
    }
    if (pthread_mutex_init(&broker->outbox_lock, NULL) != 0) {
        goto fail_state_changed;
    }

    if (pthread_mutex_init(&broker->groups_lock, NULL) != 0) {
        goto fail_outbox_lock;
    }

    broker->consumer_groups = calloc(BROKER_INITIAL_GROUPS, sizeof(consumer_group_t *));

    if (broker->consumer_groups == NULL) {
        goto fail_groups_lock;
    }

    broker->num_consumer_groups = 0;
    broker->consumer_groups_capacity = BROKER_INITIAL_GROUPS;

    if (pthread_mutex_init(&broker->topic_lock, NULL) != 0) {
        free(broker->consumer_groups);
        broker->consumer_groups = NULL;
        goto fail_groups_lock;
    }

    if (pthread_mutex_init(&broker->metadata_ack_lock, NULL) != 0) {
        pthread_mutex_destroy(&broker->topic_lock);
        free(broker->consumer_groups);
        broker->consumer_groups = NULL;
        goto fail_groups_lock;
    }

    return 0;

    fail_groups_lock:
        pthread_mutex_destroy(&broker->groups_lock);

    fail_outbox_lock:
        pthread_mutex_destroy(&broker->outbox_lock);

    fail_state_changed:
        pthread_cond_destroy(&broker->state_changed);

    fail_state_lock:
        pthread_mutex_destroy(&broker->state_lock);

    fail_strings:
        free(broker->name);
        free(broker->host);
        free(broker->storage_path);
        return -1;
}

// life_cy: run the broker, starting threads and listening for connections
int broker_run(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_INACTIVE) {
        pthread_mutex_unlock(&broker->state_lock);
        return -1;
    }
    broker->state = BROKER_STATE_ACTIVE;
    pthread_mutex_unlock(&broker->state_lock);

    // Inicializar los metadatos locales del cluster.
    broker->cluster_metadata = malloc(sizeof *broker->cluster_metadata);

    if (broker->cluster_metadata == NULL) {
        goto fail;
    }

    if (cluster_metadata_init(broker->cluster_metadata) != 0) {
        free(broker->cluster_metadata);
        broker->cluster_metadata = NULL;
        goto fail;
    }

    broker->request_queue = malloc(sizeof *broker->request_queue);
    if (broker->request_queue == NULL) {
        goto fail;
    }
    if (request_queue_init(broker->request_queue, BROKER_QUEUE_CAPACITY) != 0) {
        free(broker->request_queue);
        broker->request_queue = NULL;
        goto fail;
    }

    // Abrir los logs de las particiones asignadas a este broker.
    if (partition_storage_open(broker->cluster_metadata, broker->storage_path, broker->rank) != 0) {
        fprintf(stderr, "[broker %d] Error al abrir almacenamiento local\n", broker->rank);
        goto fail;
    }

    broker->listen_fd = open_listener(broker->port);
    if (broker->listen_fd < 0) {
        goto fail;
    }

    int mpi_initialized = 0;
    MPI_Initialized(&mpi_initialized);
    if (mpi_initialized) {
        MPI_Comm_size(MPI_COMM_WORLD, &broker->world_size);
        broker->mpi_active = true;

        if (broker->world_size > FD_MAX_BROKERS) {
            fprintf(stderr, "Demasiados brokers para el detector\n");
            goto fail;
        }

        if (pthread_create(&broker->mpi_thread, NULL, mpi_loop, broker) != 0) {
            goto fail;
        }
        broker->mpi_started = true;

        if (failure_detector_init(&broker->failure_detector,
                broker->rank,
                broker->world_size,
                BROKER_HEARTBEAT_INTERVAL_MS,
                BROKER_HEARTBEAT_TIMEOUT_MS,
                broker_send_heartbeat,
                broker_peer_down,
                broker_peer_up,
                broker
            ) != 0) {
            goto fail;
        }

        broker->failure_detector_initialized = true;

        if (failure_detector_start(&broker->failure_detector) != 0) {
            goto fail;
        }
    }

    broker->worker_threads = calloc((size_t)broker->num_workers, sizeof *broker->worker_threads);
    if (broker->worker_threads == NULL) {
        goto fail;
    }
    for (int i = 0; i < broker->num_workers; i++) {
        if (pthread_create(&broker->worker_threads[i], NULL, worker_loop, broker) != 0) {
            goto fail;
        }
        broker->workers_started++;
    }

    if (pthread_create(&broker->acceptor_thread, NULL, acceptor_loop, broker) != 0) {
        goto fail;
    }
    broker->acceptor_started = true;

    pthread_mutex_lock(&broker->state_lock);
    while (broker->state != BROKER_STATE_SHUTTING_DOWN) {
        pthread_cond_wait(&broker->state_changed, &broker->state_lock);
    }
    pthread_mutex_unlock(&broker->state_lock);
    return 0;

    fail:
        broker_shutdown(broker);
        return -1;
}

// life_cy: shutdown the broker, stopping threads and closing sockets
int broker_shutdown(broker_t *broker) {
    broker_request_shutdown(broker);

    if (broker->request_queue != NULL) {
        request_queue_close(broker->request_queue);
    }

    if (broker->acceptor_started) {
        pthread_join(broker->acceptor_thread, NULL);
        broker->acceptor_started = false;
    }

    for (int i = 0; i < broker->workers_started; i++) {
        pthread_join(broker->worker_threads[i], NULL);
    }
    broker->workers_started = 0;
    free(broker->worker_threads);
    broker->worker_threads = NULL;

    if (broker->listen_fd >= 0) {
        close(broker->listen_fd);
        broker->listen_fd = -1;
    }

    if (broker->failure_detector_initialized) {
        failure_detector_stop(&broker->failure_detector);
        failure_detector_destroy(&broker->failure_detector);
        broker->failure_detector_initialized = false;
    }

    if (broker->mpi_started) {
        pthread_mutex_lock(&broker->outbox_lock);
        broker->outbox_stop = true;
        pthread_mutex_unlock(&broker->outbox_lock);
        pthread_join(broker->mpi_thread, NULL);
        broker->mpi_started = false;
    }

    // Cerrar logs antes de destruir los metadatos.
    if (broker->cluster_metadata != NULL) {
        partition_storage_close(broker->cluster_metadata);

        cluster_metadata_destroy(broker->cluster_metadata);
        free(broker->cluster_metadata);
        broker->cluster_metadata = NULL;
    }

    if (broker->request_queue != NULL) {
        request_queue_destroy(broker->request_queue);
        free(broker->request_queue);
        broker->request_queue = NULL;
    }
    return 0;
}

// life_cy: destroy the broker, freeing resources and memory
void broker_destroy(broker_t *broker) {
    if (broker == NULL) {
        return;
    }

    // Liberar grupos de consumidores.
    for (int i = 0; i < broker->num_consumer_groups; i++) {
        if (broker->consumer_groups[i] != NULL) {
            group_destroy(broker->consumer_groups[i]);
            free(broker->consumer_groups[i]);
        }
    }

    free(broker->consumer_groups);
    broker->consumer_groups = NULL;

    broker->num_consumer_groups = 0;
    broker->consumer_groups_capacity = 0;

    pthread_mutex_destroy(&broker->groups_lock);
    pthread_mutex_destroy(&broker->topic_lock);
    pthread_mutex_destroy(&broker->metadata_ack_lock);

    free(broker->name);
    free(broker->host);
    free(broker->storage_path);
    broker->name = NULL;
    broker->host = NULL;
    broker->storage_path = NULL;

    pthread_mutex_destroy(&broker->outbox_lock);
    pthread_cond_destroy(&broker->state_changed);
    pthread_mutex_destroy(&broker->state_lock);
}