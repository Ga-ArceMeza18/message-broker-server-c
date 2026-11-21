// topic.c - Tópicos, particionado por clave y apertura de los logs de cada partición

#define _POSIX_C_SOURCE 200809L

#include "topic.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TOPIC_PATH_MAX 512

// Funciones privadas

// Revisa que el nombre sirva como carpeta: solo letras, números, punto, guion y
// guion bajo, y que no sea "." ni "..". Así un nombre como "../x" no puede
// escribir fuera del directorio de datos
static bool valid_name(const char* name) {
    if (name == NULL || name[0] == '\0' || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        return false;
    }

    for (const char* c = name; *c != '\0'; c++) {
        bool letter = (*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z');
        bool digit = *c >= '0' && *c <= '9';

        if (!letter && !digit && *c != '.' && *c != '-' && *c != '_') {
            return false;
        }
    }

    return true;
}

// Cuántos seguidores tiene la partición (las réplicas sin contar al líder)
static int follower_count(const partition_t* p) {
    int count = 0;

    for (int i = 0; i < p->num_replicas; i++) {
        if (p->replica_ranks[i] >= 0 && p->replica_ranks[i] != p->leader_rank) {
            count++;
        }
    }

    return count;
}

// Revisa si rank está en el conjunto de réplicas sincronizadas
static bool in_isr(const partition_t* p, int rank) {
    for (int i = 0; i < p->num_replicas; i++) {
        if (p->isr_ranks[i] == rank) {
            return true;
        }
    }

    return false;
}

// Le pone al log de la partición el rol que dicen los metadatos
static int partition_update_role(partition_t* p, int my_rank) {
    if (p->log == NULL) {
        return 0;
    }

    if (!partition_is_leader(p, my_rank)) {
        log_set_follower(p->log);
        return 0;
    }

    // Solo se arma de nuevo si antes no era líder, porque log_set_leader olvida
    // hasta dónde tenía cada réplica
    if (!log_is_leader(p->log) && log_set_leader(p->log, follower_count(p)) != LOG_OK) {
        return -1;
    }

    // El log solo espera a las réplicas que siguen sincronizadas
    for (int i = 0; i < p->num_replicas; i++) {
        int rank = p->replica_ranks[i];
        int index = partition_follower_index(p, rank);

        if (index >= 0 && log_set_replica_in_sync(p->log, index, in_isr(p, rank)) != LOG_OK) {
            return -1;
        }
    }

    return 0;
}

// Funciones públicas

topic_t* topic_create(const char* name, int num_partitions, int replication_factor, long retention_bytes) {
    if (!valid_name(name) || num_partitions <= 0 || replication_factor <= 0 || retention_bytes < 0) {
        return NULL;
    }

    topic_t* t = calloc(1, sizeof(*t));

    if (t == NULL) {
        return NULL;
    }

    t->name = strdup(name);
    t->num_partitions = num_partitions;
    t->replication_factor = replication_factor;
    t->retention_bytes = retention_bytes;
    t->partitions = calloc((size_t)num_partitions, sizeof(partition_t));
    atomic_init(&t->next_partition, 0);

    if (t->name == NULL || t->partitions == NULL) {
        topic_destroy(t);
        return NULL;
    }

    for (int i = 0; i < num_partitions; i++) {
        partition_t* p = &t->partitions[i];
        p->partition_id = i;
        p->leader_rank = -1;
        p->num_replicas = replication_factor;
        p->replica_ranks = malloc((size_t)replication_factor * sizeof(int));
        p->isr_ranks = malloc((size_t)replication_factor * sizeof(int));

        if (p->replica_ranks == NULL || p->isr_ranks == NULL) {
            topic_destroy(t);
            return NULL;
        }

        // Sin asignar hasta que cluster_metadata reparta líderes y réplicas
        for (int r = 0; r < replication_factor; r++) {
            p->replica_ranks[r] = -1;
            p->isr_ranks[r] = -1;
        }
    }

    return t;
}

void topic_destroy(topic_t* t) {
    if (t == NULL) {
        return;
    }

    if (t->partitions != NULL) {
        for (int i = 0; i < t->num_partitions; i++) {
            if (t->partitions[i].log != NULL) {
                log_destroy(t->partitions[i].log);
            }

            free(t->partitions[i].replica_ranks);
            free(t->partitions[i].isr_ranks);
        }

        free(t->partitions);
    }

    free(t->name);
    free(t);
}

int topic_partition_for_key(topic_t* t, const char* key, int key_size) {
    if (t == NULL || t->num_partitions <= 0) {
        return -1;
    }

    // Sin clave no importa el orden entre mensajes, se reparten para que ninguna
    // partición reciba toda la carga
    if (key == NULL || key_size <= 0) {
        unsigned int turn = atomic_fetch_add(&t->next_partition, 1);
        return (int)(turn % (unsigned int)t->num_partitions);
    }

    // djb2: determinista, la misma clave da siempre el mismo número
    unsigned long hash = 5381;

    for (int i = 0; i < key_size; i++) {
        hash = hash * 33 + (unsigned char)key[i];
    }

    return (int)(hash % (unsigned long)t->num_partitions);
}

partition_t* topic_get_partition(topic_t* t, int partition_id) {
    if (t == NULL || partition_id < 0 || partition_id >= t->num_partitions) {
        return NULL;
    }

    return &t->partitions[partition_id];
}

int topic_open_logs(topic_t* t, int my_rank, const char* data_dir, const log_config_t* config) {
    if (t == NULL || data_dir == NULL) {
        return -1;
    }

    for (int i = 0; i < t->num_partitions; i++) {
        partition_t* p = &t->partitions[i];

        if (p->log != NULL || !partition_is_replica(p, my_rank)) {
            continue;
        }

        char dir[TOPIC_PATH_MAX];
        int n = snprintf(dir, sizeof(dir), "%s/%s-%d", data_dir, t->name, p->partition_id);

        if (n < 0 || (size_t)n >= sizeof(dir) || log_open(&p->log, dir, config) != LOG_OK) {
            p->log = NULL;
            return -1;
        }
    }

    return topic_update_roles(t, my_rank);
}

int topic_update_roles(topic_t* t, int my_rank) {
    if (t == NULL) {
        return -1;
    }

    int result = 0;

    for (int i = 0; i < t->num_partitions; i++) {
        if (partition_update_role(&t->partitions[i], my_rank) != 0) {
            result = -1;
        }
    }

    return result;
}

void topic_shutdown_logs(topic_t* t) {
    if (t == NULL) {
        return;
    }

    for (int i = 0; i < t->num_partitions; i++) {
        if (t->partitions[i].log != NULL) {
            log_shutdown(t->partitions[i].log);
        }
    }
}

bool partition_is_leader(const partition_t* p, int rank) {
    return p != NULL && rank >= 0 && p->leader_rank == rank;
}

bool partition_is_replica(const partition_t* p, int rank) {
    if (p == NULL || rank < 0) {
        return false;
    }

    for (int i = 0; i < p->num_replicas; i++) {
        if (p->replica_ranks[i] == rank) {
            return true;
        }
    }

    return false;
}

int partition_follower_index(const partition_t* p, int rank) {
    if (p == NULL || rank < 0 || rank == p->leader_rank) {
        return -1;
    }

    int index = 0;

    for (int i = 0; i < p->num_replicas; i++) {
        int replica = p->replica_ranks[i];

        if (replica < 0 || replica == p->leader_rank) {
            continue;
        }

        if (replica == rank) {
            return index;
        }

        index++;
    }

    return -1;
}

int partition_isr_count(const partition_t* p) {
    if (p == NULL) {
        return 0;
    }

    int count = 0;

    for (int i = 0; i < p->num_replicas; i++) {
        if (p->isr_ranks[i] >= 0) {
            count++;
        }
    }

    return count;
}
