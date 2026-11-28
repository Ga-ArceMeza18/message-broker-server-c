#define _POSIX_C_SOURCE 200809L

#include "cluster_metadata.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAPACITY 8

/*
    HELPERS
*/

// helper: find a topic by name in the cluster metadata (assumes lock is held)
static topic_t *find_locked(const cluster_metadata_t *m, const char *name) {
    for (int i = 0; i < m->num_topics; i++)
        if (strcmp(m->topics[i]->name, name) == 0)
            return m->topics[i];
    return NULL;
}

// Valida que los brokers del ISR sean válidos y no estén duplicados.
static int validate_isr(const metadata_update_t *u) {
    if (!u || u->isr_count <= 0 || u->isr_count > MD_MAX_REPLICAS) {
        return -1;
    }

    for (int i = 0; i < u->isr_count; i++) {
        if (u->isr[i] < 0) {
            return -1;
        }

        for (int j = 0; j < i; j++) {
            if (u->isr[i] == u->isr[j]) {
                return -1;
            }
        }
    }

    return 0;
}

// helper: grow the topics array if needed (assumes lock is held)
static int grow_locked(cluster_metadata_t *m) {
    if (m->num_topics < m->topics_capacity) return 0;
    int new_cap = m->topics_capacity * 2;
    topic_t **tmp = realloc(m->topics, new_cap * sizeof(topic_t *));
    if (!tmp) return -1;
    m->topics = tmp;
    m->topics_capacity = new_cap;
    return 0;
}

// helper: create a topic and add it to the cluster metadata (assumes lock is held)
static int create_locked(cluster_metadata_t *m, const char *name,
                         int np, int rf, long retention, int num_brokers) {
    if (!name || np <= 0 || rf <= 0 || num_brokers <= 0) return -1;
    if (rf > num_brokers || rf > MD_MAX_REPLICAS)        return -1;
    if (strlen(name) >= MD_MAX_TOPIC_NAME)               return -1;
    if (find_locked(m, name))                            return -1;  // duplicado
    if (grow_locked(m) != 0)                             return -1;

    topic_t *t = topic_create(name, np, rf, retention);
    if (!t) return -1;

    for (int p = 0; p < np; p++) {
        partition_t *part = topic_get_partition(t, p);
        part->leader_rank = p % num_brokers;
        for (int r = 0; r < rf; r++) {
            part->replica_ranks[r] = (p + r) % num_brokers;
            part->isr_ranks[r]     = part->replica_ranks[r];  // ISR inicial = todas
        }
    }

    m->topics[m->num_topics++] = t;
    return 0;
}

/* 
    API pública
*/

// api: initialize the cluster metadata structure
int cluster_metadata_init(cluster_metadata_t *m) {
    if (!m) return -1;
    memset(m, 0, sizeof(*m));
    m->topics = calloc(INITIAL_CAPACITY, sizeof(topic_t *));
    if (!m->topics) return -1;
    m->topics_capacity = INITIAL_CAPACITY;
    if (pthread_rwlock_init(&m->lock, NULL) != 0) {
        free(m->topics);
        m->topics = NULL;
        return -1;
    }
    return 0;
}

// api: destroy the cluster metadata structure and free all resources
void cluster_metadata_destroy(cluster_metadata_t *m) {
    if (!m) return;
    for (int i = 0; i < m->num_topics; i++)
        topic_destroy(m->topics[i]);
    free(m->topics);
    m->topics = NULL;
    m->num_topics = m->topics_capacity = 0;
    pthread_rwlock_destroy(&m->lock);
}

// api: create a topic in the cluster metadata
int create_topic(cluster_metadata_t *m, const char *name, int num_partitions, int replication_factor, int num_brokers) {
    if (!m) return -1;
    pthread_rwlock_wrlock(&m->lock);
    int rc = create_locked(m, name, num_partitions, replication_factor, 0, num_brokers);
    if (rc == 0) m->version++;
    pthread_rwlock_unlock(&m->lock);
    return rc;
}

// api: find a topic by name in the cluster metadata
topic_t *find_topic(cluster_metadata_t *m, const char *name) {
    if (!m || !name) return NULL;
    pthread_rwlock_rdlock(&m->lock);
    topic_t *t = find_locked(m, name);
    pthread_rwlock_unlock(&m->lock);
    return t;
}

// api: apply a metadata update to the cluster metadata
int apply_update(cluster_metadata_t *m, const metadata_update_t *u) {
    if (!m || !u) return -1;

    // Validar que el nombre del tópico esté terminado correctamente.
    if (strnlen(u->topic, MD_MAX_TOPIC_NAME) >= MD_MAX_TOPIC_NAME) {
        return -1;
    }

    // Validar el tipo de actualización.
    if (u->type != MD_CREATE_TOPIC && u->type != MD_LEADER_CHANGE && u->type != MD_ISR_CHANGE) {
        return -1;
    }

    // Validar los datos específicos de cada actualización.
    if (u->type == MD_LEADER_CHANGE && u->leader_rank < 0) {
        return -1;
    }

    if (u->type == MD_ISR_CHANGE && validate_isr(u) != 0) {
        return -1;
    }

    pthread_rwlock_wrlock(&m->lock);

    if (u->version <= m->version) {       
        pthread_rwlock_unlock(&m->lock);
        return 1;
    }

    // No aplicar actualizaciones si falta una versión anterior.
    if (u->version != m->version + 1) {
        pthread_rwlock_unlock(&m->lock);
        return -1;
    }

    int rc = -1;
    switch (u->type) {
    case MD_CREATE_TOPIC:
        if (find_locked(m, u->topic)) {     
            rc = 0;
        } else {
            rc = create_locked(m, u->topic, u->num_partitions,
                               u->replication_factor, u->retention_bytes,
                               u->num_brokers);
        }
        break;

    case MD_LEADER_CHANGE: {
        topic_t *t = find_locked(m, u->topic);
        partition_t *p = t ? topic_get_partition(t, u->partition_id) : NULL;
        if (p && u->leader_rank >= 0) {
            p->leader_rank = u->leader_rank;
            rc = 0;
        }
        break;
    }

    case MD_ISR_CHANGE: {
        topic_t *t = find_locked(m, u->topic);
        partition_t *p = t ? topic_get_partition(t, u->partition_id) : NULL;

        if (p && u->isr_count <= t->replication_factor) {
            for (int i = 0; i < t->replication_factor; i++) {
                p->isr_ranks[i] = (i < u->isr_count) ? u->isr[i] : -1;
            }
            rc = 0;
        }
        break;
    }
    }

    if (rc == 0) m->version = u->version;
    pthread_rwlock_unlock(&m->lock);
    return rc;
}