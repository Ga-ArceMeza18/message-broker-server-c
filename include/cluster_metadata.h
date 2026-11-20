#ifndef CLUSTER_METADATA_H
#define CLUSTER_METADATA_H

#include <stdint.h>
#include <pthread.h>
#include "topic.h"

#define MD_MAX_REPLICAS    8
#define MD_MAX_TOPIC_NAME  64

typedef enum {
    MD_CREATE_TOPIC  = 1,
    MD_LEADER_CHANGE = 2,
    MD_ISR_CHANGE    = 3
} md_update_type_t;

/* Estructura plana (sin punteros) para poder enviarla tal cual por MPI
 * (por ejemplo como MPI_BYTE de sizeof(metadata_update_t)). */
typedef struct {
    uint64_t version;                    // versión resultante tras aplicar
    int      type;                       // md_update_type_t
    char     topic[MD_MAX_TOPIC_NAME];

    /* MD_CREATE_TOPIC */
    int      num_partitions;
    int      replication_factor;
    long     retention_bytes;
    int      num_brokers;                // para repetir el mismo reparto round-robin

    /* MD_LEADER_CHANGE / MD_ISR_CHANGE */
    int      partition_id;
    int      leader_rank;                // LEADER_CHANGE
    int      isr[MD_MAX_REPLICAS];       // ISR_CHANGE
    int      isr_count;
} metadata_update_t;

typedef struct {
    uint64_t         version;
    topic_t        **topics;
    int              num_topics;
    int              topics_capacity;
    pthread_rwlock_t lock;
} cluster_metadata_t;

/* Todas devuelven 0 en éxito y -1 en error, salvo que se indique otra cosa. */
int      cluster_metadata_init(cluster_metadata_t *m);
void     cluster_metadata_destroy(cluster_metadata_t *m);

int      create_topic(cluster_metadata_t *m, const char *name, int num_partitions, int replication_factor, int num_brokers);

/* Devuelve NULL si no existe. El puntero es válido mientras no exista delete_topic. */
topic_t *find_topic(cluster_metadata_t *m, const char *name);

/* 0 = aplicado, 1 = obsoleto (ignorado), -1 = error. */
int apply_update(cluster_metadata_t *m, const metadata_update_t *u);

#endif
