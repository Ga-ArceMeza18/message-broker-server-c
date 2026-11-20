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

// comm: flat structure to manage messages of cluster metadata updates
typedef struct {
    uint64_t version;                    // versión resultante tras aplicar
    int      type;                       // md_update_type_t
    char     topic[MD_MAX_TOPIC_NAME];

    /* create a topic  */
    int      num_partitions;
    int      replication_factor;
    long     retention_bytes;
    int      num_brokers;                

    /* MD_LEADER_CHANGE / MD_ISR_CHANGE */
    int      partition_id;
    int      leader_rank;                // val: LEADER_CHANGE
    int      isr[MD_MAX_REPLICAS];       // val: ISR_CHANGE
    int      isr_count;
} metadata_update_t;

typedef struct {
    uint64_t         version;
    topic_t        **topics;
    int              num_topics;
    int              topics_capacity;
    pthread_rwlock_t lock;
} cluster_metadata_t;

// comm: basic functions to manage cluster metadata
int cluster_metadata_init(cluster_metadata_t *m);
void cluster_metadata_destroy(cluster_metadata_t *m);

// comm: create a new topic in the cluster metadata
int create_topic(cluster_metadata_t *m, const char *name, int num_partitions, int replication_factor, int num_brokers);

// comm: find a topic by name in the cluster metadata
topic_t *find_topic(cluster_metadata_t *m, const char *name);

// comm: apply an update to the cluster metadata
int apply_update(cluster_metadata_t *m, const metadata_update_t *u);

#endif
