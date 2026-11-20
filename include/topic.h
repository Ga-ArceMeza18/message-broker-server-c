#ifndef TOPIC_H
#define TOPIC_H

#include <stddef.h>

typedef struct partition_log partition_log_t;  

typedef struct {
    int partition_id;
    int leader_rank;
    int *replica_ranks;   // comm: size is equal to replication_factor
    int *isr_ranks; 
    
    partition_log_t *log;  // comm: null if leader_rank != my_rank
} partition_t;

typedef struct topic {
    char        *name;
    int          num_partitions;
    int          replication_factor;
    long         retention_bytes;
    partition_t *partitions;
} topic_t;

topic_t *topic_create(const char *name, int num_partitions, int replication_factor, long retention_bytes);
void topic_destroy(topic_t *t);
int topic_partition_for_key(const topic_t *t, const char *key, int key_size);
partition_t *topic_get_partition(topic_t *t, int partition_id);

#endif
