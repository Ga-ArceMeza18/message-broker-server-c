#include "topic.h"
#include <stdlib.h>
#include <string.h>

topic_t *topic_create(const char *name, int np, int rf, long retention) {
    if (!name || np <= 0 || rf <= 0) return NULL;

    topic_t *t = calloc(1, sizeof(*t));
    if (!t) return NULL;

    t->name = strdup(name);
    t->num_partitions = np;
    t->replication_factor = rf;
    t->retention_bytes = retention;
    t->partitions = calloc(np, sizeof(partition_t));
    if (!t->name || !t->partitions) { topic_destroy(t); return NULL; }

    for (int i = 0; i < np; i++) {
        t->partitions[i].partition_id = i;
        t->partitions[i].leader_rank = -1;
        t->partitions[i].replica_ranks = calloc(rf, sizeof(int));
        t->partitions[i].isr_ranks = calloc(rf, sizeof(int));
        if (!t->partitions[i].replica_ranks || !t->partitions[i].isr_ranks) {
            topic_destroy(t);
            return NULL;
        }
    }
    return t;
}

void topic_destroy(topic_t *t) {
    if (!t) return;
    if (t->partitions) {
        for (int i = 0; i < t->num_partitions; i++) {
            free(t->partitions[i].replica_ranks);
            free(t->partitions[i].isr_ranks);
        }
        free(t->partitions);
    }
    free(t->name);
    free(t);
}

int topic_partition_for_key(const topic_t *t, const char *key, int key_size) {
    if (!key || key_size <= 0) return 0;
    unsigned long h = 5381;
    for (int i = 0; i < key_size; i++)
        h = h * 33 + (unsigned char)key[i];
    return (int)(h % t->num_partitions);
}

partition_t *topic_get_partition(topic_t *t, int id) {
    if (!t || id < 0 || id >= t->num_partitions) return NULL;
    return &t->partitions[id];
}
