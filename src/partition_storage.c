// partition_storage.c - Apertura y cierre de los logs de las particiones locales

#define _POSIX_C_SOURCE 200809L

#include "partition_storage.h"
#include "partition_log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

static int ensure_directory(const char *path) {
    if (mkdir(path, 0755) == 0) {
        return 0;
    }

    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            return 0;
        }
    }

    return -1;
}

static int is_local_replica(const topic_t *topic, const partition_t *partition, int local_rank) {
    for (int i = 0; i < topic->replication_factor; i++) {
        if (partition->replica_ranks[i] == local_rank) {
            return 1;
        }
    }

    return 0;
}

static int open_partition(partition_t *partition, const topic_t *topic, const char *topic_path, int local_rank) {
    if (!is_local_replica(topic, partition, local_rank)) {
        return 0;
    }

    if (partition->log != NULL) {
        return 0;
    }

    int needed = snprintf(NULL, 0, "%s/partition-%d", topic_path, partition->partition_id);

    if (needed < 0) {
        return -1;
    }

    char *path = malloc((size_t)needed + 1);
    if (!path) {
        return -1;
    }

    snprintf(path, (size_t)needed + 1, "%s/partition-%d", topic_path, partition->partition_id);

    if (ensure_directory(path) != 0) {
        free(path);
        return -1;
    }

    partition_log_t *log = NULL;

    if (log_open(&log, path, NULL) != LOG_OK) {
        free(path);
        return -1;
    }

    free(path);

    if (partition->leader_rank == local_rank) {
        if (log_set_leader(log, topic->replication_factor - 1) != LOG_OK) {
            log_destroy(log);
            return -1;
        }
    } else {
        log_set_follower(log);
    }

    partition->log = log;
    return 0;
}

int partition_storage_open(cluster_metadata_t *metadata, const char *storage_path, int local_rank) {
    if (!metadata || !storage_path ||
        storage_path[0] == '\0' || local_rank < 0) {
        return -1;
    }

    if (ensure_directory(storage_path) != 0) {
        return -1;
    }

    pthread_rwlock_wrlock(&metadata->lock);

    for (int i = 0; i < metadata->num_topics; i++) {
        topic_t *topic = metadata->topics[i];

        // Los nombres de topics no deben ser rutas.
        if (!topic || !topic->name || topic->name[0] == '\0' ||
            strcmp(topic->name, ".") == 0 || strcmp(topic->name, "..") == 0 ||
            strchr(topic->name, '/') != NULL || strchr(topic->name, '\\') != NULL) {
            pthread_rwlock_unlock(&metadata->lock);
            return -1;
        }

        int needed = snprintf(NULL, 0, "%s/%s", storage_path, topic->name);

        if (needed < 0) {
            pthread_rwlock_unlock(&metadata->lock);
            return -1;
        }

        char *topic_path = malloc((size_t)needed + 1);
        if (!topic_path) {
            pthread_rwlock_unlock(&metadata->lock);
            return -1;
        }

        snprintf(topic_path, (size_t)needed + 1, "%s/%s", storage_path, topic->name);

        if (ensure_directory(topic_path) != 0) {
            free(topic_path);
            pthread_rwlock_unlock(&metadata->lock);
            return -1;
        }

        for (int p = 0; p < topic->num_partitions; p++) {
            if (open_partition(&topic->partitions[p], topic, topic_path, local_rank) != 0) {
                free(topic_path);
                pthread_rwlock_unlock(&metadata->lock);
                return -1;
            }
        }

        free(topic_path);
    }

    pthread_rwlock_unlock(&metadata->lock);
    return 0;
}

void partition_storage_close(cluster_metadata_t *metadata) {
    if (!metadata) {
        return;
    }

    pthread_rwlock_wrlock(&metadata->lock);

    for (int i = 0; i < metadata->num_topics; i++) {
        topic_t *topic = metadata->topics[i];

        for (int p = 0; p < topic->num_partitions; p++) {
            partition_t *partition = &topic->partitions[p];

            if (partition->log != NULL) {
                log_shutdown(partition->log);
                log_destroy(partition->log);
                partition->log = NULL;
            }
        }
    }

    pthread_rwlock_unlock(&metadata->lock);
}
