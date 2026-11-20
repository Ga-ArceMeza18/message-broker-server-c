#ifndef BROKER_H
#define BROKER_H

#include <pthread.h>
#include <stdbool.h>

#include "request_queue.h"

#define BROKER_QUEUE_CAPACITY 128
#define BROKER_DEFAULT_WORKERS 4
#define BROKER_OUTBOX_CAPACITY 64
#define BROKER_LINE_MAX 1024

// comm: mpi tags to share information between brokers
enum {
    BROKER_TAG_SHUTDOWN = 100,
    BROKER_TAG_PAUSE = 101,
    BROKER_TAG_RESUME = 102
};

typedef enum {
    BROKER_STATE_INACTIVE,
    BROKER_STATE_ACTIVE,
    BROKER_STATE_PAUSED,
    BROKER_STATE_SHUTTING_DOWN
} broker_state_t;

typedef struct cluster_metadata cluster_metadata_t;

// message_config: struct to represent a message in the broker
// values: offset, timestamp, key_size, key, value_size, value
typedef struct {
    int offset;
    long timestamp;
    int key_size;
    char *key;
    int value_size;
    void *value;
} message_t;

// partition_config: basic data about a partition in the broker
// values: partition_id, leader_rank, replica_ranks, next_offset, highest_offset, storage_path
typedef struct {
    int partition_id;
    int leader_rank;
    int *replica_ranks;
    int next_offset;
    int highest_offset;
    char *storage_path;
} partition_t;

// mpi_config: manager of outgoing messages to other brokers via MPI
typedef struct {
    int dest;
    int tag;
} mpi_outgoing_t;

typedef struct {
    // comm: broker identification and connection info
    char *name;
    char *host;
    int port;
    char *username;
    char *password;

    int rank;
    char *storage_path;
    partition_t *partitions;
    int num_partitions;

    // comm: broker state and synchronization primitives
    broker_state_t state;
    pthread_mutex_t state_lock;
    pthread_cond_t state_changed;

    cluster_metadata_t *cluster_metadata;
    request_queue_t *request_queue;

    // comm: tcp server and worker threads
    int listen_fd;
    pthread_t acceptor_thread;
    bool acceptor_started;
    pthread_t *worker_threads;
    int num_workers;
    int workers_started;

    // mpi_config: information for calling MPI from the broker
    bool mpi_active;
    int world_size;
    pthread_t mpi_thread;
    bool mpi_started;
    pthread_mutex_t outbox_lock;
    mpi_outgoing_t outbox[BROKER_OUTBOX_CAPACITY];
    int outbox_count;
    bool outbox_stop;
    
} broker_t;

/*      BROKER FUNCTIONS        */
int broker_init(broker_t *broker, const char *name, const char *host, int port, int rank, const char *storage_path);
int broker_run(broker_t *broker);
int broker_pause(broker_t *broker);
int broker_resume(broker_t *broker);
int broker_request_shutdown(broker_t *broker);
int broker_shutdown(broker_t *broker);
void broker_destroy(broker_t *broker);
broker_state_t broker_get_state(broker_t *broker);

/*      PARTITION FUNCTIONS        */
int leader_of_partition(broker_t *broker, int partition_id, int N);
int get_partition_leader(broker_t *broker, int partition_id);
int replica_of_partition(broker_t *broker, int partition_id, int N);
int get_replica_of_partition(broker_t *broker, int partition_id, int replica_index);
int enroll_producer(broker_t *broker, char *producer_key);
int send_message(broker_t *broker, int partition_id, message_t *message);
int create_a_partition(partition_t *partition, int partition_id, int leader_rank, int *replica_ranks, int next_offset, int highest_offset, char *storage_path);

#endif
