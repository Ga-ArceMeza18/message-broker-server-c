#ifndef BROKER_H
#define BROKER_H

#include <pthread.h>
#include <stdbool.h>

#include "cluster_metadata.h"
#include "request_queue.h"
#include "consumer_group_t.h"
#include "failure_detector.h"

#define BROKER_QUEUE_CAPACITY 128
#define BROKER_DEFAULT_WORKERS 4
#define BROKER_OUTBOX_CAPACITY 64
#define BROKER_LINE_MAX 1024
#define BROKER_INITIAL_GROUPS 8
#define BROKER_COORDINATOR_RANK 0
#define BROKER_MAX_PENDING_METADATA 64
#define BROKER_REPLICA_RECORD_MAX 8192

// comm: mpi tags to share information between brokers
enum {
    BROKER_TAG_SHUTDOWN = 100,
    BROKER_TAG_PAUSE = 101,
    BROKER_TAG_RESUME = 102,
    BROKER_TAG_HEARTBEAT = 103,
    BROKER_TAG_METADATA = 104,
    BROKER_TAG_METADATA_ACK = 105,
    BROKER_TAG_REPLICA_APPEND = 106,
    BROKER_TAG_REPLICA_ACK = 107,
    BROKER_TAG_REPLICA_HW = 108,
    BROKER_TAG_REPLICA_SYNC = 109,
    BROKER_TAG_REPLICA_SYNC_REQUEST = 110
};

typedef struct {
    uint64_t version;
    int status;  // 0 = aplicado; -1 = error
} metadata_ack_t;

typedef struct {
    uint64_t version;
    int expected;
    int confirmed;
    int failed;
    bool active;
} metadata_ack_tracker_t;


typedef struct {
    char topic[MD_MAX_TOPIC_NAME];
    int partition_id;
    long offset;
    size_t record_size;
    unsigned char record[BROKER_REPLICA_RECORD_MAX];
} replica_append_t;

typedef struct {
    char topic[MD_MAX_TOPIC_NAME];
    int partition_id;
    long end_offset;
    int status;
} replica_ack_t;

typedef struct {
    char topic[MD_MAX_TOPIC_NAME];
    int partition_id;
    long high_watermark;
} replica_hw_t;

typedef struct {
    char topic[MD_MAX_TOPIC_NAME];
    int partition_id;
    long next_offset;
} replica_sync_t;

typedef enum {
    BROKER_STATE_INACTIVE,
    BROKER_STATE_ACTIVE,
    BROKER_STATE_PAUSED,
    BROKER_STATE_SHUTTING_DOWN
} broker_state_t;

// mpi_config: manager of outgoing messages to other brokers via MPI
typedef struct {
    int dest;
    int tag;
    metadata_update_t metadata;
    metadata_ack_t ack;
    replica_append_t replica_append;
    replica_ack_t replica_ack;
    replica_hw_t replica_hw;
    replica_sync_t replica_sync;
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

    // comm: broker state and synchronization primitives
    broker_state_t state;
    pthread_mutex_t state_lock;
    pthread_cond_t state_changed;

    cluster_metadata_t *cluster_metadata;
    request_queue_t *request_queue;

    // Serializa la creación de topics y apertura de sus logs.
    pthread_mutex_t topic_lock;

    pthread_mutex_t metadata_ack_lock;
    metadata_ack_tracker_t metadata_ack_trackers[BROKER_MAX_PENDING_METADATA];

    // consumer_groups: grupos de consumidores administrados por este broker
    consumer_group_t **consumer_groups;
    int num_consumer_groups;
    int consumer_groups_capacity;
    pthread_mutex_t groups_lock;

    // failure_detector: detector de disponibilidad de los otros brokers
    failure_detector_t failure_detector;
    bool failure_detector_initialized;

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

#endif
