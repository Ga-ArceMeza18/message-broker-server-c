#ifndef FAILURE_DETECTOR_H
#define FAILURE_DETECTOR_H

#include <pthread.h>
#include <stdbool.h>

#define FD_MAX_BROKERS 64

// comm: callback used to send one heartbeat to the broker with rank dest_rank
typedef void (*fd_send_fn)(int dest_rank, void *ctx);

// comm: callback fired when a broker is marked down or up
typedef void (*fd_event_fn)(int rank, void *ctx);

typedef struct {
    int self_rank;
    int num_brokers;
    long interval_ms;
    long timeout_ms;
    long last_seen[FD_MAX_BROKERS];
    bool down[FD_MAX_BROKERS];

    fd_send_fn send_heartbeat;
    fd_event_fn on_down;
    fd_event_fn on_up;
    void *ctx;

    pthread_mutex_t lock;
    pthread_cond_t wake;
    pthread_t thread;
    bool running;
    bool stop;
} failure_detector_t;

// comm: initialize the detector, every peer starts as up with a full timeout of grace
int failure_detector_init(failure_detector_t *fd, int self_rank, int num_brokers,long interval_ms, long timeout_ms, fd_send_fn send_heartbeat, fd_event_fn on_down,
fd_event_fn on_up, void *ctx);

// comm: stop the background thread if needed and free the detector resources
void failure_detector_destroy(failure_detector_t *fd);

// comm: start the background thread that sends heartbeats and checks timeouts
int failure_detector_start(failure_detector_t *fd);

// comm: stop the background thread and wait for it to finish
void failure_detector_stop(failure_detector_t *fd);

// comm: monotonic clock in milliseconds used for every timestamp of the detector
long failure_detector_now_ms(void);

// comm: send a heartbeat to every other broker, including the ones marked down
void send_heartbeats(failure_detector_t *fd);

// comm: register a heartbeat received from rank, revives it if it was down
int heartbeat_received(failure_detector_t *fd, int rank, long now_ms);

// comm: mark as down every broker silent for more than timeout_ms, returns how many changed
int check_timeouts(failure_detector_t *fd, long now_ms);

// comm: mark a broker as down, returns 1 if it changed, 0 if already down, -1 if invalid
int mark_broker_down(failure_detector_t *fd, int rank);

// comm: mark a broker as up, returns 1 if it changed, 0 if already up, -1 if invalid
int mark_broker_up(failure_detector_t *fd, int rank, long now_ms);

// comm: return true if the broker is currently marked as down
bool failure_detector_is_down(failure_detector_t *fd, int rank);

#endif
