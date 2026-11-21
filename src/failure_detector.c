#define _POSIX_C_SOURCE 200809L

#include "failure_detector.h"

#include <string.h>
#include <time.h>

// comm: check that rank is a valid peer (in range and not ourselves)
static bool valid_peer(const failure_detector_t *fd, int rank) {
    return rank >= 0 && rank < fd->num_brokers && rank != fd->self_rank;
}

// comm: build the absolute deadline (monotonic clock) for the next tick
static struct timespec next_deadline(long interval_ms) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += interval_ms / 1000;
    ts.tv_nsec += (interval_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    return ts;
}

// comm: background loop, send heartbeats and check timeouts every interval
static void *detector_loop(void *arg) {
    failure_detector_t *fd = arg;

    pthread_mutex_lock(&fd->lock);
    while (!fd->stop) {
        pthread_mutex_unlock(&fd->lock);
        send_heartbeats(fd);
        check_timeouts(fd, failure_detector_now_ms());
        pthread_mutex_lock(&fd->lock);

        if (fd->stop) {
            break;
        }
        struct timespec deadline = next_deadline(fd->interval_ms);
        pthread_cond_timedwait(&fd->wake, &fd->lock, &deadline);
    }
    pthread_mutex_unlock(&fd->lock);
    return NULL;
}

// comm: monotonic clock in milliseconds used for every timestamp of the detector
long failure_detector_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

// comm: initialize the detector, every peer starts as up with a full timeout of grace
int failure_detector_init(failure_detector_t *fd, int self_rank, int num_brokers, long interval_ms, long timeout_ms, fd_send_fn send_heartbeat, fd_event_fn on_down,
    fd_event_fn on_up, void *ctx) {
    if (fd == NULL || num_brokers < 1 || num_brokers > FD_MAX_BROKERS ||
        self_rank < 0 || self_rank >= num_brokers ||
        interval_ms <= 0 || timeout_ms <= interval_ms) {
        return -1;
    }

    memset(fd, 0, sizeof *fd);
    fd->self_rank = self_rank;
    fd->num_brokers = num_brokers;
    fd->interval_ms = interval_ms;
    fd->timeout_ms = timeout_ms;
    fd->send_heartbeat = send_heartbeat;
    fd->on_down = on_down;
    fd->on_up = on_up;
    fd->ctx = ctx;

    long now = failure_detector_now_ms();
    for (int i = 0; i < num_brokers; i++) {
        fd->last_seen[i] = now;
    }

    if (pthread_mutex_init(&fd->lock, NULL) != 0) {
        return -1;
    }

    pthread_condattr_t attr;
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    int rc = pthread_cond_init(&fd->wake, &attr);
    pthread_condattr_destroy(&attr);
    if (rc != 0) {
        pthread_mutex_destroy(&fd->lock);
        return -1;
    }
    return 0;
}

// comm: stop the background thread if needed and free the detector resources
void failure_detector_destroy(failure_detector_t *fd) {
    if (fd == NULL) {
        return;
    }
    failure_detector_stop(fd);
    pthread_cond_destroy(&fd->wake);
    pthread_mutex_destroy(&fd->lock);
}

// comm: start the background thread that sends heartbeats and checks timeouts
int failure_detector_start(failure_detector_t *fd) {
    if (fd == NULL) {
        return -1;
    }

    pthread_mutex_lock(&fd->lock);
    if (fd->running) {
        pthread_mutex_unlock(&fd->lock);
        return -1;
    }
    fd->running = true;
    fd->stop = false;
    pthread_mutex_unlock(&fd->lock);

    if (pthread_create(&fd->thread, NULL, detector_loop, fd) != 0) {
        pthread_mutex_lock(&fd->lock);
        fd->running = false;
        pthread_mutex_unlock(&fd->lock);
        return -1;
    }
    return 0;
}

// comm: stop the background thread and wait for it to finish
void failure_detector_stop(failure_detector_t *fd) {
    if (fd == NULL) {
        return;
    }

    pthread_mutex_lock(&fd->lock);
    if (!fd->running) {
        pthread_mutex_unlock(&fd->lock);
        return;
    }
    fd->stop = true;
    pthread_cond_broadcast(&fd->wake);
    pthread_mutex_unlock(&fd->lock);

    pthread_join(fd->thread, NULL);

    pthread_mutex_lock(&fd->lock);
    fd->running = false;
    fd->stop = false;
    pthread_mutex_unlock(&fd->lock);
}

// comm: send a heartbeat to every other broker, including the ones marked down
void send_heartbeats(failure_detector_t *fd) {
    if (fd == NULL || fd->send_heartbeat == NULL) {
        return;
    }
    for (int rank = 0; rank < fd->num_brokers; rank++) {
        if (rank != fd->self_rank) {
            fd->send_heartbeat(rank, fd->ctx);
        }
    }
}

// comm: register a heartbeat received from rank, revives it if it was down
int heartbeat_received(failure_detector_t *fd, int rank, long now_ms) {
    if (fd == NULL || !valid_peer(fd, rank)) {
        return -1;
    }

    pthread_mutex_lock(&fd->lock);
    fd->last_seen[rank] = now_ms;
    bool revived = fd->down[rank];
    fd->down[rank] = false;
    pthread_mutex_unlock(&fd->lock);

    if (revived && fd->on_up != NULL) {
        fd->on_up(rank, fd->ctx);
    }
    return 0;
}

// comm: mark as down every broker silent for more than timeout_ms, returns how many changed
int check_timeouts(failure_detector_t *fd, long now_ms) {
    if (fd == NULL) {
        return -1;
    }

    int changed[FD_MAX_BROKERS];
    int count = 0;

    pthread_mutex_lock(&fd->lock);
    for (int rank = 0; rank < fd->num_brokers; rank++) {
        if (rank == fd->self_rank || fd->down[rank]) {
            continue;
        }
        if (now_ms - fd->last_seen[rank] > fd->timeout_ms) {
            fd->down[rank] = true;
            changed[count++] = rank;
        }
    }
    pthread_mutex_unlock(&fd->lock);

    if (fd->on_down != NULL) {
        for (int i = 0; i < count; i++) {
            fd->on_down(changed[i], fd->ctx);
        }
    }
    return count;
}

// comm: mark a broker as down, returns 1 if it changed, 0 if already down, -1 if invalid
int mark_broker_down(failure_detector_t *fd, int rank) {
    if (fd == NULL || !valid_peer(fd, rank)) {
        return -1;
    }

    pthread_mutex_lock(&fd->lock);
    bool changed = !fd->down[rank];
    fd->down[rank] = true;
    pthread_mutex_unlock(&fd->lock);

    if (changed && fd->on_down != NULL) {
        fd->on_down(rank, fd->ctx);
    }
    return changed ? 1 : 0;
}

// comm: mark a broker as up, returns 1 if it changed, 0 if already up, -1 if invalid
int mark_broker_up(failure_detector_t *fd, int rank, long now_ms) {
    if (fd == NULL || !valid_peer(fd, rank)) {
        return -1;
    }

    pthread_mutex_lock(&fd->lock);
    bool changed = fd->down[rank];
    fd->down[rank] = false;
    fd->last_seen[rank] = now_ms;
    pthread_mutex_unlock(&fd->lock);

    if (changed && fd->on_up != NULL) {
        fd->on_up(rank, fd->ctx);
    }
    return changed ? 1 : 0;
}

// comm: return true if the broker is currently marked as down
bool failure_detector_is_down(failure_detector_t *fd, int rank) {
    if (fd == NULL || rank < 0 || rank >= fd->num_brokers) {
        return false;
    }

    pthread_mutex_lock(&fd->lock);
    bool down = fd->down[rank];
    pthread_mutex_unlock(&fd->lock);
    return down;
}
