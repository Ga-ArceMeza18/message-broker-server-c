#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "failure_detector.h"

typedef struct {
    pthread_mutex_t lock;
    int sent[FD_MAX_BROKERS];
    int down_events[FD_MAX_BROKERS];
    int up_events[FD_MAX_BROKERS];
    failure_detector_t *fd;
    bool query_inside_callback;
} recorder_t;

// comm: reset the recorder before each scenario
static void recorder_reset(recorder_t *r, failure_detector_t *fd) {
    memset(r, 0, sizeof *r);
    pthread_mutex_init(&r->lock, NULL);
    r->fd = fd;
}

// comm: callback that counts heartbeats sent to each rank
static void on_send(int dest, void *ctx) {
    recorder_t *r = ctx;
    pthread_mutex_lock(&r->lock);
    r->sent[dest]++;
    pthread_mutex_unlock(&r->lock);
}

// comm: callback that counts down events, and queries the detector to prove there is no deadlock
static void on_down(int rank, void *ctx) {
    recorder_t *r = ctx;
    if (r->query_inside_callback) {
        assert(failure_detector_is_down(r->fd, rank));
    }
    pthread_mutex_lock(&r->lock);
    r->down_events[rank]++;
    pthread_mutex_unlock(&r->lock);
}

// comm: callback that counts up events
static void on_up(int rank, void *ctx) {
    recorder_t *r = ctx;
    pthread_mutex_lock(&r->lock);
    r->up_events[rank]++;
    pthread_mutex_unlock(&r->lock);
}

// comm: read one counter of the recorder with the lock held
static int read_counter(recorder_t *r, const int *counter) {
    pthread_mutex_lock(&r->lock);
    int value = *counter;
    pthread_mutex_unlock(&r->lock);
    return value;
}

// comm: sleep for the given number of milliseconds
static void sleep_ms(long ms) {
    struct timespec ts = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

// comm: invalid arguments must be rejected by init
static void test_init_validation(void) {
    failure_detector_t fd;
    assert(failure_detector_init(NULL, 0, 3, 10, 50, NULL, NULL, NULL, NULL) == -1);
    assert(failure_detector_init(&fd, 0, 0, 10, 50, NULL, NULL, NULL, NULL) == -1);
    assert(failure_detector_init(&fd, 0, FD_MAX_BROKERS + 1, 10, 50, NULL, NULL, NULL, NULL) == -1);
    assert(failure_detector_init(&fd, 3, 3, 10, 50, NULL, NULL, NULL, NULL) == -1);
    assert(failure_detector_init(&fd, -1, 3, 10, 50, NULL, NULL, NULL, NULL) == -1);
    assert(failure_detector_init(&fd, 0, 3, 0, 50, NULL, NULL, NULL, NULL) == -1);
    assert(failure_detector_init(&fd, 0, 3, 50, 50, NULL, NULL, NULL, NULL) == -1);

    assert(failure_detector_init(&fd, 0, 3, 10, 50, NULL, NULL, NULL, NULL) == 0);
    failure_detector_destroy(&fd);
}

// comm: heartbeats go to every peer and never to ourselves
static void test_send_heartbeats(void) {
    failure_detector_t fd;
    recorder_t r;
    recorder_reset(&r, &fd);
    assert(failure_detector_init(&fd, 1, 4, 10, 50, on_send, on_down, on_up, &r) == 0);

    send_heartbeats(&fd);
    send_heartbeats(&fd);
    assert(r.sent[0] == 2 && r.sent[2] == 2 && r.sent[3] == 2);
    assert(r.sent[1] == 0);

    mark_broker_down(&fd, 2);
    send_heartbeats(&fd);
    assert(r.sent[2] == 3);

    failure_detector_destroy(&fd);
}

// comm: a silent broker is marked down once, only after the timeout, and never ourselves
static void test_timeouts(void) {
    failure_detector_t fd;
    recorder_t r;
    recorder_reset(&r, &fd);
    r.query_inside_callback = true;
    assert(failure_detector_init(&fd, 0, 3, 10, 100, on_send, on_down, on_up, &r) == 0);

    long t0 = failure_detector_now_ms();
    assert(heartbeat_received(&fd, 1, t0) == 0);
    assert(heartbeat_received(&fd, 2, t0) == 0);

    assert(check_timeouts(&fd, t0 + 100) == 0);
    assert(!failure_detector_is_down(&fd, 1));

    assert(heartbeat_received(&fd, 1, t0 + 90) == 0);
    assert(check_timeouts(&fd, t0 + 101) == 1);
    assert(failure_detector_is_down(&fd, 2));
    assert(!failure_detector_is_down(&fd, 1));
    assert(r.down_events[2] == 1);

    assert(check_timeouts(&fd, t0 + 150) == 0);
    assert(r.down_events[2] == 1);

    assert(check_timeouts(&fd, t0 + 1000) == 1);
    assert(failure_detector_is_down(&fd, 1));
    assert(!failure_detector_is_down(&fd, 0));
    assert(r.down_events[0] == 0);

    failure_detector_destroy(&fd);
}

// comm: a heartbeat from a broker marked down revives it and fires on_up once
static void test_recovery(void) {
    failure_detector_t fd;
    recorder_t r;
    recorder_reset(&r, &fd);
    assert(failure_detector_init(&fd, 0, 3, 10, 100, on_send, on_down, on_up, &r) == 0);

    long t0 = failure_detector_now_ms();
    assert(check_timeouts(&fd, t0 + 1000) == 2);
    assert(failure_detector_is_down(&fd, 1));

    assert(heartbeat_received(&fd, 1, t0 + 1001) == 0);
    assert(!failure_detector_is_down(&fd, 1));
    assert(r.up_events[1] == 1);

    assert(heartbeat_received(&fd, 1, t0 + 1002) == 0);
    assert(r.up_events[1] == 1);

    assert(check_timeouts(&fd, t0 + 1050) == 0);
    assert(check_timeouts(&fd, t0 + 1103) == 1);
    assert(failure_detector_is_down(&fd, 1));
    assert(r.down_events[1] == 2);

    failure_detector_destroy(&fd);
}

// comm: manual mark down/up are idempotent and reject invalid ranks
static void test_manual_marks(void) {
    failure_detector_t fd;
    recorder_t r;
    recorder_reset(&r, &fd);
    assert(failure_detector_init(&fd, 1, 3, 10, 100, on_send, on_down, on_up, &r) == 0);

    assert(mark_broker_down(&fd, 0) == 1);
    assert(mark_broker_down(&fd, 0) == 0);
    assert(r.down_events[0] == 1);

    assert(mark_broker_up(&fd, 0, failure_detector_now_ms()) == 1);
    assert(mark_broker_up(&fd, 0, failure_detector_now_ms()) == 0);
    assert(r.up_events[0] == 1);

    assert(mark_broker_down(&fd, 1) == -1);
    assert(mark_broker_down(&fd, 3) == -1);
    assert(mark_broker_down(&fd, -1) == -1);
    assert(mark_broker_up(&fd, 1, 0) == -1);
    assert(heartbeat_received(&fd, 1, 0) == -1);
    assert(heartbeat_received(&fd, 7, 0) == -1);
    assert(!failure_detector_is_down(&fd, 7));

    failure_detector_destroy(&fd);
}

// comm: the background thread detects a silent broker while a live one keeps sending heartbeats
static void test_background_thread(void) {
    failure_detector_t fd;
    recorder_t r;
    recorder_reset(&r, &fd);
    r.query_inside_callback = true;
    assert(failure_detector_init(&fd, 0, 3, 10, 80, on_send, on_down, on_up, &r) == 0);

    assert(failure_detector_start(&fd) == 0);
    assert(failure_detector_start(&fd) == -1);

    for (int i = 0; i < 30; i++) {
        heartbeat_received(&fd, 1, failure_detector_now_ms());
        sleep_ms(10);
    }

    assert(failure_detector_is_down(&fd, 2));
    assert(!failure_detector_is_down(&fd, 1));
    assert(read_counter(&r, &r.down_events[2]) == 1);
    assert(read_counter(&r, &r.down_events[1]) == 0);
    assert(read_counter(&r, &r.sent[1]) > 0);
    assert(read_counter(&r, &r.sent[2]) > 0);
    assert(read_counter(&r, &r.sent[0]) == 0);

    heartbeat_received(&fd, 2, failure_detector_now_ms());
    assert(!failure_detector_is_down(&fd, 2));
    assert(read_counter(&r, &r.up_events[2]) == 1);

    failure_detector_stop(&fd);
    int sent_after_stop = read_counter(&r, &r.sent[1]);
    sleep_ms(50);
    assert(read_counter(&r, &r.sent[1]) == sent_after_stop);

    assert(failure_detector_start(&fd) == 0);
    failure_detector_destroy(&fd);
}

// comm: run every scenario
int main(void) {
    test_init_validation();
    test_send_heartbeats();
    test_timeouts();
    test_recovery();
    test_manual_marks();
    test_background_thread();
    printf("OK: todos los tests pasaron\n");
    return 0;
}
