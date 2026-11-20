#ifndef REQUEST_QUEUE_H
#define REQUEST_QUEUE_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>

// comm: struct to represent a thread-safe request queue
typedef struct {
    int *items;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    bool closed;
    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} request_queue_t;

// func: initialize a request queue with the specified capacity
int request_queue_init(request_queue_t *queue, size_t capacity);

// func: push a file descriptor into the request queue
int request_queue_push(request_queue_t *queue, int fd);

// func: pop a file descriptor from the request queue
int request_queue_pop(request_queue_t *queue, int *fd);

// func: close the request queue, preventing new pushes and waking up all blocked threads
void request_queue_close(request_queue_t *queue);

// func: destroy the request queue, freeing all resources
void request_queue_destroy(request_queue_t *queue);

#endif
