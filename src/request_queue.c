#include "request_queue.h"

#include <stdlib.h>

// request_queue_init: initialize a request queue with the specified capacity
int request_queue_init(request_queue_t *queue, size_t capacity) {
    if (queue == NULL || capacity == 0) {
        return -1;
    }

    queue->items = malloc(capacity * sizeof *queue->items);
    if (queue->items == NULL) {
        return -1;
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->closed = false;

    if (pthread_mutex_init(&queue->lock, NULL) != 0) {
        free(queue->items);
        return -1;
    }
    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_mutex_destroy(&queue->lock);
        free(queue->items);
        return -1;
    }
    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_cond_destroy(&queue->not_full);
        pthread_mutex_destroy(&queue->lock);
        free(queue->items);
        return -1;
    }
    return 0;
}

// request_queue_push: push a file descriptor into the request queue
int request_queue_push(request_queue_t *queue, int fd) {
    pthread_mutex_lock(&queue->lock);
    while (queue->count == queue->capacity && !queue->closed) {
        pthread_cond_wait(&queue->not_full, &queue->lock);
    }
    if (queue->closed) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }

    queue->items[queue->tail] = fd;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;

    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

// request_queue_pop: pop a file descriptor from the request queue
int request_queue_pop(request_queue_t *queue, int *fd) {
    pthread_mutex_lock(&queue->lock);
    while (queue->count == 0 && !queue->closed) {
        pthread_cond_wait(&queue->not_empty, &queue->lock);
    }
    if (queue->count == 0) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }

    *fd = queue->items[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

// request_queue_close: close the request queue, preventing new pushes and waking up all blocked threads
void request_queue_close(request_queue_t *queue) {
    pthread_mutex_lock(&queue->lock);
    queue->closed = true;
    pthread_cond_broadcast(&queue->not_full);
    pthread_cond_broadcast(&queue->not_empty);
    pthread_mutex_unlock(&queue->lock);
}

// request_queue_destroy: destroy the request queue, freeing all resources
void request_queue_destroy(request_queue_t *queue) {
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
    pthread_mutex_destroy(&queue->lock);
    free(queue->items);
    queue->items = NULL;
}