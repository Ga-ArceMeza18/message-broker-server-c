#define _POSIX_C_SOURCE 200809L

#include "broker.h"

#include <errno.h>
#include <openmpi/mpi.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define BROKER_POLL_MS 200
#define BROKER_BACKLOG 128

/* 
    UTILITIES     
*/

// utility: returns a string representation of the broker state
static const char *state_name(broker_state_t state) {
    
    switch (state) {
        case BROKER_STATE_INACTIVE:      return "INACTIVE";
        case BROKER_STATE_ACTIVE:        return "ACTIVE";
        case BROKER_STATE_PAUSED:        return "PAUSED";
        case BROKER_STATE_SHUTTING_DOWN: return "SHUTTING_DOWN";
    }
    return "UNKNOWN";
}

// utility: create directories recursively
static int make_directories(const char *path) {
    char *copy = strdup(path);
    if (copy == NULL) {
        return -1;
    }
    for (char *p = copy + 1; *p != '\0'; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(copy, 0755) != 0 && errno != EEXIST) {
                free(copy);
                return -1;
            }
            *p = '/';
        }
    }
    int rc = (mkdir(copy, 0755) != 0 && errno != EEXIST) ? -1 : 0;
    free(copy);
    return rc;
}

// utility: send a text message to a socket
static int send_text(int fd, const char *text) {
    size_t total = strlen(text);
    size_t sent = 0;
    while (sent < total) {
        ssize_t n = send(fd, text + sent, total - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

// utility: set state of broker with thread safety
broker_state_t broker_get_state(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    broker_state_t state = broker->state;
    pthread_mutex_unlock(&broker->state_lock);
    return state;
}

/* 
    OUTBOX UTILITIES  
*/

// out_ut: push a message to the outbox queue
static int outbox_push(broker_t *broker, int dest, int tag) {

    pthread_mutex_lock(&broker->outbox_lock);
    if (broker->outbox_count == BROKER_OUTBOX_CAPACITY) {
        pthread_mutex_unlock(&broker->outbox_lock);
        return -1;
    }
    broker->outbox[broker->outbox_count].dest = dest;
    broker->outbox[broker->outbox_count].tag = tag;
    broker->outbox_count++;
    pthread_mutex_unlock(&broker->outbox_lock);
    return 0;
}

// out_ut: send a control message to another broker via MPI
static int send_control(broker_t *broker, int dest, int tag) {
    if (!broker->mpi_active || dest < 0 || dest >= broker->world_size) {
        return -1;
    }
    return outbox_push(broker, dest, tag);
}

/* 
    STATE TRANSITIONS
*/

// state_tr: pause the broker
int broker_pause(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_ACTIVE) {
        pthread_mutex_unlock(&broker->state_lock);
        return -1;
    }
    broker->state = BROKER_STATE_PAUSED;
    pthread_cond_broadcast(&broker->state_changed);
    pthread_mutex_unlock(&broker->state_lock);
    return 0;
}

// state_tr: resume the broker
int broker_resume(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_PAUSED) {
        pthread_mutex_unlock(&broker->state_lock);
        return -1;
    }
    broker->state = BROKER_STATE_ACTIVE;
    pthread_cond_broadcast(&broker->state_changed);
    pthread_mutex_unlock(&broker->state_lock);
    return 0;
}

//  propagate: send a shutdown message to all other brokers.
//  state_tr: request shutdown of the broker and propagate to others
static void request_shutdown(broker_t *broker, bool propagate) {
    bool first = false;

    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_SHUTTING_DOWN) {
        broker->state = BROKER_STATE_SHUTTING_DOWN;
        first = true;
    }

    pthread_cond_broadcast(&broker->state_changed);
    pthread_mutex_unlock(&broker->state_lock);

    if (first && propagate && broker->mpi_active) {
        for (int rank = 0; rank < broker->world_size; rank++) {
            if (rank != broker->rank) {
                outbox_push(broker, rank, BROKER_TAG_SHUTDOWN);
            }
        }
    }
}

// state_tr: request shutdown of the broker and propagate to others
int broker_request_shutdown(broker_t *broker) {
    request_shutdown(broker, true);
    return 0;
}

/* 
    MPI THREAD
*/

// mpi_ut: flush the outbox queue and send messages via MPI
static void mpi_flush_outbox(broker_t *broker) {
    mpi_outgoing_t pending[BROKER_OUTBOX_CAPACITY];
    int count;

    pthread_mutex_lock(&broker->outbox_lock);
    count = broker->outbox_count;
    memcpy(pending, broker->outbox, (size_t)count * sizeof pending[0]);
    broker->outbox_count = 0;
    pthread_mutex_unlock(&broker->outbox_lock);

    for (int i = 0; i < count; i++) {
        int payload = 0;
        MPI_Send(&payload, 1, MPI_INT, pending[i].dest, pending[i].tag, MPI_COMM_WORLD);
    }
}

// mpi_ut: handle a received message based on its tag
static void mpi_handle_message(broker_t *broker, int tag) {
    switch (tag) {
        case BROKER_TAG_SHUTDOWN: request_shutdown(broker, false); break;
        case BROKER_TAG_PAUSE:    broker_pause(broker);            break;
        case BROKER_TAG_RESUME:   broker_resume(broker);           break;
        default:                                                   break;
    }
}

// mpi_ut: receive one message from MPI and optionally handle it
static bool mpi_receive_one(broker_t *broker, bool handle) {
    int flag = 0;
    MPI_Status status;

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
    if (!flag) {
        return false;
    }

    int payload = 0;
    MPI_Recv(&payload, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (handle) {
        mpi_handle_message(broker, status.MPI_TAG);
    }
    return true;
}

// mpi_ut: main loop of the MPI thread, flushing the outbox and receiving messages
static void *mpi_loop(void *arg) {
    broker_t *broker = arg;
    const struct timespec nap = {.tv_sec = 0, .tv_nsec = 1000000L};

    for (;;) {
        mpi_flush_outbox(broker);

        pthread_mutex_lock(&broker->outbox_lock);
        bool done = broker->outbox_stop && broker->outbox_count == 0;
        pthread_mutex_unlock(&broker->outbox_lock);
        if (done) {
            break;
        }

        if (!mpi_receive_one(broker, true)) {
            nanosleep(&nap, NULL);
        }
    }

    // comm: receive any remaining messages without handling them
    while (mpi_receive_one(broker, false)) {
    }
    return NULL;
}

/* 
    TCP_CLIENTS
*/

// tcp: process a single line of input from a client connection
static int process_line(broker_t *broker, int fd, char *line) {
    char *cr = strchr(line, '\r');
    if (cr != NULL) {
        *cr = '\0';
    }

    char cmd[16];
    int id = 0;
    int fields = sscanf(line, "%15s %d", cmd, &id);
    if (fields <= 0) {
        return 0;
    }

    broker_state_t state = broker_get_state(broker);
    if (state == BROKER_STATE_PAUSED &&
        strcmp(cmd, "RESUME") != 0 && strcmp(cmd, "SHUTDOWN") != 0) {
        return 0;  // broker en pausa: no responde
    }

    char reply[128];

    if (strcmp(cmd, "PING") == 0) {
        snprintf(reply, sizeof reply, "PONG %d\n", broker->rank);
        return send_text(fd, reply);
    }

    if (strcmp(cmd, "STATE") == 0) {
        snprintf(reply, sizeof reply, "STATE %s\n", state_name(state));
        return send_text(fd, reply);
    }

    if (strcmp(cmd, "PAUSE") == 0 || strcmp(cmd, "RESUME") == 0) {
        bool pausing = strcmp(cmd, "PAUSE") == 0;
        if (fields < 2) {
            return send_text(fd, "ERR usage: PAUSE|RESUME <broker_id>\n");
        }

        int rc;
        if (id == broker->rank) {
            rc = pausing ? broker_pause(broker) : broker_resume(broker);
        } else {
            rc = send_control(broker, id, pausing ? BROKER_TAG_PAUSE : BROKER_TAG_RESUME);
        }
        return send_text(fd, rc == 0 ? "OK\n" : "ERR invalid state or target\n");
    }

    if (strcmp(cmd, "SHUTDOWN") == 0) {
        broker_request_shutdown(broker);
        send_text(fd, "OK\n");
        return -1;
    }

    return send_text(fd, "ERR unknown command\n");
}

// tcp: handle a connection from a client   
static void handle_connection(broker_t *broker, int fd) {
    char buf[BROKER_LINE_MAX];
    size_t len = 0;
    bool open = true;

    while (open) {
        if (broker_get_state(broker) == BROKER_STATE_SHUTTING_DOWN) {
            send_text(fd, "ERR shutting down\n");
            break;
        }

        struct pollfd pfd = {.fd = fd, .events = POLLIN, .revents = 0};
        int ready = poll(&pfd, 1, BROKER_POLL_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0) {
            continue;
        }

        if (len == sizeof buf) {
            send_text(fd, "ERR line too long\n");
            break;
        }

        ssize_t n = recv(fd, buf + len, sizeof buf - len, 0);
        if (n == 0) {
            break;
        }
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }
            break;
        }
        len += (size_t)n;

        size_t start = 0;
        for (size_t i = 0; i < len && open; i++) {
            if (buf[i] == '\n') {
                buf[i] = '\0';
                if (process_line(broker, fd, buf + start) != 0) {
                    open = false;
                }
                start = i + 1;
            }
        }
        if (start > 0) {
            memmove(buf, buf + start, len - start);
            len -= start;
        }
    }

    close(fd);
}

// tcp: worker thread loop to handle client connections
static void *worker_loop(void *arg) {
    broker_t *broker = arg;
    int fd;

    while (request_queue_pop(broker->request_queue, &fd) == 0) {
        handle_connection(broker, fd);
    }
    return NULL;
}

// tcp: loop to accept new client connections
static void *acceptor_loop(void *arg) {
    broker_t *broker = arg;

    while (broker_get_state(broker) != BROKER_STATE_SHUTTING_DOWN) {
        struct pollfd pfd = {.fd = broker->listen_fd, .events = POLLIN, .revents = 0};
        int ready = poll(&pfd, 1, BROKER_POLL_MS);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0) {
            continue;
        }

        int client_fd = accept(broker->listen_fd, NULL, NULL);
        if (client_fd < 0) {
            continue;
        }
        if (request_queue_push(broker->request_queue, client_fd) != 0) {
            close(client_fd);
        }
    }
    return NULL;
}

// tcp: open a listening socket on the specified port
static int open_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) != 0 ||
        listen(fd, BROKER_BACKLOG) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* 
    LIFE CYCLE
*/

// life_cy: initialize the broker with the specified parameters
int broker_init(broker_t *broker, const char *name, const char *host, int port, int rank, const char *storage_path) {
    if (broker == NULL || name == NULL || host == NULL || storage_path == NULL) {
        return -1;
    }

    // comm: initialize broker structure and allocate memory for strings
    memset(broker, 0, sizeof *broker);
    broker->name = strdup(name);
    broker->host = strdup(host);
    broker->storage_path = strdup(storage_path);
    if (broker->name == NULL || broker->host == NULL || broker->storage_path == NULL) {
        free(broker->name);
        free(broker->host);
        free(broker->storage_path);
        return -1;
    }

    broker->port = port;
    broker->rank = rank;
    broker->listen_fd = -1;
    broker->num_workers = BROKER_DEFAULT_WORKERS;
    broker->state = BROKER_STATE_INACTIVE;

    if (make_directories(storage_path) != 0) {
        free(broker->name);
        free(broker->host);
        free(broker->storage_path);
        return -1;
    }

    if (pthread_mutex_init(&broker->state_lock, NULL) != 0) {
        goto fail_strings;
    }
    if (pthread_cond_init(&broker->state_changed, NULL) != 0) {
        goto fail_state_lock;
    }
    if (pthread_mutex_init(&broker->outbox_lock, NULL) != 0) {
        goto fail_state_changed;
    }
    return 0;

    fail_state_changed:
        pthread_cond_destroy(&broker->state_changed);
    fail_state_lock:
        pthread_mutex_destroy(&broker->state_lock);
    fail_strings:
        free(broker->name);
        free(broker->host);
        free(broker->storage_path);
        return -1;
}

// life_cy: run the broker, starting threads and listening for connections
int broker_run(broker_t *broker) {
    pthread_mutex_lock(&broker->state_lock);
    if (broker->state != BROKER_STATE_INACTIVE) {
        pthread_mutex_unlock(&broker->state_lock);
        return -1;
    }
    broker->state = BROKER_STATE_ACTIVE;
    pthread_mutex_unlock(&broker->state_lock);

    broker->request_queue = malloc(sizeof *broker->request_queue);
    if (broker->request_queue == NULL) {
        goto fail;
    }
    if (request_queue_init(broker->request_queue, BROKER_QUEUE_CAPACITY) != 0) {
        free(broker->request_queue);
        broker->request_queue = NULL;
        goto fail;
    }

    broker->listen_fd = open_listener(broker->port);
    if (broker->listen_fd < 0) {
        goto fail;
    }

    int mpi_initialized = 0;
    MPI_Initialized(&mpi_initialized);
    if (mpi_initialized) {
        MPI_Comm_size(MPI_COMM_WORLD, &broker->world_size);
        broker->mpi_active = true;
        if (pthread_create(&broker->mpi_thread, NULL, mpi_loop, broker) != 0) {
            goto fail;
        }
        broker->mpi_started = true;
    }

    broker->worker_threads = calloc((size_t)broker->num_workers, sizeof *broker->worker_threads);
    if (broker->worker_threads == NULL) {
        goto fail;
    }
    for (int i = 0; i < broker->num_workers; i++) {
        if (pthread_create(&broker->worker_threads[i], NULL, worker_loop, broker) != 0) {
            goto fail;
        }
        broker->workers_started++;
    }

    if (pthread_create(&broker->acceptor_thread, NULL, acceptor_loop, broker) != 0) {
        goto fail;
    }
    broker->acceptor_started = true;

    pthread_mutex_lock(&broker->state_lock);
    while (broker->state != BROKER_STATE_SHUTTING_DOWN) {
        pthread_cond_wait(&broker->state_changed, &broker->state_lock);
    }
    pthread_mutex_unlock(&broker->state_lock);
    return 0;

    fail:
        broker_shutdown(broker);
        return -1;
}

// life_cy: shutdown the broker, stopping threads and closing sockets
int broker_shutdown(broker_t *broker) {
    broker_request_shutdown(broker);

    if (broker->request_queue != NULL) {
        request_queue_close(broker->request_queue);
    }

    if (broker->acceptor_started) {
        pthread_join(broker->acceptor_thread, NULL);
        broker->acceptor_started = false;
    }

    for (int i = 0; i < broker->workers_started; i++) {
        pthread_join(broker->worker_threads[i], NULL);
    }
    broker->workers_started = 0;
    free(broker->worker_threads);
    broker->worker_threads = NULL;

    if (broker->listen_fd >= 0) {
        close(broker->listen_fd);
        broker->listen_fd = -1;
    }

    if (broker->mpi_started) {
        pthread_mutex_lock(&broker->outbox_lock);
        broker->outbox_stop = true;
        pthread_mutex_unlock(&broker->outbox_lock);
        pthread_join(broker->mpi_thread, NULL);
        broker->mpi_started = false;
    }

    if (broker->request_queue != NULL) {
        request_queue_destroy(broker->request_queue);
        free(broker->request_queue);
        broker->request_queue = NULL;
    }
    return 0;
}

// life_cy: destroy the broker, freeing resources and memory
void broker_destroy(broker_t *broker) {
    free(broker->name);
    free(broker->host);
    free(broker->storage_path);
    broker->name = NULL;
    broker->host = NULL;
    broker->storage_path = NULL;

    pthread_mutex_destroy(&broker->outbox_lock);
    pthread_cond_destroy(&broker->state_changed);
    pthread_mutex_destroy(&broker->state_lock);
}