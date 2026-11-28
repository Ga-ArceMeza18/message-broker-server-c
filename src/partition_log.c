// partition_log.c - Log de una partición en segmentos, con offsets y high watermark

#define _POSIX_C_SOURCE 200809L

#include "partition_log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define LOG_PATH_MAX 512
#define SEGMENT_DIGITS 20
#define SEGMENT_SUFFIX ".log"
#define CHECKPOINT_NAME "hw.checkpoint"
#define INITIAL_CAPACITY 64

// Un segmento: un archivo y dónde empieza cada uno de sus mensajes
typedef struct {
    long base_offset; // offset del primer mensaje del archivo
    int fd;
    long size; // bytes válidos del archivo
    long* positions; // positions[i] es donde empieza el mensaje base_offset + i
    long count;
    long capacity;
} segment_t;

// El orden de los locks es primero lock y después segments_lock
struct partition_log {
    char* dir;
    log_config_t config;

    // lock protege los offsets, el high watermark, las réplicas y closed.
    // También hace que las escrituras vayan de una en una
    pthread_mutex_t lock;
    pthread_cond_t hw_advanced;
    long next_offset;
    long high_watermark;
    long log_start_offset;
    bool closed;

    // Solo en el líder. Hasta dónde tiene cada réplica y si está sincronizada
    bool is_leader;
    int num_replicas;
    long* replica_end_offsets;
    bool* replica_in_sync;

    // segments_lock protege el arreglo de segmentos y sus índices. Los lectores
    // lo toman en modo lectura y leen del disco sin frenar a los escritores
    pthread_rwlock_t segments_lock;
    segment_t* segments;
    int num_segments;
    int segments_capacity;
};

// Funciones privadas

// Lee hasta size bytes desde position. Devuelve los bytes leídos o -1
static ssize_t read_upto(int fd, unsigned char* buf, size_t size, long position) {
    size_t done = 0;

    while (done < size) {
        ssize_t n = pread(fd, buf + done, size - done, (off_t)(position + (long)done));

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }

        if (n == 0) {
            break; // se llegó al final del archivo
        }

        done += (size_t)n;
    }

    return (ssize_t)done;
}

// Escribe todo el búfer aunque write() lo haga por partes
static int write_all(int fd, const unsigned char* buf, size_t size) {
    size_t done = 0;

    while (done < size) {
        ssize_t n = write(fd, buf + done, size - done);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }

        done += (size_t)n;
    }

    return 0;
}

// Crea el directorio y los que falten
static int make_dirs(const char* path) {
    char tmp[LOG_PATH_MAX];
    int n = snprintf(tmp, sizeof(tmp), "%s", path);

    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        return -1;
    }

    for (char* p = tmp + 1; *p != '\0'; p++) {
        if (*p == '/') {
            *p = '\0';

            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
                return -1;
            }

            *p = '/';
        }
    }

    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
        return -1;
    }

    return 0;
}

// fsync del directorio, para que un archivo nuevo o renombrado no se pierda
static void fsync_dir(const char* dir) {
    int fd = open(dir, O_RDONLY);

    if (fd >= 0) {
        fsync(fd);
        close(fd);
    }
}

// Arma la ruta de un archivo dentro del directorio del log
static int log_path(const partition_log_t* log, const char* name, char* out, size_t size) {
    int n = snprintf(out, size, "%s/%s", log->dir, name);

    return (n < 0 || (size_t)n >= size) ? -1 : 0;
}

// Arma la ruta del segmento que empieza en base_offset
static int segment_path(const partition_log_t* log, long base_offset, char* out, size_t size) {
    int n = snprintf(out, size, "%s/%0*ld%s", log->dir, SEGMENT_DIGITS, base_offset, SEGMENT_SUFFIX);

    return (n < 0 || (size_t)n >= size) ? -1 : 0;
}

// Revisa si el nombre es de un segmento (20 dígitos + .log) y saca su offset
static bool parse_segment_name(const char* name, long* base_offset) {
    if (strlen(name) != SEGMENT_DIGITS + strlen(SEGMENT_SUFFIX)
        || strcmp(name + SEGMENT_DIGITS, SEGMENT_SUFFIX) != 0) {
        return false;
    }

    long value = 0;

    for (int i = 0; i < SEGMENT_DIGITS; i++) {
        if (name[i] < '0' || name[i] > '9') {
            return false;
        }

        int digit = name[i] - '0';

        if (value > (LONG_MAX - digit) / 10) {
            return false;
        }

        value = value * 10 + digit;
    }

    *base_offset = value;
    return true;
}

// Compara dos long para qsort
static int compare_long(const void* a, const void* b) {
    long x = *(const long*)a;
    long y = *(const long*)b;

    return (x > y) - (x < y);
}

// Busca los segmentos del directorio y los devuelve ordenados por offset
static log_status_t list_segments(const char* dir, long** out, int* count) {
    DIR* d = opendir(dir);

    if (d == NULL) {
        return LOG_ERR_IO;
    }

    long* bases = NULL;
    int n = 0;
    int capacity = 0;
    struct dirent* entry;

    while ((entry = readdir(d)) != NULL) {
        long base;

        if (!parse_segment_name(entry->d_name, &base)) {
            continue;
        }

        if (n == capacity) {
            capacity = capacity > 0 ? capacity * 2 : 8;
            long* tmp = realloc(bases, (size_t)capacity * sizeof(long));

            if (tmp == NULL) {
                free(bases);
                closedir(d);
                return LOG_ERR_NOMEM;
            }

            bases = tmp;
        }

        bases[n++] = base;
    }

    closedir(d);

    if (n > 0) {
        qsort(bases, (size_t)n, sizeof(long), compare_long);
    }

    *out = bases;
    *count = n;
    return LOG_OK;
}

// Guarda la posición de un mensaje más en el índice del segmento
static int segment_push(segment_t* seg, long position) {
    if (seg->count == seg->capacity) {
        long capacity = seg->capacity > 0 ? seg->capacity * 2 : INITIAL_CAPACITY;
        long* tmp = realloc(seg->positions, (size_t)capacity * sizeof(long));

        if (tmp == NULL) {
            return -1;
        }

        seg->positions = tmp;
        seg->capacity = capacity;
    }

    seg->positions[seg->count++] = position;
    return 0;
}

// Agrega un segmento al final del arreglo.
static segment_t* segments_add(partition_log_t* log, long base_offset, int fd) {
    if (log->num_segments == log->segments_capacity) {
        int capacity = log->segments_capacity > 0 ? log->segments_capacity * 2 : 8;
        segment_t* tmp = realloc(log->segments, (size_t)capacity * sizeof(segment_t));

        if (tmp == NULL) {
            return NULL;
        }

        log->segments = tmp;
        log->segments_capacity = capacity;
    }

    segment_t* seg = &log->segments[log->num_segments++];
    memset(seg, 0, sizeof(*seg));
    seg->base_offset = base_offset;
    seg->fd = fd;

    return seg;
}

// Crea un archivo de segmento vacío y lo agrega al arreglo
static log_status_t create_segment(partition_log_t* log, long base_offset) {
    char path[LOG_PATH_MAX];

    if (segment_path(log, base_offset, path, sizeof(path)) != 0) {
        return LOG_ERR_ARGS;
    }

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0644);

    if (fd < 0) {
        return LOG_ERR_IO;
    }

    fsync_dir(log->dir);

    pthread_rwlock_wrlock(&log->segments_lock);
    segment_t* seg = segments_add(log, base_offset, fd);
    pthread_rwlock_unlock(&log->segments_lock);

    if (seg == NULL) {
        close(fd);
        return LOG_ERR_NOMEM;
    }

    return LOG_OK;
}

// Lee los registros de un segmento y arma su índice. Si encuentra un registro
// cortado, dañado o con un offset que no sigue, corta el archivo justo antes
// y deja *bad en true para que no se confíe en lo que viene después
static log_status_t segment_load(segment_t* seg, long* expected_offset, bool* bad) {
    struct stat st;

    if (fstat(seg->fd, &st) != 0) {
        return LOG_ERR_IO;
    }

    long file_size = (long)st.st_size;
    long position = 0;
    unsigned char* buf = NULL;
    size_t buf_capacity = 0;
    log_status_t status = LOG_OK;

    while (position < file_size) {
        unsigned char length[MESSAGE_LENGTH_SIZE];

        if (read_upto(seg->fd, length, sizeof(length), position) != (ssize_t)sizeof(length)) {
            *bad = true;
            break;
        }

        size_t size = message_record_size(length);

        if (size == 0 || position + (long)size > file_size) {
            *bad = true;
            break;
        }

        if (size > buf_capacity) {
            unsigned char* tmp = realloc(buf, size);

            if (tmp == NULL) {
                status = LOG_ERR_NOMEM;
                break;
            }

            buf = tmp;
            buf_capacity = size;
        }

        message_t msg;
        size_t used = 0;

        if (read_upto(seg->fd, buf, size, position) != (ssize_t)size
            || message_deserialize(buf, size, &msg, &used) != MESSAGE_OK) {
            *bad = true;
            break;
        }

        long offset = msg.offset;
        message_destroy(&msg);

        if (offset != *expected_offset) {
            *bad = true;
            break;
        }

        if (segment_push(seg, position) != 0) {
            status = LOG_ERR_NOMEM;
            break;
        }

        position += (long)size;
        (*expected_offset)++;
    }

    free(buf);
    seg->size = position;

    if (status == LOG_OK && *bad && ftruncate(seg->fd, position) != 0) {
        status = LOG_ERR_IO;
    }

    return status;
}

// Lee el high watermark guardado. Devuelve -1 si no hay uno válido
static long checkpoint_read(const partition_log_t* log) {
    char path[LOG_PATH_MAX];

    if (log_path(log, CHECKPOINT_NAME, path, sizeof(path)) != 0) {
        return -1;
    }

    FILE* file = fopen(path, "r");

    if (file == NULL) {
        return -1;
    }

    long value = -1;

    if (fscanf(file, "%ld", &value) != 1) {
        value = -1;
    }

    fclose(file);
    return value;
}

// Guarda el high watermark en un archivo temporal y luego lo renombra, para
//  que nunca quede un checkpoint escrito a medias
static log_status_t checkpoint_write(const partition_log_t* log, long value) {
    char path[LOG_PATH_MAX];
    char tmp[LOG_PATH_MAX];

    if (log_path(log, CHECKPOINT_NAME, path, sizeof(path)) != 0
        || log_path(log, CHECKPOINT_NAME ".tmp", tmp, sizeof(tmp)) != 0) {
        return LOG_ERR_ARGS;
    }

    FILE* file = fopen(tmp, "w");

    if (file == NULL) {
        return LOG_ERR_IO;
    }

    bool ok = fprintf(file, "%ld\n", value) > 0 && fflush(file) == 0 && fsync(fileno(file)) == 0;

    if (fclose(file) != 0 || !ok || rename(tmp, path) != 0) {
        remove(tmp);
        return LOG_ERR_IO;
    }

    fsync_dir(log->dir);
    return LOG_OK;
}

// Arma el log a partir de lo que hay en el directorio
static log_status_t log_recover(partition_log_t* log) {
    long* bases = NULL;
    int count = 0;
    log_status_t status = list_segments(log->dir, &bases, &count);

    if (status != LOG_OK) {
        return status;
    }

    long expected = count > 0 ? bases[0] : 0;
    bool bad = false;

    for (int i = 0; i < count && status == LOG_OK; i++) {
        char path[LOG_PATH_MAX];

        if (segment_path(log, bases[i], path, sizeof(path)) != 0) {
            status = LOG_ERR_ARGS;
            break;
        }

        // Lo que viene después de un registro dañado o de un hueco no es confiable
        if (bad || bases[i] != expected) {
            bad = true;
            unlink(path);
            continue;
        }

        int fd = open(path, O_RDWR | O_APPEND);

        if (fd < 0) {
            status = LOG_ERR_IO;
            break;
        }

        segment_t* seg = segments_add(log, bases[i], fd);

        if (seg == NULL) {
            close(fd);
            status = LOG_ERR_NOMEM;
            break;
        }

        status = segment_load(seg, &expected, &bad);
    }

    free(bases);

    if (status == LOG_OK && log->num_segments == 0) {
        status = create_segment(log, expected);
    }

    if (status != LOG_OK) {
        return status;
    }

    log->log_start_offset = log->segments[0].base_offset;
    log->next_offset = expected;

    // Sin checkpoint no se sabe qué estaba confirmado, entonces no se muestra
    // nada hasta que el líder vuelva a calcular el high watermark
    long hw = checkpoint_read(log);

    if (hw < log->log_start_offset) {
        hw = log->log_start_offset;
    }

    if (hw > log->next_offset) {
        hw = log->next_offset;
    }

    log->high_watermark = hw;
    return LOG_OK;
}
 
static void recompute_hw_locked(partition_log_t* log) {
    if (!log->is_leader) {
        return;
    }

    long candidate = log->next_offset;

    for (int i = 0; i < log->num_replicas; i++) {
        if (log->replica_in_sync[i] && log->replica_end_offsets[i] < candidate) {
            candidate = log->replica_end_offsets[i];
        }
    }

    if (candidate > log->high_watermark) {
        log->high_watermark = candidate;
        pthread_cond_broadcast(&log->hw_advanced);
    }
}

// Termina el segmento actual y empieza otro.
static log_status_t roll_segment_locked(partition_log_t* log) {
    segment_t* active = &log->segments[log->num_segments - 1];

    // El segmento viejo ya no cambia, se deja en disco antes de seguir
    if (fsync(active->fd) != 0) {
        return LOG_ERR_IO;
    }

    return create_segment(log, log->next_offset);
}

// Quita lo que haya alcanzado a escribirse de un registro que falló, para no
// dejar uno a medias. Si ni eso se puede, el archivo ya no coincide con el
// índice y el log deja de aceptar escrituras. Se llama con lock tomado
static void undo_write_locked(partition_log_t* log, segment_t* seg, long position) {
    if (ftruncate(seg->fd, position) != 0) {
        log->closed = true;
    }
}

// Escribe un registro ya serializado al final del log. Se llama con lock
// tomado, así solo un hilo escribe a la vez en el segmento actual
static log_status_t append_locked(partition_log_t* log, const unsigned char* record, size_t size) {
    segment_t* active = &log->segments[log->num_segments - 1];

    if (active->size > 0 && active->size + (long)size > log->config.segment_bytes) {
        log_status_t status = roll_segment_locked(log);

        if (status != LOG_OK) {
            return status;
        }

        active = &log->segments[log->num_segments - 1];
    }

    long position = active->size;
    bool written = write_all(active->fd, record, size) == 0;

    if (written && log->config.fsync_each_append) {
        written = fsync(active->fd) == 0;
    }

    if (!written) {
        undo_write_locked(log, active, position);
        return LOG_ERR_IO;
    }

    // El mensaje se agrega al índice después de estar en el archivo, así un
    // lector nunca encuentra en el índice algo que todavía no se escribió
    pthread_rwlock_wrlock(&log->segments_lock);
    int pushed = segment_push(active, position);

    if (pushed == 0) {
        active->size = position + (long)size;
    }

    pthread_rwlock_unlock(&log->segments_lock);

    if (pushed != 0) {
        undo_write_locked(log, active, position);
        return LOG_ERR_NOMEM;
    }

    log->next_offset++;
    recompute_hw_locked(log);

    return LOG_OK;
}

// Busca el segmento que contiene offset. Se llama con segments_lock tomado
static const segment_t* find_segment(const partition_log_t* log, long offset) {
    int low = 0;
    int high = log->num_segments - 1;
    int found = -1;

    while (low <= high) {
        int mid = low + (high - low) / 2;

        if (log->segments[mid].base_offset <= offset) {
            found = mid;
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    if (found < 0) {
        return NULL;
    }

    const segment_t* seg = &log->segments[found];

    return (offset - seg->base_offset < seg->count) ? seg : NULL;
}

// Dónde empieza y cuánto mide el registro de offset dentro de su segmento
static void record_bounds(const segment_t* seg, long offset, long* position, size_t* size) {
    long i = offset - seg->base_offset;
    long end = (i + 1 < seg->count) ? seg->positions[i + 1] : seg->size;

    *position = seg->positions[i];
    *size = (size_t)(end - *position);
}

// Libera todo lo del log. Se usa al destruirlo o si log_open falla
static void log_free(partition_log_t* log) {
    for (int i = 0; i < log->num_segments; i++) {
        close(log->segments[i].fd);
        free(log->segments[i].positions);
    }

    free(log->segments);
    free(log->replica_end_offsets);
    free(log->replica_in_sync);
    free(log->dir);

    pthread_rwlock_destroy(&log->segments_lock);
    pthread_cond_destroy(&log->hw_advanced);
    pthread_mutex_destroy(&log->lock);
    free(log);
}

// Funciones públicas

log_status_t log_open(partition_log_t** out, const char* dir, const log_config_t* config) {
    if (out == NULL || dir == NULL || dir[0] == '\0') {
        return LOG_ERR_ARGS;
    }

    *out = NULL;

    if (make_dirs(dir) != 0) {
        return LOG_ERR_IO;
    }

    partition_log_t* log = calloc(1, sizeof(*log));

    if (log == NULL) {
        return LOG_ERR_NOMEM;
    }

    log->dir = strdup(dir);
    log->config.segment_bytes = LOG_DEFAULT_SEGMENT_BYTES;

    if (config != NULL) {
        if (config->segment_bytes > 0) {
            log->config.segment_bytes = config->segment_bytes;
        }

        log->config.fsync_each_append = config->fsync_each_append;
    }

    // La espera del high watermark usa el reloj monotónico, así no le afecta
    // que alguien cambie la hora del sistema
    pthread_condattr_t attr;
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);

    bool ready = log->dir != NULL
        && pthread_mutex_init(&log->lock, NULL) == 0
        && pthread_cond_init(&log->hw_advanced, &attr) == 0
        && pthread_rwlock_init(&log->segments_lock, NULL) == 0;

    pthread_condattr_destroy(&attr);

    if (!ready) {
        free(log->dir);
        free(log);
        return LOG_ERR_NOMEM;
    }

    log_status_t status = log_recover(log);

    if (status != LOG_OK) {
        log_free(log);
        return status;
    }

    *out = log;
    return LOG_OK;
}

void log_shutdown(partition_log_t* log) {
    if (log == NULL) {
        return;
    }

    pthread_mutex_lock(&log->lock);
    log->closed = true;
    pthread_cond_broadcast(&log->hw_advanced);
    pthread_mutex_unlock(&log->lock);
}

log_status_t log_destroy(partition_log_t* log) {
    if (log == NULL) {
        return LOG_ERR_ARGS;
    }

    log_shutdown(log);
    log_status_t status = log_flush(log);
    log_free(log);

    return status;
}

log_status_t log_append(partition_log_t* log, message_t* msg, long* offset) {
    if (log == NULL || msg == NULL) {
        return LOG_ERR_ARGS;
    }

    size_t size = message_serialized_size(msg);

    if (size == 0 || size > MESSAGE_MAX_SIZE) {
        return LOG_ERR_ARGS;
    }

    unsigned char* record = malloc(size);

    if (record == NULL) {
        return LOG_ERR_NOMEM;
    }

    log_status_t status;
    pthread_mutex_lock(&log->lock);

    if (log->closed) {
        status = LOG_CLOSED;
    } else {
        // El offset se asigna con el lock tomado, así dos productores nunca
        // reciben el mismo y no quedan huecos
        msg->offset = log->next_offset;

        if (message_serialize(msg, record, size) < 0) {
            status = LOG_ERR_ARGS;
        } else {
            status = append_locked(log, record, size);
        }

        if (status == LOG_OK && offset != NULL) {
            *offset = msg->offset;
        } else if (status != LOG_OK) {
            msg->offset = -1;
        }
    }

    pthread_mutex_unlock(&log->lock);
    free(record);

    return status;
}

log_status_t log_append_record(partition_log_t* log, const unsigned char* record, size_t size) {
    if (log == NULL || record == NULL) {
        return LOG_ERR_ARGS;
    }

    // Antes de escribir se revisa que el registro esté completo y sin daños
    message_t msg;
    size_t used = 0;

    if (message_deserialize(record, size, &msg, &used) != MESSAGE_OK) {
        return LOG_ERR_ARGS;
    }

    long offset = msg.offset;
    message_destroy(&msg);

    if (used != size) {
        return LOG_ERR_ARGS;
    }

    log_status_t status;
    pthread_mutex_lock(&log->lock);

    if (log->closed) {
        status = LOG_CLOSED;
    } else if (offset != log->next_offset) {
        // La réplica se desfasó del líder y tiene que ponerse al día
        status = LOG_ERR_OFFSET;
    } else {
        status = append_locked(log, record, size);
    }

    pthread_mutex_unlock(&log->lock);
    return status;
}

log_status_t log_read(partition_log_t* log, long offset, message_t* out, int max_messages, int* count) {
    if (log == NULL || out == NULL || max_messages <= 0 || count == NULL) {
        return LOG_ERR_ARGS;
    }

    *count = 0;

    pthread_mutex_lock(&log->lock);
    long start = log->log_start_offset;
    long hw = log->high_watermark;
    pthread_mutex_unlock(&log->lock);

    if (offset < start || offset > hw) {
        return LOG_ERR_OFFSET;
    }

    // Solo hasta el high watermark. lo de arriba puede no estar replicado
    long end = (hw - offset < max_messages) ? hw : offset + max_messages;
    unsigned char* buf = NULL;
    size_t buf_capacity = 0;
    log_status_t status = LOG_OK;

    // Todo lo que está por debajo del high watermark ya no cambia, por eso se
    // puede leer del disco con pread mientras otros hilos escriben más adelante
    pthread_rwlock_rdlock(&log->segments_lock);

    for (long current = offset; current < end; current++) {
        const segment_t* seg = find_segment(log, current);

        if (seg == NULL) {
            status = LOG_ERR_OFFSET;
            break;
        }

        long position;
        size_t size;
        record_bounds(seg, current, &position, &size);

        if (size > buf_capacity) {
            unsigned char* tmp = realloc(buf, size);

            if (tmp == NULL) {
                status = LOG_ERR_NOMEM;
                break;
            }

            buf = tmp;
            buf_capacity = size;
        }

        size_t used = 0;

        if (read_upto(seg->fd, buf, size, position) != (ssize_t)size
            || message_deserialize(buf, size, &out[*count], &used) != MESSAGE_OK) {
            status = LOG_ERR_IO;
            break;
        }

        (*count)++;
    }

    pthread_rwlock_unlock(&log->segments_lock);
    free(buf);

    if (status != LOG_OK) {
        for (int i = 0; i < *count; i++) {
            message_destroy(&out[i]);
        }

        *count = 0;
    }

    return status;
}

log_status_t log_read_raw(partition_log_t* log, long offset, unsigned char* buf, size_t buf_size,
                          size_t* used, int* count) {

    if (log == NULL || buf == NULL || used == NULL || count == NULL) {
        return LOG_ERR_ARGS;
    }

    *used = 0;
    *count = 0;

    pthread_mutex_lock(&log->lock);
    long start = log->log_start_offset;
    long end = log->next_offset;
    pthread_mutex_unlock(&log->lock);

    if (offset < start || offset > end) {
        return LOG_ERR_OFFSET;
    }

    log_status_t status = LOG_OK;
    pthread_rwlock_rdlock(&log->segments_lock);

    for (long current = offset; current < end; current++) {
        const segment_t* seg = find_segment(log, current);

        if (seg == NULL) {
            status = LOG_ERR_OFFSET;
            break;
        }

        long position;
        size_t size;
        record_bounds(seg, current, &position, &size);

        // Solo se copian registros completos
        if (*used + size > buf_size) {
            if (*count == 0) {
                status = LOG_ERR_SPACE;
            }

            break;
        }

        if (read_upto(seg->fd, buf + *used, size, position) != (ssize_t)size) {
            status = LOG_ERR_IO;
            break;
        }

        *used += size;
        (*count)++;
    }

    pthread_rwlock_unlock(&log->segments_lock);

    if (status != LOG_OK) {
        *used = 0;
        *count = 0;
    }

    return status;
}

log_status_t log_set_leader(partition_log_t* log, int num_replicas) {
    if (log == NULL || num_replicas < 0) {
        return LOG_ERR_ARGS;
    }

    long* ends = NULL;
    bool* in_sync = NULL;

    if (num_replicas > 0) {
        ends = malloc((size_t)num_replicas * sizeof(long));
        in_sync = malloc((size_t)num_replicas * sizeof(bool));

        if (ends == NULL || in_sync == NULL) {
            free(ends);
            free(in_sync);
            return LOG_ERR_NOMEM;
        }
    }

    pthread_mutex_lock(&log->lock);

    // Todavía no se sabe qué tiene cada réplica, se asume lo mínimo. Como el
    // high watermark nunca retrocede, eso no esconde mensajes ya confirmados
    for (int i = 0; i < num_replicas; i++) {
        ends[i] = log->log_start_offset;
        in_sync[i] = true;
    }

    long* old_ends = log->replica_end_offsets;
    bool* old_in_sync = log->replica_in_sync;

    log->is_leader = true;
    log->num_replicas = num_replicas;
    log->replica_end_offsets = ends;
    log->replica_in_sync = in_sync;
    recompute_hw_locked(log);

    pthread_mutex_unlock(&log->lock);

    free(old_ends);
    free(old_in_sync);

    return LOG_OK;
}

void log_set_follower(partition_log_t* log) {
    if (log == NULL) {
        return;
    }

    pthread_mutex_lock(&log->lock);

    long* old_ends = log->replica_end_offsets;
    bool* old_in_sync = log->replica_in_sync;

    log->is_leader = false;
    log->num_replicas = 0;
    log->replica_end_offsets = NULL;
    log->replica_in_sync = NULL;

    pthread_mutex_unlock(&log->lock);

    free(old_ends);
    free(old_in_sync);
}

log_status_t log_replica_ack(partition_log_t* log, int replica, long end_offset) {
    if (log == NULL) {
        return LOG_ERR_ARGS;
    }

    log_status_t status = LOG_OK;
    pthread_mutex_lock(&log->lock);

    if (!log->is_leader || replica < 0 || replica >= log->num_replicas) {
        status = LOG_ERR_ARGS;
    } else {
        // Una réplica no puede tener más de lo que tiene el líder
        if (end_offset > log->next_offset) {
            end_offset = log->next_offset;
        }

        if (end_offset > log->replica_end_offsets[replica]) {
            log->replica_end_offsets[replica] = end_offset;
        }

        recompute_hw_locked(log);
    }

    pthread_mutex_unlock(&log->lock);
    return status;
}


log_status_t log_replica_reset_progress(partition_log_t *log, int replica, long end_offset) {
    if (log == NULL) return LOG_ERR_ARGS;

    pthread_mutex_lock(&log->lock);

    if (!log->is_leader || replica < 0 || replica >= log->num_replicas ||
        end_offset < log->log_start_offset || end_offset > log->next_offset) {
        pthread_mutex_unlock(&log->lock);
        return LOG_ERR_ARGS;
    }

    // La replica debe demostrar nuevamente que esta sincronizada.
    log->replica_in_sync[replica] = false;

    // Reemplazar el progreso anterior por el offset comunicado.
    log->replica_end_offsets[replica] = end_offset;

    recompute_hw_locked(log);

    pthread_mutex_unlock(&log->lock);
    return LOG_OK;
}

log_status_t log_replica_progress(partition_log_t *log, int replica, long *end_offset) {
    if (log == NULL || end_offset == NULL) {
        return LOG_ERR_ARGS;
    }

    pthread_mutex_lock(&log->lock);

    if (!log->is_leader || replica < 0 || replica >= log->num_replicas) {
        pthread_mutex_unlock(&log->lock);
        return LOG_ERR_ARGS;
    }

    *end_offset = log->replica_end_offsets[replica];

    pthread_mutex_unlock(&log->lock);
    return LOG_OK;
}


log_status_t log_set_replica_in_sync(partition_log_t* log, int replica, bool in_sync) {
    if (log == NULL) {
        return LOG_ERR_ARGS;
    }

    log_status_t status = LOG_OK;
    pthread_mutex_lock(&log->lock);

    if (!log->is_leader || replica < 0 || replica >= log->num_replicas) {
        status = LOG_ERR_ARGS;
    } else {
        // Sacar una réplica caída del conjunto deja avanzar el high watermark
        log->replica_in_sync[replica] = in_sync;
        recompute_hw_locked(log);
    }

    pthread_mutex_unlock(&log->lock);
    return status;
}

bool log_all_replicas_in_sync(partition_log_t *log) {
    if (log == NULL) return false;

    pthread_mutex_lock(&log->lock);

    bool ready = log->is_leader && !log->closed;

    for (int i = 0; ready && i < log->num_replicas; i++) {
        if (!log->replica_in_sync[i]) {
            ready = false;
        }
    }

    pthread_mutex_unlock(&log->lock);
    return ready;
}

void log_advance_hw(partition_log_t* log, long high_watermark) {
    if (log == NULL) {
        return;
    }

    pthread_mutex_lock(&log->lock);

    // En la réplica el high watermark no puede pasar de lo que ella tiene
    if (high_watermark > log->next_offset) {
        high_watermark = log->next_offset;
    }

    if (!log->is_leader && high_watermark > log->high_watermark) {
        log->high_watermark = high_watermark;
        pthread_cond_broadcast(&log->hw_advanced);
    }

    pthread_mutex_unlock(&log->lock);
}

log_status_t log_wait_hw(partition_log_t* log, long offset, int timeout_ms) {
    if (log == NULL || offset < 0) {
        return LOG_ERR_ARGS;
    }

    struct timespec deadline;

    if (timeout_ms >= 0) {
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;

        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
    }

    log_status_t status = LOG_OK;
    pthread_mutex_lock(&log->lock);

    // Se espera en un ciclo porque la variable de condición puede despertar
    // sin que el high watermark haya llegado
    while (log->high_watermark <= offset && !log->closed) {
        if (timeout_ms < 0) {
            pthread_cond_wait(&log->hw_advanced, &log->lock);
        } else if (pthread_cond_timedwait(&log->hw_advanced, &log->lock, &deadline) == ETIMEDOUT
            && log->high_watermark <= offset && !log->closed) {
            status = LOG_TIMEOUT;
            break;
        }
    }

    if (status == LOG_OK && log->high_watermark <= offset) {
        status = LOG_CLOSED;
    }

    pthread_mutex_unlock(&log->lock);
    return status;
}

log_status_t log_flush(partition_log_t* log) {
    if (log == NULL) {
        return LOG_ERR_ARGS;
    }

    pthread_mutex_lock(&log->lock);

    // Los segmentos viejos ya se guardaron al cambiar de segmento
    log_status_t status = LOG_OK;

    if (fsync(log->segments[log->num_segments - 1].fd) != 0) {
        status = LOG_ERR_IO;
    }

    if (status == LOG_OK) {
        status = checkpoint_write(log, log->high_watermark);
    }

    pthread_mutex_unlock(&log->lock);
    return status;
}

bool log_is_leader(partition_log_t* log) {
    pthread_mutex_lock(&log->lock);
    bool value = log->is_leader;
    pthread_mutex_unlock(&log->lock);

    return value;
}

long log_next_offset(partition_log_t* log) {
    pthread_mutex_lock(&log->lock);
    long value = log->next_offset;
    pthread_mutex_unlock(&log->lock);

    return value;
}

long log_high_watermark(partition_log_t* log) {
    pthread_mutex_lock(&log->lock);
    long value = log->high_watermark;
    pthread_mutex_unlock(&log->lock);

    return value;
}

long log_start_offset(partition_log_t* log) {
    pthread_mutex_lock(&log->lock);
    long value = log->log_start_offset;
    pthread_mutex_unlock(&log->lock);

    return value;
}