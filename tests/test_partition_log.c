// test_partition_log.c - Pruebas unitarias y de concurrencia del log de una partición

#define _POSIX_C_SOURCE 200809L

#include "partition_log.h"

#include <dirent.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CONCURRENT_THREADS 8
#define CONCURRENT_APPENDS 500
#define PATH_SIZE 512
#define DIR_SIZE 64

typedef struct {
    const char* name;
    bool (*run)(void);
} SimpleTest;

// Crea un directorio temporal vacío para una prueba
static void make_temp_dir(char* out, size_t size) {
    snprintf(out, size, "/tmp/test_partition_log_XXXXXX");

    if (mkdtemp(out) == NULL) {
        perror("mkdtemp");
        exit(EXIT_FAILURE);
    }
}

// Borra los archivos del directorio temporal y el directorio
static void remove_temp_dir(const char* dir) {
    DIR* d = opendir(dir);

    if (d != NULL) {
        struct dirent* entry;

        while ((entry = readdir(d)) != NULL) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            char path[PATH_SIZE];
            snprintf(path, sizeof(path), "%s/%s", dir, entry->d_name);
            remove(path);
        }

        closedir(d);
    }

    rmdir(dir);
}

// Cuenta los segmentos .log del directorio
static int count_segments(const char* dir) {
    DIR* d = opendir(dir);
    int count = 0;

    if (d == NULL) {
        return -1;
    }

    struct dirent* entry;

    while ((entry = readdir(d)) != NULL) {
        size_t len = strlen(entry->d_name);

        if (len > 4 && strcmp(entry->d_name + len - 4, ".log") == 0) {
            count++;
        }
    }

    closedir(d);
    return count;
}

// Espera unos milisegundos
static void sleep_ms(long ms) {
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

// Escribe un mensaje de texto y devuelve su offset, o -1 si falla
static long append_text(partition_log_t* log, const char* key, const char* text) {
    message_t msg;
    int key_size = key != NULL ? (int)strlen(key) : 0;

    if (message_init(&msg, key, key_size, text, (int)strlen(text)) != MESSAGE_OK) {
        return -1;
    }

    long offset = -1;

    if (log_append(log, &msg, &offset) != LOG_OK) {
        offset = -1;
    }

    message_destroy(&msg);
    return offset;
}

// Revisa que el mensaje tenga el offset y el texto esperados
static bool message_is(const message_t* msg, long offset, const char* text) {
    return msg->offset == offset && msg->value_size == (int)strlen(text)
        && memcmp(msg->value, text, strlen(text)) == 0;
}

// Libera un arreglo de mensajes leídos
static void destroy_messages(message_t* msgs, int count) {
    for (int i = 0; i < count; i++) {
        message_destroy(&msgs[i]);
    }
}

// Escribe "mensaje 0", "mensaje 1", ... hasta count
static bool append_numbered(partition_log_t* log, int count) {
    for (int i = 0; i < count; i++) {
        char text[32];
        snprintf(text, sizeof(text), "mensaje %d", i);

        if (append_text(log, NULL, text) != i) {
            return false;
        }
    }

    return true;
}

// Lee count mensajes desde 0 y revisa que sean "mensaje 0", "mensaje 1", ...
static bool read_numbered(partition_log_t* log, int count) {
    message_t* msgs = malloc((size_t)count * sizeof(message_t));
    int read = 0;

    if (msgs == NULL || log_read(log, 0, msgs, count, &read) != LOG_OK || read != count) {
        free(msgs);
        return false;
    }

    bool ok = true;

    for (int i = 0; i < count && ok; i++) {
        char text[32];
        snprintf(text, sizeof(text), "mensaje %d", i);
        ok = message_is(&msgs[i], i, text);
    }

    destroy_messages(msgs, read);
    free(msgs);
    return ok;
}

// Pruebas

// Comprueba que los offsets empiezan en 0 y son consecutivos
static bool test_append_assigns_offsets(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 0) == LOG_OK;
    ok = ok && append_numbered(log, 5);
    ok = ok && log_next_offset(log) == 5 && log_high_watermark(log) == 5;
    ok = ok && read_numbered(log, 5);

    if (log != NULL) {
        log_destroy(log);
    }

    remove_temp_dir(dir);
    return ok;
}

// Comprueba que lo que no está replicado no se puede leer
static bool test_hw_hides_unreplicated(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;
    message_t msgs[4];
    int count = -1;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 1) == LOG_OK;
    ok = ok && append_numbered(log, 3);

    // Sin confirmación de la réplica no se ve nada
    ok = ok && log_high_watermark(log) == 0;
    ok = ok && log_read(log, 0, msgs, 4, &count) == LOG_OK && count == 0;

    // La réplica tiene 0 y 1, el mensaje 2 sigue oculto
    ok = ok && log_replica_ack(log, 0, 2) == LOG_OK && log_high_watermark(log) == 2;
    ok = ok && log_read(log, 0, msgs, 4, &count) == LOG_OK && count == 2;
    destroy_messages(msgs, count);

    // Si la réplica sale del conjunto sincronizado, el líder confirma solo
    ok = ok && log_set_replica_in_sync(log, 0, false) == LOG_OK;
    ok = ok && log_high_watermark(log) == 3;

    // El high watermark nunca retrocede aunque la réplica vuelva atrasada
    ok = ok && log_set_replica_in_sync(log, 0, true) == LOG_OK;
    ok = ok && log_high_watermark(log) == 3;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

typedef struct {
    partition_log_t* log;
    long offset;
    int timeout_ms;
    log_status_t result;
} WaitArgs;

// Hilo que espera a que un offset quede confirmado
static void* wait_worker(void* arg) {
    WaitArgs* args = arg;
    args->result = log_wait_hw(args->log, args->offset, args->timeout_ms);

    return NULL;
}

// Comprueba que log_wait_hw despierta cuando avanza el high watermark
static bool test_wait_hw_wakes_up(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 1) == LOG_OK;
    ok = ok && append_text(log, NULL, "hola") == 0;

    // Sin confirmación se acaba el tiempo
    ok = ok && log_wait_hw(log, 0, 50) == LOG_TIMEOUT;

    // Un productor con acks=all espera y la réplica confirma
    WaitArgs args = {log, 0, 5000, LOG_ERR_ARGS};
    pthread_t thread;
    ok = ok && pthread_create(&thread, NULL, wait_worker, &args) == 0;

    sleep_ms(50);
    log_replica_ack(log, 0, 1);
    pthread_join(thread, NULL);
    ok = ok && args.result == LOG_OK;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

// Comprueba que cerrar el log despierta a quien espera, sin dejarlo bloqueado
static bool test_shutdown_wakes_waiters(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 1) == LOG_OK;
    ok = ok && append_text(log, NULL, "hola") == 0;

    WaitArgs args = {log, 0, -1, LOG_ERR_ARGS};
    pthread_t thread;
    ok = ok && pthread_create(&thread, NULL, wait_worker, &args) == 0;

    sleep_ms(50);
    log_shutdown(log);
    pthread_join(thread, NULL);
    ok = ok && args.result == LOG_CLOSED;

    // Después de cerrar no se aceptan escrituras
    ok = ok && append_text(log, NULL, "tarde") == -1;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

typedef struct {
    partition_log_t* log;
    int id;
} AppendArgs;

// Hilo productor: escribe sus mensajes "t<id> <i>"
static void* append_worker(void* arg) {
    AppendArgs* args = arg;

    for (int i = 0; i < CONCURRENT_APPENDS; i++) {
        char text[32];
        snprintf(text, sizeof(text), "t%d %d", args->id, i);
        append_text(args->log, NULL, text);
    }

    return NULL;
}

// Comprueba que con muchos productores a la vez los offsets no tienen huecos ni
// repeticiones, cada mensaje está una sola vez y el orden de cada productor se respeta
static bool test_concurrent_appends(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;
    log_config_t config = {4096, false}; // segmentos pequeños para que también cambie de segmento

    if (log_open(&log, dir, &config) != LOG_OK || log_set_leader(log, 0) != LOG_OK) {
        remove_temp_dir(dir);
        return false;
    }

    pthread_t threads[CONCURRENT_THREADS];
    AppendArgs args[CONCURRENT_THREADS];

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        args[i] = (AppendArgs){log, i};
        pthread_create(&threads[i], NULL, append_worker, &args[i]);
    }

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    int total = CONCURRENT_THREADS * CONCURRENT_APPENDS;
    message_t* msgs = malloc((size_t)total * sizeof(message_t));
    int count = 0;
    bool ok = msgs != NULL && log_next_offset(log) == total
        && log_read(log, 0, msgs, total, &count) == LOG_OK && count == total;

    // next_seen[t] es el siguiente número que se espera del productor t
    int next_seen[CONCURRENT_THREADS] = {0};

    for (int i = 0; i < count && ok; i++) {
        int id = -1;
        int seq = -1;
        char text[32] = {0};
        memcpy(text, msgs[i].value, (size_t)msgs[i].value_size < sizeof(text) - 1
            ? (size_t)msgs[i].value_size : sizeof(text) - 1);

        ok = msgs[i].offset == i && sscanf(text, "t%d %d", &id, &seq) == 2
            && id >= 0 && id < CONCURRENT_THREADS && seq == next_seen[id];

        if (ok) {
            next_seen[id]++;
        }
    }

    destroy_messages(msgs, count);
    free(msgs);
    ok = ok && count_segments(dir) > 1;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

// Comprueba que después de cerrar y abrir el log todo sigue ahí
static bool test_recover_after_reopen(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_numbered(log, 10);
    ok = ok && log_destroy(log) == LOG_OK;

    ok = ok && log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_next_offset(log) == 10 && log_high_watermark(log) == 10;
    ok = ok && read_numbered(log, 10);

    // Se sigue escribiendo donde había quedado
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_text(log, NULL, "otro") == 10;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

// Comprueba que un registro a medias al final se corta al abrir y el 
// siguiente mensaje reutiliza ese offset sin dejar hueco
static bool test_recover_truncated_tail(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_numbered(log, 3);
    ok = ok && log_destroy(log) == LOG_OK;

    // Se pegan los primeros 10 bytes de un cuarto registro
    char path[PATH_SIZE];
    snprintf(path, sizeof(path), "%s/%020d.log", dir, 0);
    struct stat before;
    ok = ok && stat(path, &before) == 0;

    message_t msg;
    unsigned char buf[64];
    ok = ok && message_init(&msg, NULL, 0, "a medias", 8) == MESSAGE_OK;
    msg.offset = 3;
    ok = ok && message_serialize(&msg, buf, sizeof(buf)) > 10;
    message_destroy(&msg);

    FILE* file = fopen(path, "ab");
    ok = ok && file != NULL && fwrite(buf, 1, 10, file) == 10;

    if (file != NULL) {
        fclose(file);
    }

    ok = ok && log_open(&log, dir, NULL) == LOG_OK;
    struct stat after;
    ok = ok && stat(path, &after) == 0 && after.st_size == before.st_size;
    ok = ok && log_next_offset(log) == 3 && read_numbered(log, 3);
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_text(log, NULL, "nuevo") == 3;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

// Comprueba que sin checkpoint no se muestra nada hasta que haya un líder
static bool test_recover_without_checkpoint(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_numbered(log, 4);
    ok = ok && log_destroy(log) == LOG_OK;

    char path[PATH_SIZE];
    snprintf(path, sizeof(path), "%s/hw.checkpoint", dir);
    ok = ok && remove(path) == 0;

    ok = ok && log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_next_offset(log) == 4 && log_high_watermark(log) == 0;

    // Un líder sin réplicas confirma todo lo que tiene
    ok = ok && log_set_leader(log, 0) == LOG_OK && log_high_watermark(log) == 4;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

// Comprueba que el log cambia de segmento y se lee igual a través de varios
static bool test_segments_roll(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;
    log_config_t config = {200, false};

    bool ok = log_open(&log, dir, &config) == LOG_OK;
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_numbered(log, 50);
    ok = ok && count_segments(dir) > 5 && read_numbered(log, 50);
    ok = ok && log_destroy(log) == LOG_OK;

    // Al reabrir se arma el índice de todos los segmentos
    ok = ok && log_open(&log, dir, &config) == LOG_OK;
    ok = ok && log_next_offset(log) == 50 && read_numbered(log, 50);

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

// Comprueba que una réplica copia los registros del líder y queda igual
static bool test_replica_copies_leader(void) {
    char leader_dir[DIR_SIZE];
    char follower_dir[DIR_SIZE];
    make_temp_dir(leader_dir, sizeof(leader_dir));
    make_temp_dir(follower_dir, sizeof(follower_dir));
    partition_log_t* leader;
    partition_log_t* follower;
    unsigned char buf[1024];
    size_t used = 0;
    int count = 0;

    bool ok = log_open(&leader, leader_dir, NULL) == LOG_OK
        && log_open(&follower, follower_dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(leader, 1) == LOG_OK && append_numbered(leader, 3);

    // El líder manda lo que tiene aunque todavía no esté confirmado
    ok = ok && log_read_raw(leader, 0, buf, sizeof(buf), &used, &count) == LOG_OK && count == 3;

    size_t position = 0;
    size_t first_size = message_record_size(buf);

    for (int i = 0; i < count && ok; i++) {
        size_t size = message_record_size(buf + position);
        ok = size > 0 && log_append_record(follower, buf + position, size) == LOG_OK;
        position += size;
    }

    // Repetir un registro o mandar uno dañado se rechaza
    ok = ok && log_append_record(follower, buf, first_size) == LOG_ERR_OFFSET;
    buf[first_size - 1] ^= 0x01;
    ok = ok && log_append_record(follower, buf, first_size) == LOG_ERR_ARGS;

    // La réplica confirma y el líder le avisa el high watermark nuevo
    ok = ok && log_replica_ack(leader, 0, log_next_offset(follower)) == LOG_OK;
    ok = ok && log_high_watermark(leader) == 3;
    log_advance_hw(follower, 10); // no puede pasar de lo que tiene
    ok = ok && log_high_watermark(follower) == 3 && read_numbered(follower, 3);

    log_destroy(leader);
    log_destroy(follower);
    remove_temp_dir(leader_dir);
    remove_temp_dir(follower_dir);
    return ok;
}

// Comprueba los errores por offset fuera de rango y búfer pequeño
static bool test_out_of_range(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    partition_log_t* log;
    message_t msgs[2];
    unsigned char small[8];
    size_t used = 0;
    int count = 0;

    bool ok = log_open(&log, dir, NULL) == LOG_OK;
    ok = ok && log_set_leader(log, 0) == LOG_OK && append_numbered(log, 2);
    ok = ok && log_read(log, 3, msgs, 2, &count) == LOG_ERR_OFFSET;
    ok = ok && log_read(log, -1, msgs, 2, &count) == LOG_ERR_OFFSET;
    ok = ok && log_read(log, 2, msgs, 2, &count) == LOG_OK && count == 0;
    ok = ok && log_read_raw(log, 0, small, sizeof(small), &used, &count) == LOG_ERR_SPACE;

    log_destroy(log);
    remove_temp_dir(dir);
    return ok;
}

int main(void) {
    SimpleTest tests[] = {
        {"Los offsets empiezan en 0 y son consecutivos", test_append_assigns_offsets},
        {"Lo no replicado queda oculto por el high watermark", test_hw_hides_unreplicated},
        {"log_wait_hw despierta al confirmar", test_wait_hw_wakes_up},
        {"Cerrar el log despierta a quien espera", test_shutdown_wakes_waiters},
        {"Escrituras concurrentes sin huecos ni repeticiones", test_concurrent_appends},
        {"El log se recupera al reabrir", test_recover_after_reopen},
        {"Un registro a medias se corta al recuperar", test_recover_truncated_tail},
        {"Sin checkpoint no se muestra nada sin líder", test_recover_without_checkpoint},
        {"El log cambia de segmento y se lee igual", test_segments_roll},
        {"La réplica copia los registros del líder", test_replica_copies_leader},
        {"Se rechazan offsets fuera de rango", test_out_of_range}
    };

    int total = sizeof(tests) / sizeof(tests[0]);
    int passed = 0;

    printf("================================\n");
    printf("  PRUEBAS PARTITION LOG\n");
    printf("================================\n\n");

    for (int i = 0; i < total; i++) {
        if (tests[i].run()) {
            printf("[OK] %s\n", tests[i].name);
            passed++;
        } else {
            printf("[FAIL] %s\n", tests[i].name);
        }
    }

    printf("\n================================\n");
    printf("Pruebas de partition log: %d/%d aprobadas\n", passed, total);

    return passed == total ? EXIT_SUCCESS : EXIT_FAILURE;
}
