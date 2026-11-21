// test_topic.c - Pruebas de topic: particionado, apertura de logs y roles con cluster_metadata

#define _XOPEN_SOURCE 700

#include "cluster_metadata.h"
#include "topic.h"

#include <ftw.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define NUM_BROKERS 3
#define ROUND_ROBIN_THREADS 8
#define ROUND_ROBIN_CALLS 3000
#define DIR_SIZE 64
#define PATH_SIZE 256

typedef struct {
    const char* name;
    bool (*run)(void);
} SimpleTest;

// Funciones de apoyo

// Crea un directorio temporal vacío para una prueba
static void make_temp_dir(char* out, size_t size) {
    snprintf(out, size, "/tmp/test_topic_XXXXXX");

    if (mkdtemp(out) == NULL) {
        perror("mkdtemp");
        exit(EXIT_FAILURE);
    }
}

// Borra un archivo o carpeta, la usa nftw para recorrer de adentro hacia afuera
static int remove_entry(const char* path, const struct stat* st, int flag, struct FTW* ftw) {
    (void)st;
    (void)flag;
    (void)ftw;

    return remove(path);
}

// Borra el directorio temporal con todo lo que tiene
static void remove_temp_dir(const char* dir) {
    nftw(dir, remove_entry, 16, FTW_DEPTH | FTW_PHYS);
}

// Revisa si existe la carpeta del log de una partición
static bool log_dir_exists(const char* data_dir, const char* topic, int partition) {
    char path[PATH_SIZE];
    struct stat st;
    snprintf(path, sizeof(path), "%s/%s-%d", data_dir, topic, partition);

    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

// Escribe un mensaje de texto en el log y devuelve su offset, o -1 si falla
static long produce(partition_log_t* log, const char* text) {
    message_t msg;

    if (message_init(&msg, NULL, 0, text, (int)strlen(text)) != MESSAGE_OK) {
        return -1;
    }

    long offset = -1;

    if (log_append(log, &msg, &offset) != LOG_OK) {
        offset = -1;
    }

    message_destroy(&msg);
    return offset;
}

// Copia al seguidor todo lo que tiene el líder, como haría la replicación por MPI
static bool replicate(partition_log_t* leader, partition_log_t* follower) {
    unsigned char buf[4096];
    size_t used = 0;
    int count = 0;
    long from = log_next_offset(follower);

    if (log_read_raw(leader, from, buf, sizeof(buf), &used, &count) != LOG_OK) {
        return false;
    }

    size_t position = 0;

    for (int i = 0; i < count; i++) {
        size_t size = message_record_size(buf + position);

        if (size == 0 || log_append_record(follower, buf + position, size) != LOG_OK) {
            return false;
        }

        position += size;
    }

    return true;
}

// Crea "pedidos" con 3 particiones y factor 2 en los metadatos de un broker y
// le abre los logs que le tocan. Con 3 brokers, la partición p tiene líder p
// y réplicas p y p + 1
static bool setup_broker(cluster_metadata_t* m, int rank, const char* data_dir, topic_t** topic) {
    if (cluster_metadata_init(m) != 0) {
        return false;
    }

    if (create_topic(m, "pedidos", 3, 2, NUM_BROKERS) != 0) {
        cluster_metadata_destroy(m);
        return false;
    }

    *topic = find_topic(m, "pedidos");

    return *topic != NULL && topic_open_logs(*topic, rank, data_dir, NULL) == 0;
}

// Pruebas

// Comprueba que la misma clave va siempre a la misma partición
static bool test_same_key_same_partition(void) {
    topic_t* t = topic_create("pedidos", 5, 1, 0);

    if (t == NULL) {
        return false;
    }

    bool ok = true;

    for (int i = 0; i < 100 && ok; i++) {
        char key[32];
        int len = snprintf(key, sizeof(key), "cliente-%d", i);
        int first = topic_partition_for_key(t, key, len);

        ok = first >= 0 && first < 5;

        for (int j = 0; j < 10 && ok; j++) {
            ok = topic_partition_for_key(t, key, len) == first;
        }
    }

    // Claves distintas no deben ir todas a la misma partición
    bool used[5] = {false};

    for (int i = 0; i < 100; i++) {
        char key[32];
        int len = snprintf(key, sizeof(key), "cliente-%d", i);
        used[topic_partition_for_key(t, key, len)] = true;
    }

    for (int i = 0; i < 5; i++) {
        ok = ok && used[i];
    }

    topic_destroy(t);
    return ok;
}

// Comprueba que los mensajes sin clave se reparten en turno rotativo
static bool test_no_key_round_robin(void) {
    topic_t* t = topic_create("pedidos", 3, 1, 0);

    if (t == NULL) {
        return false;
    }

    bool ok = true;

    for (int i = 0; i < 9 && ok; i++) {
        ok = topic_partition_for_key(t, NULL, 0) == i % 3;
    }

    topic_destroy(t);
    return ok;
}

// Hilo que pide particiones sin clave y cuenta cuántas veces salió cada una
static void* round_robin_worker(void* arg) {
    topic_t* t = arg;
    int* counts = calloc((size_t)t->num_partitions, sizeof(int));

    for (int i = 0; counts != NULL && i < ROUND_ROBIN_CALLS; i++) {
        counts[topic_partition_for_key(t, NULL, 0)]++;
    }

    return counts;
}

// Comprueba que el turno rotativo reparte exacto aunque lo pidan varios hilos
// a la vez, con un contador sin protección se repetirían turnos
static bool test_round_robin_concurrent(void) {
    topic_t* t = topic_create("pedidos", 4, 1, 0);
    pthread_t threads[ROUND_ROBIN_THREADS];
    int totals[4] = {0};
    bool ok = t != NULL;

    for (int i = 0; ok && i < ROUND_ROBIN_THREADS; i++) {
        ok = pthread_create(&threads[i], NULL, round_robin_worker, t) == 0;
    }

    for (int i = 0; ok && i < ROUND_ROBIN_THREADS; i++) {
        void* result;
        pthread_join(threads[i], &result);
        int* counts = result;

        if (counts == NULL) {
            ok = false;
            continue;
        }

        for (int p = 0; p < 4; p++) {
            totals[p] += counts[p];
        }

        free(counts);
    }

    int expected = ROUND_ROBIN_THREADS * ROUND_ROBIN_CALLS / 4;

    for (int p = 0; ok && p < 4; p++) {
        ok = totals[p] == expected;
    }

    topic_destroy(t);
    return ok;
}

// Comprueba que se rechazan nombres que no sirven como carpeta
static bool test_invalid_names(void) {
    const char* invalid[] = {"", ".", "..", "../x", "a/b", "con espacio", "ñandú"};
    const char* valid[] = {"pedidos", "t19", "pagos.v2", "log-errores", "a_b"};

    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        topic_t* t = topic_create(invalid[i], 1, 1, 0);

        if (t != NULL) {
            topic_destroy(t);
            return false;
        }
    }

    for (size_t i = 0; i < sizeof(valid) / sizeof(valid[0]); i++) {
        topic_t* t = topic_create(valid[i], 1, 1, 0);

        if (t == NULL) {
            return false;
        }

        topic_destroy(t);
    }

    return topic_create(NULL, 1, 1, 0) == NULL && topic_create("x", 0, 1, 0) == NULL
        && topic_create("x", 1, 0, 0) == NULL && topic_create("x", 1, 1, -1) == NULL;
}

// Comprueba los seguidores e ISR que ven las funciones de consulta
static bool test_partition_queries(void) {
    topic_t* t = topic_create("pedidos", 1, 3, 0);

    if (t == NULL) {
        return false;
    }

    partition_t* p = topic_get_partition(t, 0);
    p->leader_rank = 2;
    p->replica_ranks[0] = 2;
    p->replica_ranks[1] = 0;
    p->replica_ranks[2] = 1;
    p->isr_ranks[0] = 2;
    p->isr_ranks[1] = 1;

    bool ok = partition_is_leader(p, 2) && !partition_is_leader(p, 0)
        && partition_is_replica(p, 0) && !partition_is_replica(p, 5)
        && partition_follower_index(p, 2) == -1 // el líder no es seguidor
        && partition_follower_index(p, 0) == 0
        && partition_follower_index(p, 1) == 1
        && partition_follower_index(p, 7) == -1
        && partition_isr_count(p) == 2
        && topic_get_partition(t, 1) == NULL && topic_get_partition(t, -1) == NULL;

    topic_destroy(t);
    return ok;
}

// Comprueba que un broker abre solo los logs de las particiones que guarda, y
// con el rol correcto
static bool test_open_only_local_logs(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    cluster_metadata_t m;
    topic_t* t = NULL;

    // El broker 0 es líder de la partición 0 y réplica de la 2
    bool ok = setup_broker(&m, 0, dir, &t);
    partition_t* p0 = ok ? topic_get_partition(t, 0) : NULL;
    partition_t* p1 = ok ? topic_get_partition(t, 1) : NULL;
    partition_t* p2 = ok ? topic_get_partition(t, 2) : NULL;

    ok = ok && p0->log != NULL && p1->log == NULL && p2->log != NULL;
    ok = ok && log_dir_exists(dir, "pedidos", 0) && !log_dir_exists(dir, "pedidos", 1)
        && log_dir_exists(dir, "pedidos", 2);
    ok = ok && log_is_leader(p0->log) && !log_is_leader(p2->log);

    if (t != NULL) {
        cluster_metadata_destroy(&m);
    }

    remove_temp_dir(dir);
    return ok;
}

// Comprueba el camino completo de una partición entre dos brokers: el líder
// escribe, la réplica copia, confirma y el mensaje queda visible
static bool test_leader_and_follower(void) {
    char dir0[DIR_SIZE];
    char dir1[DIR_SIZE];
    make_temp_dir(dir0, sizeof(dir0));
    make_temp_dir(dir1, sizeof(dir1));
    cluster_metadata_t m0;
    cluster_metadata_t m1;
    topic_t* t0 = NULL;
    topic_t* t1 = NULL;

    // La partición 0 tiene líder 0 y réplica 1
    bool ok = setup_broker(&m0, 0, dir0, &t0) && setup_broker(&m1, 1, dir1, &t1);
    partition_t* leader = ok ? topic_get_partition(t0, 0) : NULL;
    partition_t* follower = ok ? topic_get_partition(t1, 0) : NULL;

    ok = ok && produce(leader->log, "hola") == 0 && produce(leader->log, "mundo") == 1;

    // Sin la réplica todavía no hay nada confirmado
    ok = ok && log_high_watermark(leader->log) == 0;

    ok = ok && replicate(leader->log, follower->log);
    int index = ok ? partition_follower_index(leader, 1) : -1;
    ok = ok && index == 0;
    ok = ok && log_replica_ack(leader->log, index, log_next_offset(follower->log)) == LOG_OK;
    ok = ok && log_high_watermark(leader->log) == 2;

    log_advance_hw(follower->log, log_high_watermark(leader->log));
    ok = ok && log_high_watermark(follower->log) == 2;

    if (t0 != NULL) {
        cluster_metadata_destroy(&m0);
    }

    if (t1 != NULL) {
        cluster_metadata_destroy(&m1);
    }

    remove_temp_dir(dir0);
    remove_temp_dir(dir1);
    return ok;
}

// Comprueba que si la réplica sale del ISR el líder deja de esperarla y confirma solo
static bool test_isr_change_updates_log(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    cluster_metadata_t m;
    topic_t* t = NULL;

    bool ok = setup_broker(&m, 0, dir, &t);
    partition_t* p = ok ? topic_get_partition(t, 0) : NULL;

    ok = ok && produce(p->log, "uno") == 0 && log_high_watermark(p->log) == 0;

    // El broker 1 sale del ISR de la partición 0
    metadata_update_t u;
    memset(&u, 0, sizeof(u));
    u.version = m.version + 1;
    u.type = MD_ISR_CHANGE;
    strcpy(u.topic, "pedidos");
    u.partition_id = 0;
    u.isr[0] = 0;
    u.isr_count = 1;

    ok = ok && apply_update(&m, &u) == 0 && topic_update_roles(t, 0) == 0;
    ok = ok && log_high_watermark(p->log) == 1;

    if (t != NULL) {
        cluster_metadata_destroy(&m);
    }

    remove_temp_dir(dir);
    return ok;
}

// Comprueba que al cambiar de líder los logs cambian de rol
static bool test_leader_change_updates_role(void) {
    char dir0[DIR_SIZE];
    char dir1[DIR_SIZE];
    make_temp_dir(dir0, sizeof(dir0));
    make_temp_dir(dir1, sizeof(dir1));
    cluster_metadata_t m0;
    cluster_metadata_t m1;
    topic_t* t0 = NULL;
    topic_t* t1 = NULL;

    bool ok = setup_broker(&m0, 0, dir0, &t0) && setup_broker(&m1, 1, dir1, &t1);
    ok = ok && log_is_leader(topic_get_partition(t0, 0)->log)
        && !log_is_leader(topic_get_partition(t1, 0)->log);

    // El broker 1 pasa a ser el líder de la partición 0 en los dos brokers
    metadata_update_t u;
    memset(&u, 0, sizeof(u));
    u.version = 2;
    u.type = MD_LEADER_CHANGE;
    strcpy(u.topic, "pedidos");
    u.partition_id = 0;
    u.leader_rank = 1;

    ok = ok && apply_update(&m0, &u) == 0 && topic_update_roles(t0, 0) == 0;
    ok = ok && apply_update(&m1, &u) == 0 && topic_update_roles(t1, 1) == 0;
    ok = ok && !log_is_leader(topic_get_partition(t0, 0)->log)
        && log_is_leader(topic_get_partition(t1, 0)->log);

    if (t0 != NULL) {
        cluster_metadata_destroy(&m0);
    }

    if (t1 != NULL) {
        cluster_metadata_destroy(&m1);
    }

    remove_temp_dir(dir0);
    remove_temp_dir(dir1);
    return ok;
}

// Comprueba que después de reiniciar el broker los logs se recuperan
static bool test_reopen_after_restart(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    cluster_metadata_t m;
    topic_t* t = NULL;

    bool ok = setup_broker(&m, 0, dir, &t);
    partition_t* p = ok ? topic_get_partition(t, 2) : NULL; // el broker 0 es réplica

    // La réplica recibe tres mensajes del líder (broker 2)
    for (int i = 0; ok && i < 3; i++) {
        message_t msg;
        unsigned char buf[64];
        ok = message_init(&msg, NULL, 0, "dato", 4) == MESSAGE_OK;
        msg.offset = i;
        long size = ok ? message_serialize(&msg, buf, sizeof(buf)) : -1;
        message_destroy(&msg);
        ok = size > 0 && log_append_record(p->log, buf, (size_t)size) == LOG_OK;
    }

    if (t != NULL) {
        cluster_metadata_destroy(&m); // también cierra los logs
    }

    // Al volver a arrancar se recrean los metadatos y se reabren los logs
    t = NULL;
    ok = ok && setup_broker(&m, 0, dir, &t);
    ok = ok && log_next_offset(topic_get_partition(t, 2)->log) == 3;

    if (t != NULL) {
        cluster_metadata_destroy(&m);
    }

    remove_temp_dir(dir);
    return ok;
}

// Comprueba que apagar los logs del tópico despierta a quien espera
static bool test_shutdown_logs(void) {
    char dir[DIR_SIZE];
    make_temp_dir(dir, sizeof(dir));
    cluster_metadata_t m;
    topic_t* t = NULL;

    bool ok = setup_broker(&m, 0, dir, &t);
    partition_t* p = ok ? topic_get_partition(t, 0) : NULL;

    ok = ok && produce(p->log, "x") == 0;
    topic_shutdown_logs(t);
    ok = ok && log_wait_hw(p->log, 0, -1) == LOG_CLOSED && produce(p->log, "y") == -1;

    if (t != NULL) {
        cluster_metadata_destroy(&m);
    }

    remove_temp_dir(dir);
    return ok;
}

int main(void) {
    SimpleTest tests[] = {
        {"La misma clave va siempre a la misma partición", test_same_key_same_partition},
        {"Sin clave se reparte en turno rotativo", test_no_key_round_robin},
        {"El turno rotativo es exacto con varios hilos", test_round_robin_concurrent},
        {"Se rechazan nombres de tópico inválidos", test_invalid_names},
        {"Consultas de líder, seguidores e ISR", test_partition_queries},
        {"Un broker abre solo sus logs y con su rol", test_open_only_local_logs},
        {"Líder y réplica de una partición entre dos brokers", test_leader_and_follower},
        {"Un cambio de ISR deja avanzar el high watermark", test_isr_change_updates_log},
        {"Un cambio de líder cambia el rol de los logs", test_leader_change_updates_role},
        {"Los logs se recuperan al reiniciar el broker", test_reopen_after_restart},
        {"Apagar los logs despierta a quien espera", test_shutdown_logs}
    };

    int total = sizeof(tests) / sizeof(tests[0]);
    int passed = 0;

    printf("================================\n");
    printf("  PRUEBAS TOPIC\n");
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
    printf("Pruebas de topic: %d/%d aprobadas\n", passed, total);

    return passed == total ? EXIT_SUCCESS : EXIT_FAILURE;
}
