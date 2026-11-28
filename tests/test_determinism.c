#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

#include "cluster_metadata.h"
#include "consumer_group_t.h"
#include "message.h"
#include "protocol.h"
#include "topic.h"

#define VUELTAS 37
#define HILOS_RARO 5
#define MENSAJES_POR_HILO 13

typedef struct {
    int fallos;
} hilo_datos_t;

static void probar_protocolo(void) {
    request_t req;
    memset(&req, 0, sizeof(req));
    req.op = PROTO_OP_PRODUCE;
    snprintf(req.topic, sizeof(req.topic), "papas_con_queso");
    snprintf(req.key, sizeof(req.key), "llave_9999");
    req.partition_id = 2;
    req.acks = PROTO_ACKS_LEADER;
    req.value_len = 7;
    memcpy(req.value, "xyz_777", 7);

    unsigned char base[PROTO_MAX_FRAME_BYTES];
    size_t len_base = encode_request(&req, base, sizeof(base));
    assert(len_base > 0);

    unsigned char copia[PROTO_MAX_FRAME_BYTES];
    for (int i = 0; i < VUELTAS; i++) {
        size_t len_copia = encode_request(&req, copia, sizeof(copia));
        assert(len_copia == len_base);
        assert(memcmp(base, copia, len_base) == 0);

        request_t out;
        assert(decode_request(copia, len_copia, &out) == 0);
        assert(out.op == req.op);
        assert(strcmp(out.topic, "papas_con_queso") == 0);
        assert(strcmp(out.key, "llave_9999") == 0);
        assert(out.value_len == 7);
        assert(memcmp(out.value, "xyz_777", 7) == 0);
    }

    response_t resp;
    memset(&resp, 0, sizeof(resp));
    resp.status = PROTO_STATUS_OK;
    resp.partition_id = 2;
    resp.member_id = 42069;
    resp.offset = 808080L;
    snprintf(resp.text_info, sizeof(resp.text_info), "todo bien 314");
    resp.data_len = 6;
    memcpy(resp.payload, "123abc", 6);

    unsigned char rbase[PROTO_MAX_FRAME_BYTES];
    size_t rlen_base = encode_response(&resp, rbase, sizeof(rbase));
    assert(rlen_base > 0);

    unsigned char rcopia[PROTO_MAX_FRAME_BYTES];
    for (int i = 0; i < VUELTAS; i++) {
        size_t rlen_copia = encode_response(&resp, rcopia, sizeof(rcopia));
        assert(rlen_copia == rlen_base);
        assert(memcmp(rbase, rcopia, rlen_base) == 0);

        response_t rout;
        assert(decode_response(rcopia, rlen_copia, &rout) == 0);
        assert(rout.status == resp.status);
        assert(rout.partition_id == 2);
        assert(rout.member_id == 42069);
        assert(rout.offset == 808080L);
        assert(rout.data_len == 6);
        assert(memcmp(rout.payload, "123abc", 6) == 0);
    }

    printf("[OK] protocolo igual en %d vueltas\n", VUELTAS);
}

static void *hilo_mensaje(void *arg) {
    hilo_datos_t *d = (hilo_datos_t *)arg;
    unsigned char ref[MESSAGE_MAX_SIZE];
    long ref_len = 0;

    for (int i = 0; i < MENSAJES_POR_HILO; i++) {
        message_t m;
        const char *k = "k_555";
        const char *v = "val_12345";
        if (message_init(&m, k, (int)strlen(k), v, (int)strlen(v)) != MESSAGE_OK) {
            d->fallos++;
            return NULL;
        }
        m.offset = 314;
        m.timestamp = 1600000777000L;

        unsigned char buf[MESSAGE_MAX_SIZE];
        long n = message_serialize(&m, buf, sizeof(buf));
        message_destroy(&m);

        if (n <= 0) {
            d->fallos++;
            return NULL;
        }
        if (i == 0) {
            ref_len = n;
            memcpy(ref, buf, (size_t)n);
        } else if (n != ref_len || memcmp(ref, buf, (size_t)n) != 0) {
            d->fallos++;
            return NULL;
        }
    }
    return NULL;
}

static void probar_mensajes_y_particiones(void) {
    pthread_t th[HILOS_RARO];
    hilo_datos_t datos[HILOS_RARO];

    for (int i = 0; i < HILOS_RARO; i++) {
        datos[i].fallos = 0;
        assert(pthread_create(&th[i], NULL, hilo_mensaje, &datos[i]) == 0);
    }
    for (int i = 0; i < HILOS_RARO; i++) {
        pthread_join(th[i], NULL);
        assert(datos[i].fallos == 0);
    }

    topic_t *a = topic_create("topic_loco", 11, 2, 0);
    topic_t *b = topic_create("topic_loco", 11, 2, 0);
    assert(a != NULL && b != NULL);

    const char *claves[] = {"clave_13", "otra_888", "", "dato_404", "z_99"};
    for (int i = 0; i < 5; i++) {
        int pa = topic_partition_for_key(a, claves[i], (int)strlen(claves[i]));
        int pb = topic_partition_for_key(b, claves[i], (int)strlen(claves[i]));
        assert(pa == pb);
    }

    topic_destroy(a);
    topic_destroy(b);

    printf("[OK] mensajes en %d hilos y particiones sin cambios\n", HILOS_RARO);
}

static void probar_metadata_y_grupo(void) {
    cluster_metadata_t c1, c2;
    assert(cluster_metadata_init(&c1) == 0);
    assert(cluster_metadata_init(&c2) == 0);

    assert(create_topic(&c1, "meta_99", 9, 2, 3) == 0);
    assert(create_topic(&c2, "meta_99", 9, 2, 3) == 0);

    topic_t *t1 = find_topic(&c1, "meta_99");
    topic_t *t2 = find_topic(&c2, "meta_99");
    assert(t1 != NULL && t2 != NULL);

    for (int p = 0; p < 9; p++) {
        partition_t *pa = topic_get_partition(t1, p);
        partition_t *pb = topic_get_partition(t2, p);
        assert(pa->leader_rank == pb->leader_rank);
        assert(pa->replica_ranks[0] == pb->replica_ranks[0]);
        assert(pa->replica_ranks[1] == pb->replica_ranks[1]);
    }

    cluster_metadata_destroy(&c1);
    cluster_metadata_destroy(&c2);

    consumer_group_t g1, g2;
    assert(group_init(&g1, "grupo_churro", 13) == 0);
    assert(group_init(&g2, "grupo_churro", 13) == 0);

    int m1 = 0, m2 = 0, n1 = 0, n2 = 0;
    assert(group_join(&g1, 67, &m1) == 0);
    assert(group_join(&g1, 88, &m2) == 0);
    assert(group_join(&g2, 67, &n1) == 0);
    assert(group_join(&g2, 88, &n2) == 0);

    assert(m1 == n1 && m2 == n2);
    assert(g1.generation == g2.generation);
    for (int p = 0; p < 13; p++) {
        assert(g1.assignment[p] == g2.assignment[p]);
    }

    group_destroy(&g1);
    group_destroy(&g2);

    printf("[OK] metadata y grupo iguales\n");
}

int main(void) {
    probar_protocolo();
    probar_mensajes_y_particiones();
    probar_metadata_y_grupo();
    printf("listo, todo determinista\n");
    return 0;
}