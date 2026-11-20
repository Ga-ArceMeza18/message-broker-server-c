#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "cluster_metadata.h"

int main(void) {
    cluster_metadata_t m;
    assert(cluster_metadata_init(&m) == 0);

    /* create_topic */
    assert(create_topic(&m, "pedidos", 4, 2, 3) == 0);
    topic_t *t = find_topic(&m, "pedidos");
    assert(t && t->num_partitions == 4);
    for (int p = 0; p < 4; p++) {
        partition_t *part = topic_get_partition(t, p);
        assert(part->leader_rank == p % 3);
        assert(part->replica_ranks[0] == part->leader_rank);
        assert(part->replica_ranks[0] != part->replica_ranks[1]);
        assert(part->isr_ranks[0] == part->replica_ranks[0]);
    }
    assert(create_topic(&m, "pedidos", 4, 2, 3) != 0);   // duplicado
    assert(create_topic(&m, "otro", 2, 5, 3) != 0);      // rf > brokers
    assert(find_topic(&m, "no_existe") == NULL);
    assert(m.version == 1);

    /* crecimiento del arreglo (más de INITIAL_CAPACITY topics) */
    char name[32];
    for (int i = 0; i < 20; i++) {
        snprintf(name, sizeof name, "t%d", i);
        assert(create_topic(&m, name, 2, 1, 3) == 0);
    }
    assert(m.num_topics == 21 && m.version == 21);
    assert(find_topic(&m, "t19") != NULL);

    /* apply_update: cambio de líder */
    metadata_update_t u;
    memset(&u, 0, sizeof u);
    u.version = 22; u.type = MD_LEADER_CHANGE;
    strcpy(u.topic, "pedidos"); u.partition_id = 0; u.leader_rank = 2;
    assert(apply_update(&m, &u) == 0);
    assert(topic_get_partition(t, 0)->leader_rank == 2);
    assert(m.version == 22);

    /* update obsoleto se ignora */
    u.leader_rank = 1;
    assert(apply_update(&m, &u) == 1);
    assert(topic_get_partition(t, 0)->leader_rank == 2);

    /* ISR se reduce */
    memset(&u, 0, sizeof u);
    u.version = 23; u.type = MD_ISR_CHANGE;
    strcpy(u.topic, "pedidos"); u.partition_id = 1;
    u.isr[0] = 1; u.isr_count = 1;
    assert(apply_update(&m, &u) == 0);
    assert(topic_get_partition(t, 1)->isr_ranks[0] == 1);
    assert(topic_get_partition(t, 1)->isr_ranks[1] == -1);

    /* CREATE_TOPIC por update (lo que hace un broker que no es el origen) */
    cluster_metadata_t m2;
    assert(cluster_metadata_init(&m2) == 0);
    memset(&u, 0, sizeof u);
    u.version = 1; u.type = MD_CREATE_TOPIC; strcpy(u.topic, "pedidos");
    u.num_partitions = 4; u.replication_factor = 2; u.num_brokers = 3;
    assert(apply_update(&m2, &u) == 0);
    topic_t *t2 = find_topic(&m2, "pedidos");
    assert(t2 && topic_get_partition(t2, 3)->leader_rank == 0);

    /* update sobre topic inexistente */
    memset(&u, 0, sizeof u);
    u.version = 99; u.type = MD_LEADER_CHANGE; strcpy(u.topic, "nada");
    assert(apply_update(&m, &u) == -1);

    cluster_metadata_destroy(&m);
    cluster_metadata_destroy(&m2);
    printf("OK: todos los tests pasaron\n");
    return 0;
}
